package phase0

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/transport"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/media"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/parse"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/task"
)

// The operations the crawler calls (internal/tt/stage).
const (
	queryList     = "creator_list"
	queryComments = "comments"
	queryReplies  = "replies"
)

// Page sizes, as the crawler asks (configs/tt/default.yaml).
const (
	listPageSize    = 15
	commentPageSize = 20
	replyPageSize   = 3
)

// Limits shape one account's work: what the crawler does for an account
// in its first days (a backfill, scheduled checks, whole comment reads),
// cut to what a measurement needs.
type Limits struct {
	// BackfillDays is the backfill's window (365, as the crawler's).
	BackfillDays int
	// MaxListPages caps the list pages after the first.
	MaxListPages int
	// CommentPosts is how many posts with comments get a comment read;
	// MaxCommentPages caps each read's top-level pages.
	CommentPosts    int
	MaxCommentPages int
	// ReplyThreads is how many threads per post get their replies read;
	// MaxReplyPages caps each thread's pages.
	ReplyThreads  int
	MaxReplyPages int
	// Videos downloads one video both ways for the first this many
	// accounts (T33).
	Videos int
	// Replays keeps the first signed comment URL of the first this many
	// accounts and sends it again at each of the kit's replay ages (T27).
	Replays int
}

// blocking reports whether a class is TikTok pushing back on the exit or
// identity (as opposed to the account or the parser).
func blocking(c string) bool {
	switch c {
	case detect.Checkpoint.String(), detect.RateLimited.String(), detect.LoginWall.String(), detect.RegistryStale.String(), classChallenge:
		return true
	}
	return false
}

// Classes the kit adds to detect's.
const (
	classChallenge = "challenge"  // the JavaScript challenge page (T28)
	classSignError = "sign_error" // the signer page failed to sign
	classRefused   = "refused"    // a media host refused the file (403, 410)
)

// work is one step's accounts on one tier.
type work struct {
	k       *Kit
	client  *graphql.Client
	rec     *Recorder
	tier    string
	lim     Limits
	replays *replayer

	captchas atomic.Int64
	mu       sync.Mutex
	resolved map[int]string // account line -> secUid, for the ladder (in memory only)
}

func (w *work) now() time.Time { return w.k.now() }

func ms(start time.Time) int64 { return time.Since(start).Milliseconds() }

// call sends one operation and returns its record, not yet added: the
// caller fills in what the parser found.
func (w *work) call(ctx context.Context, id *session.Identity, exit string, acct int, op, query string, vars map[string]any, referer string) (*graphql.Response, Record) {
	return w.callWith(ctx, w.client, id, exit, acct, op, query, vars, referer)
}

// callWith is call with another client (the variants step's).
func (w *work) callWith(ctx context.Context, client *graphql.Client, id *session.Identity, exit string, acct int, op, query string, vars map[string]any, referer string) (*graphql.Response, Record) {
	c := &call{account: acct}
	r := Record{Op: op, Account: acct, Exit: exit}
	resp, err := client.Do(withCall(ctx, c), id, graphql.Request{Query: query, Vars: vars, Referer: referer})
	r.Signed, r.SignMs, r.PageOpenMs, r.CrossAccount, r.SignError = c.signed.Load(), c.signMs.Load(), c.pageOpenMs.Load(), c.cross.Load(), c.signErr.Load()
	if err != nil {
		r.Class, r.Reason = detect.Transport.String(), err.Error()
		if errors.Is(err, graphql.ErrSign) {
			r.Class = classSignError
		}
		return nil, r
	}
	r.Variant, r.Class, r.Status, r.Reason = resp.Query.DocID, resp.Class.String(), resp.Status, resp.Reason
	r.Wire, r.Ms, r.SignRetried = resp.BytesIn+resp.BytesOut, resp.Elapsed.Milliseconds(), resp.SignRetried
	if resp.Class == detect.Checkpoint {
		w.captchas.Add(1)
	}
	if w.replays != nil && r.Signed && resp.Class == detect.OK && op == OpComments {
		c.mu.Lock()
		u, ref := c.url, c.referer
		c.mu.Unlock()
		w.replays.offer(ctx, acct, u, ref)
	}
	return resp, r
}

func profileURL(id *session.Identity, name string) string {
	u, _ := id.Base.Parse(task.ProfilePath(name))
	return u.String()
}

func postURL(id *session.Identity, name string, p *ttv1.Post) string {
	kind := "/video/"
	if p.GetKind() == ttv1.PostKind_POST_KIND_PHOTO {
		kind = "/photo/"
	}
	u, _ := id.Base.Parse(task.ProfilePath(name) + kind + strconv.FormatInt(p.GetPostId(), 10))
	return u.String()
}

// account does one account's work: its page, the list's check and
// backfill, comment reads with replies, and a video both ways; then a
// summary record.
func (w *work) account(ctx context.Context, acct int, name string) {
	sum := Record{Op: OpAccount, Account: acct}
	defer func() {
		if ctx.Err() == nil {
			w.rec.Add(sum)
		}
	}()
	id, err := w.k.Stack.Pool.Acquire(ctx, w.tier)
	if err != nil {
		sum.Stop, sum.Reason = "no_identity", err.Error()
		return
	}
	defer w.k.Stack.Pool.Release(id)
	exit := w.k.exits.Label(id)
	sum.Exit = exit
	sec, status := w.page(ctx, id, exit, acct, name)
	if sec == "" {
		sum.Stop = status
		return
	}
	w.mu.Lock()
	w.resolved[acct] = sec
	w.mu.Unlock()

	posts, stop, pages := w.list(ctx, id, exit, acct, name, sec)
	sum.Stop, sum.Pages = stop, pages
	now := w.now()
	cutoff := now.AddDate(0, 0, -w.lim.BackfillDays)
	oldest := now
	var inWindow []parse.PostBundle
	for _, b := range posts {
		t := b.Post.GetCreatedAt().AsTime()
		if t.Before(oldest) {
			oldest = t
		}
		if t.Before(cutoff) {
			continue
		}
		inWindow = append(inWindow, b)
		sum.Posts++
		if b.Post.GetKind() == ttv1.PostKind_POST_KIND_VIDEO {
			sum.VideoPosts++
		}
		cc := b.Post.GetCounts().GetComments()
		sum.CommentCount += cc
		sum.CommentPagesEst += (cc + commentPageSize - 1) / commentPageSize
	}
	// The days the posts cover: the whole window when the list went past
	// it, else back to the oldest post read (at least a day).
	switch {
	case stop == "age" || oldest.Before(cutoff):
		sum.Days = float64(w.lim.BackfillDays)
	default:
		sum.Days = max(1, now.Sub(oldest).Hours()/24)
	}

	n := 0
	for _, b := range inWindow {
		if n >= w.lim.CommentPosts || ctx.Err() != nil {
			break
		}
		if b.Post.GetCounts().GetComments() == 0 || b.Post.GetCommentsDisabled() {
			continue
		}
		n++
		w.comments(ctx, id, exit, acct, name, b.Post)
	}
	if acct <= w.lim.Videos {
		for _, b := range inWindow {
			if u := videoURL(b); u != "" {
				w.video(ctx, id, exit, acct, u)
				break
			}
		}
	}
}

// page loads the account's profile page (the identity's bootstrap) and
// returns the account's secUid, or why there is none.
func (w *work) page(ctx context.Context, id *session.Identity, exit string, acct int, name string) (string, string) {
	start := time.Now()
	pl, err := w.k.Stack.Loader.Load(ctx, id, profileURL(id, name))
	r := Record{Op: OpPage, Account: acct, Exit: exit, Ms: ms(start)}
	if pl == nil {
		r.Class = detect.Transport.String()
		if err != nil {
			r.Reason = err.Error()
		}
		w.rec.Add(r)
		return "", "page_" + r.Class
	}
	r.Class, r.Status, r.Wire, r.Reason = pl.Class.String(), pl.Status, pl.Bytes, pl.Reason
	if challenged(pl.HTML) {
		r.Class = classChallenge
	}
	if r.Class != detect.OK.String() {
		w.rec.Add(r)
		return "", "page_" + r.Class
	}
	res, rep := parse.ProfilePage(pl.HTML, parse.Options{Query: "profile_page"})
	if !rep.OK() {
		r.Drift = strings.Join(rep.Missing, ",")
	}
	w.rec.Add(r)
	a := res.Account
	switch {
	case a == nil && res.Status != ttv1.AccountStatus_ACCOUNT_STATUS_UNSPECIFIED:
		return "", strings.ToLower(strings.TrimPrefix(res.Status.String(), "ACCOUNT_STATUS_"))
	case a == nil || a.GetSecUid() == "":
		return "", "no_account"
	case a.GetPrivate():
		return "", "private"
	}
	return a.GetSecUid(), ""
}

// list reads the creator list as a check, then back as a backfill does,
// to the window, the first post or the page cap.
func (w *work) list(ctx context.Context, id *session.Identity, exit string, acct int, name, sec string) ([]parse.PostBundle, string, int) {
	now := w.now()
	cutoff := now.AddDate(0, 0, -w.lim.BackfillDays).UnixMilli()
	cursor := (now.Unix() + 1) * 1000
	seen := map[int64]bool{}
	var posts []parse.PostBundle
	op := OpListCheck
	pages, stuck := 0, 0
	softRetried := false
	for {
		if ctx.Err() != nil {
			return posts, "stopped", pages
		}
		resp, r := w.call(ctx, id, exit, acct, op, queryList, map[string]any{"sec_uid": sec, "count": listPageSize, "cursor": cursor}, profileURL(id, name))
		if r.Class == detect.SoftError.String() && !softRetried {
			// TikTok's occasional list error: once more after a pause, as
			// the crawler does.
			r.Retried = true
			w.rec.Add(r)
			softRetried = true
			if !w.k.sleep(ctx, w.k.Cfg.SoftPause) {
				return posts, "stopped", pages
			}
			continue
		}
		softRetried = false
		if resp == nil || resp.Class != detect.OK {
			w.rec.Add(r)
			return posts, "error_" + r.Class, pages
		}
		res, rep := parse.PostList(resp.Body, parse.Options{Query: queryList, Variant: resp.Query.DocID})
		r.Items, r.More = len(res.Posts), res.HasMore
		if !rep.OK() {
			r.Drift = strings.Join(rep.Missing, ",")
		}
		w.rec.Add(r)
		if !rep.OK() {
			return posts, "drift", pages
		}
		pages++
		fresh := 0
		for _, b := range res.Posts {
			if !seen[b.Post.GetPostId()] {
				seen[b.Post.GetPostId()] = true
				posts = append(posts, b)
				fresh++
			}
		}
		if fresh == 0 {
			stuck++
		} else {
			stuck = 0
		}
		next, more := ttclient.NextCreatorCursor(resp.Parts[0], cursor)
		switch {
		case stuck >= 3:
			return posts, "stuck", pages
		case !more:
			return posts, "first_post", pages
		case next < cutoff:
			return posts, "age", pages
		case pages > w.lim.MaxListPages:
			return posts, "max_pages", pages
		}
		cursor, op = next, OpListBackfill
	}
}

// comments reads a post's comments as a whole read does, then the replies
// of its threads, every page through the signer; then a summary record.
func (w *work) comments(ctx context.Context, id *session.Identity, exit string, acct int, name string, p *ttv1.Post) {
	ref := postURL(id, name, p)
	cc := parse.CommentContext{PostID: p.GetPostId(), UserID: p.GetUserId(), PostCreatedAt: p.GetCreatedAt()}
	sum := Record{Op: OpCommentRead, Account: acct, Exit: exit, CommentCount: p.GetCounts().GetComments()}
	var threads []parse.ThreadRef
	cursor := int64(0)
	topDone := false
	for sum.Pages < w.lim.MaxCommentPages && ctx.Err() == nil {
		resp, r := w.call(ctx, id, exit, acct, OpComments, queryComments,
			map[string]any{"item_id": p.GetPostId(), "count": commentPageSize, "cursor": cursor}, ref)
		sum.Pages++
		if resp == nil || resp.Class != detect.OK {
			w.rec.Add(r)
			break
		}
		res, rep := parse.Comments(resp.Body, cc, parse.Options{Query: queryComments, Variant: resp.Query.DocID})
		r.Items, r.More = len(res.Comments), res.HasMore
		if !rep.OK() {
			r.Drift = strings.Join(rep.Missing, ",")
		}
		w.rec.Add(r)
		if !rep.OK() {
			break
		}
		sum.Collected += len(res.Comments)
		for _, t := range res.Threads {
			if t.ReplyCount > int64(t.Inline) {
				threads = append(threads, t)
			}
		}
		if !res.HasMore {
			topDone = true
			break
		}
		cursor = res.Cursor
	}
	threadsDone := true
	for i, t := range threads {
		if i >= w.lim.ReplyThreads || ctx.Err() != nil {
			threadsDone = false
			break
		}
		rc := cc
		rc.Replies, rc.ParentCommentID = true, t.CommentID
		cursor, pages, got, done := int64(0), 0, 0, false
		for pages < w.lim.MaxReplyPages && ctx.Err() == nil {
			resp, r := w.call(ctx, id, exit, acct, OpReplies, queryReplies,
				map[string]any{"item_id": p.GetPostId(), "comment_id": t.CommentID, "count": replyPageSize, "cursor": cursor}, ref)
			pages++
			if resp == nil || resp.Class != detect.OK {
				w.rec.Add(r)
				break
			}
			res, rep := parse.Comments(resp.Body, rc, parse.Options{Query: queryReplies, Variant: resp.Query.DocID})
			r.Items, r.More = len(res.Comments), res.HasMore
			if !rep.OK() {
				r.Drift = strings.Join(rep.Missing, ",")
			}
			w.rec.Add(r)
			if !rep.OK() {
				break
			}
			got += len(res.Comments)
			if !res.HasMore {
				done = true
				break
			}
			cursor = res.Cursor
		}
		sum.ReplyPages += pages
		// The page quoted some replies already; count each reply once.
		sum.Collected += max(0, got-t.Inline)
		threadsDone = threadsDone && done
	}
	sum.Complete = topDone && threadsDone
	w.rec.Add(sum)
}

// videoURL returns a post's video file link, the smallest rendition first
// as the downloader would take it.
func videoURL(b parse.PostBundle) string {
	for _, m := range b.Media {
		if m.GetKind() == ttv1.MediaKind_MEDIA_KIND_VIDEO && m.GetSourceUrl() != "" {
			return m.GetSourceUrl()
		}
	}
	return ""
}

// mediaHeaders are the headers Chrome sends for a <video> element's file
// on TikTok's page (the probe's and the downloader's).
func mediaHeaders(fp session.Fingerprint, acceptLanguage string) [][2]string {
	return [][2]string{
		{"sec-ch-ua", fp.SecChUa}, {"sec-ch-ua-mobile", fp.SecChUaMobile}, {"sec-ch-ua-platform", fp.SecChUaPlatform},
		{"user-agent", fp.UserAgent}, {"accept", "*/*"}, {"sec-fetch-site", "same-site"}, {"sec-fetch-mode", "no-cors"},
		{"sec-fetch-dest", "video"}, {"referer", "https://www.tiktok.com/"}, {"accept-encoding", "identity;q=1, *;q=0"},
		{"accept-language", acceptLanguage}, {"range", "bytes=0-"},
	}
}

// directVideoBytes is how much of a file the direct way reads: enough to
// tell whether the host serves it.
const directVideoBytes = 1 << 20

// video downloads one video file through the identity's exit with its
// tt_chain_token, as the crawl tier would, then asks for the same file from
// this machine's own IP with the same cookie, as tks-media-downloader does
// (T33).
func (w *work) video(ctx context.Context, id *session.Identity, exit string, acct int, raw string) {
	u, err := url.Parse(raw)
	if err != nil {
		return
	}
	chain := id.Doer.Cookies(id.Base)["tt_chain_token"]
	cookie := chain != "" && media.InDomains(u.Hostname(), w.k.cookieDomains())
	hdr := mediaHeaders(id.FP, w.k.Cfg.AcceptLanguage)
	if cookie {
		hdr = append(hdr, [2]string{"cookie", "tt_chain_token=" + chain})
	}
	r := Record{Op: OpVideoProxy, Account: acct, Exit: exit}
	if err := id.Wait(ctx); err != nil {
		return
	}
	start := time.Now()
	resp, err := id.Doer.Do(ctx, &transport.Request{Method: http.MethodGet, URL: raw, Header: hdr})
	r.Ms = ms(start)
	if err != nil {
		r.Class, r.Reason = detect.Transport.String(), err.Error()
	} else {
		r.Status, r.Wire, r.Size = resp.Status, resp.BytesIn+resp.BytesOut, int64(len(resp.Body))
		r.Class = mediaClass(resp.Status)
	}
	w.rec.Add(r)

	d := Record{Op: OpVideoDirect, Account: acct, Exit: "direct"}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return
	}
	for _, h := range hdr {
		req.Header.Set(h[0], h[1])
	}
	req.Header.Set("range", "bytes=0-"+strconv.Itoa(directVideoBytes-1))
	start = time.Now()
	hr, err := w.k.Stack.HTTP.Do(req)
	if err != nil {
		d.Class, d.Reason, d.Ms = detect.Transport.String(), err.Error(), ms(start)
		w.rec.Add(d)
		return
	}
	n, _ := io.Copy(io.Discard, io.LimitReader(hr.Body, directVideoBytes))
	_ = hr.Body.Close()
	d.Ms, d.Status, d.Wire, d.Class = ms(start), hr.StatusCode, n, mediaClass(hr.StatusCode)
	d.Size = declaredSize(hr)
	w.rec.Add(d)
}

func mediaClass(status int) string {
	switch status {
	case http.StatusOK, http.StatusPartialContent:
		return detect.OK.String()
	case http.StatusForbidden, http.StatusGone:
		return classRefused
	default:
		return detect.SoftError.String()
	}
}

// declaredSize reads a file's whole size from Content-Range ("bytes
// 0-1023/53412"), else Content-Length.
func declaredSize(r *http.Response) int64 {
	if cr := r.Header.Get("Content-Range"); cr != "" {
		if _, total, ok := strings.Cut(cr, "/"); ok {
			if n, err := strconv.ParseInt(total, 10, 64); err == nil {
				return n
			}
		}
	}
	return max(r.ContentLength, 0)
}

// replayer sends signed comment URLs again from another identity at the
// kit's ages (T27: a signature worked 14–84 s later in C2 run 3; how long
// past that is Phase 0's to measure). The run waits for them up to
// Config.ReplayWait, then drops the rest.
type replayer struct {
	w    *work
	ages []time.Duration
	max  int

	mu      sync.Mutex
	offered map[int]bool
	timers  []*time.Timer
	wg      sync.WaitGroup
	ctx     context.Context
}

func newReplayer(ctx context.Context, w *work, ages []time.Duration, maxAccounts int) *replayer {
	return &replayer{w: w, ages: ages, max: maxAccounts, offered: map[int]bool{}, ctx: ctx}
}

// offer schedules the replays of an account's first signed comment URL,
// for the first max accounts.
func (r *replayer) offer(_ context.Context, acct int, signedURL, referer string) {
	if signedURL == "" || acct > r.max {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.offered[acct] {
		return
	}
	r.offered[acct] = true
	signedAt := time.Now()
	for _, age := range r.ages {
		r.wg.Add(1)
		r.timers = append(r.timers, time.AfterFunc(age, func() {
			defer r.wg.Done()
			r.replay(acct, signedURL, referer, time.Since(signedAt))
		}))
	}
}

func (r *replayer) replay(acct int, signedURL, referer string, age time.Duration) {
	ctx := r.ctx
	if ctx.Err() != nil {
		return
	}
	k := r.w.k
	id, err := k.Stack.Pool.Acquire(ctx, r.w.tier)
	if err != nil {
		return
	}
	defer k.Stack.Pool.Release(id)
	rec := Record{Op: OpSigReplay, Account: acct, Exit: k.exits.Label(id), AgeS: int64(age.Seconds()), Signed: true}
	// The page signs on its own host; the call goes to the identity's.
	if u, err := url.Parse(signedURL); err == nil {
		u.Scheme, u.Host = id.Base.Scheme, id.Base.Host
		signedURL = u.String()
	}
	hdr := ttclient.Builder{AcceptLanguage: k.Cfg.AcceptLanguage}.Build(id, registry.Query{Method: http.MethodGet}, nil, nil, referer).Header
	if err := id.Wait(ctx); err != nil {
		return
	}
	start := time.Now()
	resp, err := id.Doer.Do(ctx, &transport.Request{Method: http.MethodGet, URL: signedURL, Header: hdr})
	rec.Ms = ms(start)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		rec.Class, rec.Reason = detect.Transport.String(), err.Error()
	} else {
		rec.Status, rec.Wire = resp.Status, resp.BytesIn+resp.BytesOut
		parts, errs, hasData := ttclient.Decode(resp.Body)
		cls, reason := k.Stack.Rules.Classify(detect.Input{Status: resp.Status, Location: resp.Location,
			ContentType: resp.Header.Get("Content-Type"), Body: resp.Body, Errors: errs, HasData: hasData, Expect: "graphql"})
		if cls == detect.OK && len(parts) == 0 {
			cls, reason = detect.RegistryStale, "empty answer: request refused"
		}
		rec.Class, rec.Reason = cls.String(), reason
		session.Outcome(id, cls)
	}
	r.w.rec.Add(rec)
}

// finish waits up to maxWait for the replays still due, then drops the
// rest.
func (r *replayer) finish(maxWait time.Duration) {
	done := make(chan struct{})
	go func() { r.wg.Wait(); close(done) }()
	t := time.NewTimer(maxWait)
	defer t.Stop()
	select {
	case <-done:
		return
	case <-t.C:
	case <-r.ctx.Done():
	}
	r.stop()
}

// pending reports whether replays are still due.
func (r *replayer) pending() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.offered) > 0 && len(r.timers) > 0
}

// stop drops the replays not yet due and waits for those under way.
func (r *replayer) stop() {
	r.mu.Lock()
	for _, t := range r.timers {
		if t.Stop() {
			r.wg.Done()
		}
	}
	r.timers = nil
	r.mu.Unlock()
	r.wg.Wait()
}
