package spike

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/task"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/htmlx"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/parse"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fixtures"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/docids"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
)

// Capture records real responses of one page as parser fixtures under
// dir/<query>/<date>_<name>/, scrubbed of personal data. Review the files
// before committing them.
func (r *Runner) Capture(ctx context.Context, page, dir, name string) ([]string, error) {
	ref, err := task.NormalizeRef(page)
	if err != nil {
		return nil, err
	}
	id, pl, _, err := r.loadPage(ctx, ref, nil)
	if err != nil {
		return nil, err
	}
	defer r.Pool.Release(id)
	date := time.Now().UTC().Format("20060102")
	if pl.Class != detect.OK || !id.Bootstrapped() {
		return r.keepPage(id, date+"_"+name, pl, fmt.Errorf("%w: %s (%s)", session.ErrBootstrap, pl.Class, pl.Reason))
	}
	pg, _ := parse.Page(pl.HTML, 0, task.RefID(ref))
	if pg == nil {
		return r.keepPage(id, date+"_"+name, pl, errors.New("spike: page id not found in html"))
	}
	pageMeta := map[string]any{"page_id": pg.PageId}
	if htmlx.PageID(pl.HTML) == 0 {
		// The id is the one the page was asked for: the HTML names none.
		pageMeta["id_from_ref"] = true
	}
	keep := []string{strconv.FormatInt(pg.PageId, 10)}
	var written []string
	write := func(query, file string, body []byte, meta map[string]any) error {
		meta["source"] = "capture"
		d, err := fixtures.Write(dir, query, date+"_"+name, file, body, meta, keep)
		if d != "" {
			written = append(written, d)
		}
		return err
	}
	if err := write("page_html", "response.html", pl.HTML, pageMeta); err != nil {
		return written, err
	}
	failed := func(query string, resp *graphql.Response, err error) ([]string, error) {
		err = fmt.Errorf("spike: %s: %w (%s)", query, errOr(err), classOf(resp))
		d, kerr := r.keepFailure(id, query, date+"_"+name, pg.PageId, resp, keep)
		if d != "" {
			written = append(written, d)
		}
		return written, errors.Join(err, kerr)
	}
	resp, err := r.GQL.Do(ctx, id, graphql.Request{Query: "timeline", Vars: map[string]any{"page_id": strconv.FormatInt(pg.PageId, 10), "count": 10}})
	if err != nil || resp.Class != detect.OK {
		return failed("timeline", resp, err)
	}
	if err := write("timeline", "response.ndjson", resp.Body, map[string]any{"page_id": pg.PageId}); err != nil {
		return written, err
	}
	tl, _ := parse.Timeline(resp.Parts, pg.PageId, id.Base.String())
	sort.Slice(tl.Posts, func(i, j int) bool { return tl.Posts[i].Post.GetCommentCount() > tl.Posts[j].Post.GetCommentCount() })
	if len(tl.Posts) == 0 || tl.Posts[0].Post.GetCommentCount() == 0 {
		return written, nil
	}
	p := tl.Posts[0].Post
	cresp, err := r.GQL.Do(ctx, id, graphql.Request{Query: "comments", Vars: map[string]any{"feedback_id": p.FeedbackId}})
	if err != nil || cresp.Class != detect.OK {
		return failed("comments", cresp, err)
	}
	if err := write("comments", "response.ndjson", cresp.Body, map[string]any{"page_id": pg.PageId, "post_id": p.PostId}); err != nil {
		return written, err
	}
	cr, _ := parse.Comments(cresp.Parts, parse.CommentContext{PostID: p.PostId, PageID: pg.PageId})
	if len(cr.Threads) == 0 {
		return written, nil
	}
	th := cr.Threads[0]
	rresp, err := r.GQL.Do(ctx, id, graphql.Request{Query: "replies", Vars: map[string]any{"feedback_id": th.FeedbackID, "expansion_token": th.ExpansionToken}})
	if err != nil || rresp.Class != detect.OK {
		return failed("replies", rresp, err)
	}
	err = write("replies", "response.ndjson", rresp.Body, map[string]any{"page_id": pg.PageId, "post_id": p.PostId, "parent_comment_id": th.CommentID})
	return written, err
}

// errNotOK is reported when a response came back but was not OK.
var errNotOK = errors.New("response not ok")

func errOr(err error) error {
	if err != nil {
		return err
	}
	return errNotOK
}

func classOf(r *graphql.Response) string {
	if r == nil {
		return ""
	}
	if r.Status == 0 { // nothing was sent (an open breaker)
		return r.Class.String() + " " + r.Reason
	}
	ct := r.ContentType
	if ct == "" {
		ct = "no content type"
	}
	return fmt.Sprintf("%s %s; http %d, %s, %d bytes", r.Class, r.Reason, r.Status, ct, len(r.Body))
}

// keepFailure writes a response that was not OK, scrubbed like a fixture,
// to FailDir/<query>/<case>/ with what came back and which tokens and
// cookies the identity held (names only), so the bundle shows what Facebook
// said. Nothing is kept when no response came back.
func (r *Runner) keepFailure(id *session.Identity, query, caseName string, pageID int64, resp *graphql.Response, keep []string) (string, error) {
	if r.FailDir == "" || resp == nil || resp.Status == 0 {
		return "", nil
	}
	file := "response.ndjson"
	if bytes.HasPrefix(bytes.TrimSpace(resp.Body), []byte("<")) {
		file = "response.html"
	}
	meta := map[string]any{"source": "capture", "page_id": pageID, "status": resp.Status, "content_type": resp.ContentType,
		"class": resp.Class.String(), "reason": resp.Reason, "bytes": len(resp.Body),
		"tokens_found": tokensFound(id.Tokens()), "cookies": slices.Sorted(maps.Keys(id.Doer.Cookies(id.Base)))}
	return fixtures.Write(r.FailDir, query, caseName, file, resp.Body, meta, keep)
}

// keepPage writes a page capture could not use (not ok, or no id found),
// scrubbed like a fixture, to FailDir/page_html/<case>/, with its outcome
// class, the path it ended on after same-site redirects, which tokens it
// held, and how many ids its HTML holds under each key the extractor knows
// (counts only), so the bundle shows what the extractor missed. It returns
// err, naming where the page is kept.
func (r *Runner) keepPage(id *session.Identity, caseName string, pl *session.PageLoad, err error) ([]string, error) {
	if r.FailDir == "" {
		return nil, err
	}
	final := ""
	if u, err := url.Parse(pl.URL); err == nil {
		final = u.Path
	}
	meta := map[string]any{"source": "capture", "status": pl.Status, "class": pl.Class.String(), "reason": pl.Reason,
		"bytes": len(pl.HTML), "final_path": final, "id_hints": htmlx.PageIDHints(pl.HTML),
		"tokens_found": tokensFound(htmlx.ExtractTokens(pl.HTML)), "cookies": slices.Sorted(maps.Keys(id.Doer.Cookies(id.Base)))}
	d, kerr := fixtures.Write(r.FailDir, "page_html", caseName, "response.html", pl.HTML, meta, nil)
	if d == "" {
		return nil, errors.Join(err, kerr)
	}
	return []string{d}, errors.Join(fmt.Errorf("%w; the page is under %s", err, d), kerr)
}

// DocIDs loads pages and scans them and their JS bundles for Relay
// operation ids (friendly_name -> doc_id). Bundles are static and immutable,
// so they are fetched over direct egress (r.HTTP), and only from Facebook's
// CDN or the page's own host, at most maxBundles in all. It fails only when
// no page could be loaded.
func (r *Runner) DocIDs(ctx context.Context, pages []string, maxBundles int) (map[string]string, int, error) {
	out := map[string]string{}
	sc := &docids.Scanner{Fetch: docids.HTTPFetcher(r.HTTP)}
	n, loaded := 0, 0
	var lastErr error
	for _, page := range pages {
		pl, pageURL, err := r.load(ctx, page)
		if err != nil {
			lastErr = err
			continue
		}
		loaded++
		sc.MaxBundles = maxBundles - n
		n += sc.Page(ctx, pl, pageURL, out)
	}
	if loaded == 0 {
		if lastErr == nil {
			lastErr = ErrNoPages
		}
		return nil, n, lastErr
	}
	return out, n, nil
}

func (r *Runner) load(ctx context.Context, page string) ([]byte, *url.URL, error) {
	ref, err := task.NormalizeRef(page)
	if err != nil {
		return nil, nil, err
	}
	id, err := r.Pool.Acquire(ctx, r.Opt.Tier)
	if err != nil {
		return nil, nil, err
	}
	defer r.Pool.Release(id)
	u, _ := id.Base.Parse(task.RefPath(ref))
	pl, err := r.Loader.Load(ctx, id, u.String())
	if err != nil {
		return nil, nil, err
	}
	return pl.HTML, u, nil
}
