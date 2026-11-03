package phase0

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/sign"
)

// Operations a record measures. The first five are what the crawler sends
// in production; the rest are Phase 0's own questions.
const (
	OpPage         = "page"          // a profile page: the identity's bootstrap and the account's page refresh
	OpListCheck    = "list_check"    // the creator list's first page: a scheduled check
	OpListBackfill = "list_backfill" // the next pages, back to the backfill window
	OpComments     = "comments"      // a page of top-level comments (signed)
	OpReplies      = "replies"       // a page of replies (signed)
	OpVideoProxy   = "video_proxy"   // a video file through the identity's exit, with its cookie
	OpVideoDirect  = "video_direct"  // the same file from this machine's own IP (T33)
	OpSigReplay    = "sig_replay"    // a signed URL sent again later from another identity (T27)
	OpLadder       = "ladder"        // a creator-list call at a ladder step's rate (T20)
	OpVariant      = "variant"       // an operation variant tried by the variants step

	// Summary records, one per account or comment read.
	OpAccount     = "account"
	OpCommentRead = "comment_read"
	OpLadderStep  = "ladder_step"
)

// Record is one line of a results file: a request, or a summary of an
// account, a comment read or a ladder step. Records never hold a username,
// a name, an id, a link or any text TikTok served: accounts are their line
// in the accounts file, exits a label (docs/tiktok build brief §7), so the
// bundle can be shared.
type Record struct {
	T       time.Time `json:"t"`
	Step    string    `json:"step"`
	Op      string    `json:"op"`
	Tier    string    `json:"tier"`
	Exit    string    `json:"exit,omitempty"`
	Account int       `json:"account,omitempty"`
	Variant string    `json:"variant,omitempty"`
	Signed  bool      `json:"signed,omitempty"`
	Class   string    `json:"class,omitempty"`
	Status  int       `json:"status,omitempty"`
	// Wire is the bytes on the wire, both ways, compressed: what a proxy
	// bills.
	Wire  int64 `json:"wire,omitempty"`
	Ms    int64 `json:"ms,omitempty"`
	Items int   `json:"items,omitempty"`
	More  bool  `json:"more,omitempty"`
	// Retried marks a soft error the crawler would retry (and the kit did).
	Retried bool `json:"retried,omitempty"`
	// Drift lists the required fields a parser missed.
	Drift string `json:"drift,omitempty"`
	// Reason is the class's reason, with any URL removed.
	Reason string `json:"reason,omitempty"`

	// The signer, on signed calls: the browser time of the signature, of
	// the page opened for it (0 when an open page signed), and whether that
	// page was opened for another account (T32).
	SignMs       int64 `json:"sign_ms,omitempty"`
	PageOpenMs   int64 `json:"page_open_ms,omitempty"`
	CrossAccount bool  `json:"cross_account,omitempty"`
	SignError    bool  `json:"sign_error,omitempty"`

	// sig_replay: the signature's age when it was sent again.
	AgeS int64 `json:"age_s,omitempty"`
	// ladder and ladder_step: the step's rate, requests per second per IP.
	Rate float64 `json:"rate,omitempty"`

	// account: what the list showed within the backfill window.
	Posts      int     `json:"posts,omitempty"`
	VideoPosts int     `json:"video_posts,omitempty"`
	Days       float64 `json:"days,omitempty"`
	Stop       string  `json:"stop,omitempty"`
	// CommentCount is the sum of the posts' comment counts (account), or
	// the post's count (comment_read).
	CommentCount int64 `json:"comment_count,omitempty"`
	// CommentPagesEst is the comment pages the account's posts would take
	// read whole, at 20 a page: Σ ceil(count / 20).
	CommentPagesEst int64 `json:"comment_pages_est,omitempty"`
	// comment_read: comments and replies collected, the pages it took, and
	// whether the read reached the end of every list it started.
	Collected  int  `json:"collected,omitempty"`
	Pages      int  `json:"pages,omitempty"`
	ReplyPages int  `json:"reply_pages,omitempty"`
	Complete   bool `json:"complete,omitempty"`
	// ladder_step: the step's calls and blocks.
	Calls  int `json:"calls,omitempty"`
	Blocks int `json:"blocks,omitempty"`
	// video: the file's size as the host declared it.
	Size int64 `json:"size,omitempty"`
}

// Recorder writes records as JSON lines. It is safe for concurrent use.
type Recorder struct {
	mu   sync.Mutex
	enc  *json.Encoder
	err  error
	n    int
	Step string
	Tier string
	Now  func() time.Time
}

// NewRecorder writes to w; step and tier label every record.
func NewRecorder(w io.Writer, step, tier string) *Recorder {
	return &Recorder{enc: json.NewEncoder(w), Step: step, Tier: tier}
}

// Add writes r, filling its time, step and tier.
func (rc *Recorder) Add(r Record) {
	if rc == nil {
		return
	}
	now := time.Now
	if rc.Now != nil {
		now = rc.Now
	}
	if r.T.IsZero() {
		r.T = now().UTC()
	}
	if r.Step == "" {
		r.Step = rc.Step
	}
	if r.Tier == "" {
		r.Tier = rc.Tier
	}
	r.Reason = Clean(r.Reason)
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if rc.err == nil {
		rc.err = rc.enc.Encode(r)
		rc.n++
	}
}

// Err returns the first write error.
func (rc *Recorder) Err() error {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.err
}

// ReadRecords reads a results file's records; a torn last line (a run
// stopped mid-write) is skipped.
func ReadRecords(r io.Reader) ([]Record, error) {
	var out []Record
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 4<<20)
	for sc.Scan() {
		var rec Record
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			continue
		}
		out = append(out, rec)
	}
	return out, sc.Err()
}

// ReadFile reads a results file; a missing one holds no records.
func ReadFile(path string) ([]Record, error) {
	f, err := os.Open(path) //nolint:gosec // the kit's own output directory
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return ReadRecords(f)
}

var (
	urls        = regexp.MustCompile(`[a-z]+://[^\s"']+`)
	credentials = regexp.MustCompile(`://[^/@\s]+@`)
)

// Clean removes URLs from a reason: they would carry ids, signatures and
// proxy credentials.
func Clean(s string) string {
	s = credentials.ReplaceAllString(s, "://***@")
	return urls.ReplaceAllString(s, "<url>")
}

// Exits labels exits "<tier>/<n>" in the order they are first seen, so the
// records show per-IP behaviour without the IPs or credentials.
type Exits struct {
	mu sync.Mutex
	m  map[string]string
	n  map[string]int
}

// Label returns id's exit label.
func (e *Exits) Label(id *session.Identity) string {
	if id == nil {
		return ""
	}
	ep := id.Endpoint
	key := ep.Tier + "|" + ep.Exit
	if ep.Exit == "" {
		key = ep.Tier + "|" + ep.SessionID // a gateway's sticky session, or direct
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.m == nil {
		e.m, e.n = map[string]string{}, map[string]int{}
	}
	if l, ok := e.m[key]; ok {
		return l
	}
	e.n[ep.Tier]++
	l := ep.Tier + "/" + strconv.Itoa(e.n[ep.Tier])
	e.m[key] = l
	return l
}

// call is what the signer learned about one signed call; the workload
// reads it after the client's Do returns. It travels in the context, which
// the signer pool passes to the page opener and the page.
type call struct {
	account    int
	signMs     atomic.Int64
	pageOpenMs atomic.Int64
	cross      atomic.Bool
	signed     atomic.Bool
	signErr    atomic.Bool
	mu         sync.Mutex
	url        string // the signed URL, for the replays
	referer    string
}

type callKey struct{}

func withCall(ctx context.Context, c *call) context.Context {
	return context.WithValue(ctx, callKey{}, c)
}

func callFrom(ctx context.Context) *call {
	c, _ := ctx.Value(callKey{}).(*call)
	return c
}

// measuredOpener times the opening of each signer page and tags it with
// the account whose call opened it.
func measuredOpener(open sign.Opener) sign.Opener {
	return func(ctx context.Context, id *session.Identity, pageURL string) (sign.Page, error) {
		c := callFrom(ctx)
		start := time.Now()
		pg, err := open(ctx, id, pageURL)
		if c != nil {
			c.pageOpenMs.Add(time.Since(start).Milliseconds())
		}
		if err != nil {
			return nil, err
		}
		acct := 0
		if c != nil {
			acct = c.account
		}
		return &measuredPage{Page: pg, account: acct}, nil
	}
}

type measuredPage struct {
	sign.Page
	account int
}

func (p *measuredPage) Sign(ctx context.Context, rawURL string) (string, error) {
	start := time.Now()
	out, err := p.Page.Sign(ctx, rawURL)
	if c := callFrom(ctx); c != nil {
		c.signMs.Add(time.Since(start).Milliseconds())
		c.cross.Store(c.account != 0 && p.account != 0 && c.account != p.account)
	}
	return out, err
}

// measuredSigner is the client's URL signer around the pool: it marks the
// call signed and keeps the signed URL for the replays.
type measuredSigner struct{ pool *sign.Pool }

func (s measuredSigner) SignURL(ctx context.Context, id *session.Identity, query, rawURL, referer string) (string, error) {
	out, err := s.pool.SignURL(ctx, id, query, rawURL, referer)
	if c := callFrom(ctx); c != nil {
		c.signed.Store(true)
		if err != nil {
			c.signErr.Store(true)
		} else {
			c.mu.Lock()
			c.url, c.referer = out, referer
			c.mu.Unlock()
		}
	}
	return out, err
}

// SignOutcome passes how a signed call was answered to the pool, which
// replaces a page whose signatures keep being refused.
func (s measuredSigner) SignOutcome(ctx context.Context, sentURL string, cls detect.Class) {
	s.pool.SignOutcome(ctx, sentURL, cls)
}
