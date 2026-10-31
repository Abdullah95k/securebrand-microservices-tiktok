package spike

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/task"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/parse"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
)

// TrimOptions configures the payload experiment: which changes to the
// timeline and comments queries' variables are measured against the
// production request.
type TrimOptions struct {
	// Each also switches every provider flag off on its own, to see what
	// each one costs; otherwise only all of them at once.
	Each bool
	// TimelineCounts are timeline "count" values measured besides the
	// crawler's (TimelineCount).
	TimelineCounts []int
	// TimelineCount is the crawler's crawler.timeline_page_size.
	TimelineCount int
	// CommentCounts are commentsAfterCount values measured besides the
	// template's.
	CommentCounts []int
}

// TrimRecord is one request of the payload experiment: what came back and
// what the parser read from it, never its content.
type TrimRecord struct {
	Type     string    `json:"type"` // "trim"
	At       time.Time `json:"at"`
	Tier     string    `json:"tier"`
	Page     string    `json:"page"`
	Query    string    `json:"query"`
	Variant  string    `json:"variant"`
	Class    string    `json:"class"`
	Status   int       `json:"status,omitempty"`
	Bytes    int       `json:"bytes"`      // response body
	Wire     int64     `json:"wire_bytes"` // both ways on the wire, as proxies bill it
	Items    int       `json:"items"`      // posts or comments parsed
	Missing  []string  `json:"missing,omitempty"`
	Optional []string  `json:"optional,omitempty"`
	Error    string    `json:"error,omitempty"`
}

// BaselineVariant is the request the crawler makes today.
const BaselineVariant = "baseline"

// providerPrefix starts the Relay provider flags, which switch parts of a
// response on (most of them render-only).
const providerPrefix = "__relay_internal__pv__"

type trimVariant struct {
	name    string
	set     map[string]any // template variables replaced
	vars    map[string]any // placeholder values
	referer string         // as the crawler sends it
}

// providerFlags returns the template's provider flags that are on.
func providerFlags(tmpl json.RawMessage) []string {
	var vars map[string]any
	if json.Unmarshal(tmpl, &vars) != nil {
		return nil
	}
	var out []string
	for k, v := range vars {
		if on, ok := v.(bool); ok && on && strings.HasPrefix(k, providerPrefix) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func trimVariants(tmpl json.RawMessage, base map[string]any, referer string, each bool) []trimVariant {
	vs := []trimVariant{{name: BaselineVariant, vars: base, referer: referer}}
	flags := providerFlags(tmpl)
	if len(flags) == 0 {
		return vs
	}
	off := map[string]any{}
	for _, f := range flags {
		off[f] = false
	}
	vs = append(vs, trimVariant{name: "flags_off", set: off, vars: base, referer: referer})
	if each {
		for _, f := range flags {
			vs = append(vs, trimVariant{name: "off:" + strings.TrimPrefix(f, providerPrefix), set: map[string]any{f: false}, vars: base, referer: referer})
		}
	}
	return vs
}

func withVar(m map[string]any, k string, v any) map[string]any {
	out := make(map[string]any, len(m)+1)
	for x, y := range m {
		out[x] = y
	}
	out[k] = v
	return out
}

// trimCommentQueries are the comment queries the crawler sends: full reads,
// and the newest-first reads of the refreshes.
var trimCommentQueries = []string{"comments", "comments_newest"}

// Trim runs the payload experiment on pages: for each, the timeline and
// then the comments of its busiest post, once per variant. It returns the
// records it also writes to Out.
func (r *Runner) Trim(ctx context.Context, pages []string, opt TrimOptions) []TrimRecord {
	var all []TrimRecord
	for _, page := range pages {
		if ctx.Err() != nil {
			break
		}
		all = append(all, r.trimPage(ctx, page, opt)...)
	}
	return all
}

func (r *Runner) trimPage(ctx context.Context, page string, opt TrimOptions) []TrimRecord {
	var out []TrimRecord
	fail := func(query string, err error) []TrimRecord {
		return append(out, r.emitTrim(TrimRecord{Page: page, Query: query, Class: "error", Error: Redact(err.Error())}))
	}
	ref, err := task.NormalizeRef(page)
	if err != nil {
		return fail("page", err)
	}
	id, pl, _, err := r.loadPage(ctx, ref, nil)
	if err != nil {
		return fail("page", err)
	}
	defer r.Pool.Release(id)
	if pl.Class != detect.OK || !id.Bootstrapped() {
		return fail("page", fmt.Errorf("%w: %s (%s)", session.ErrBootstrap, pl.Class, pl.Reason))
	}
	pg, _ := parse.Page(pl.HTML, 0, task.RefID(ref))
	if pg == nil {
		return fail("page", fmt.Errorf("spike: page id not found in html"))
	}

	tq, err := r.GQL.Registry.Get("timeline")
	if err != nil {
		return fail("timeline", err)
	}
	count := max(opt.TimelineCount, 1)
	base := map[string]any{"page_id": strconv.FormatInt(pg.PageId, 10), "count": count}
	vs := trimVariants(tq.Variables, base, id.Base.String(), opt.Each)
	for _, n := range opt.TimelineCounts {
		if n != count {
			vs = append(vs, trimVariant{name: "count=" + strconv.Itoa(n), vars: withVar(base, "count", n), referer: id.Base.String()})
		}
	}
	var posts []parse.PostBundle
	for _, v := range vs {
		rec, resp := r.trimDo(ctx, id, ref, tq, v)
		if resp != nil && resp.Class == detect.OK {
			tl, rep := parse.Timeline(resp.Parts, pg.PageId, id.Base.String())
			rec.Items, rec.Missing, rec.Optional = len(tl.Posts), rep.Missing, rep.Optional
			if tl.NoFeed {
				rec.Missing = append(rec.Missing, "timeline.feed")
			}
			if v.name == BaselineVariant {
				posts = tl.Posts
			}
		}
		out = append(out, r.emitTrim(rec))
	}

	// The busiest post the baseline returned: the most comments to page.
	var post *parse.PostBundle
	for i := range posts {
		if posts[i].Post.GetCommentCount() > 0 && (post == nil || posts[i].Post.GetCommentCount() > post.Post.GetCommentCount()) {
			post = &posts[i]
		}
	}
	if post == nil {
		return out
	}
	fid := post.Post.FeedbackId
	if fid == "" {
		fid = parse.FeedbackID(post.Post.PostId)
	}
	cbase := map[string]any{"feedback_id": fid}
	cc := parse.CommentContext{PostID: post.Post.PostId, PageID: pg.PageId}
	for _, name := range trimCommentQueries {
		cq, err := r.GQL.Registry.Get(name)
		if err != nil {
			continue // a registry without this ordering
		}
		cvs := trimVariants(cq.Variables, cbase, post.Post.Permalink, opt.Each)
		for _, n := range opt.CommentCounts {
			cvs = append(cvs, trimVariant{name: "after_count=" + strconv.Itoa(n), set: map[string]any{"commentsAfterCount": n}, vars: cbase,
				referer: post.Post.Permalink})
		}
		for _, v := range cvs {
			rec, resp := r.trimDo(ctx, id, ref, cq, v)
			if resp != nil && resp.Class == detect.OK {
				res, rep := parse.Comments(resp.Parts, cc)
				rec.Items, rec.Missing, rec.Optional = len(res.Comments), rep.Missing, rep.Optional
			}
			out = append(out, r.emitTrim(rec))
		}
	}
	return out
}

// trimDo sends q with v's changes, on a copy of the client that has no
// breakers or raw capture: an experiment must not trip production state.
func (r *Runner) trimDo(ctx context.Context, id *session.Identity, ref string, q registry.Query, v trimVariant) (TrimRecord, *graphql.Response) {
	rec := TrimRecord{Page: ref, Query: q.Name, Variant: v.name}
	if len(v.set) > 0 {
		tmpl, err := setVariables(q.Variables, v.set)
		if err != nil {
			rec.Class, rec.Error = "error", err.Error()
			return rec, nil
		}
		q.Variables = tmpl
	}
	c := *r.GQL
	c.Registry = registry.NewStatic(registry.Seed{ClientParams: r.GQL.Registry.ClientParams(), Queries: []registry.Query{q}})
	c.Breakers, c.Raw = nil, nil
	resp, err := c.Do(ctx, id, graphql.Request{Query: q.Name, Vars: v.vars, Referer: v.referer})
	if err != nil {
		rec.Class, rec.Error = detect.Transport.String(), Redact(err.Error())
		return rec, nil
	}
	rec.Class, rec.Status, rec.Bytes, rec.Wire = resp.Class.String(), resp.Status, len(resp.Body), resp.BytesIn+resp.BytesOut
	return rec, resp
}

func (r *Runner) emitTrim(rec TrimRecord) TrimRecord {
	rec.Type, rec.At, rec.Tier = "trim", time.Now().UTC(), r.Opt.Tier
	b, _ := json.Marshal(rec)
	r.mu.Lock()
	_, _ = r.Out.Write(append(b, '\n'))
	r.mu.Unlock()
	return rec
}

// ReadTrim reads the trim records of a results file.
func ReadTrim(path string) ([]TrimRecord, error) {
	f, err := os.Open(path) //nolint:gosec // the operator's results file
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return readTrim(f)
}

func readTrim(rd io.Reader) ([]TrimRecord, error) {
	var out []TrimRecord
	sc := bufio.NewScanner(rd)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var rec TrimRecord
		if json.Unmarshal(sc.Bytes(), &rec) == nil && rec.Type == "trim" {
			out = append(out, rec)
		}
	}
	return out, sc.Err()
}

// TrimRow summarises one variant of one query over the measured pages.
type TrimRow struct {
	Query, Variant string
	Requests, OK   int
	MedianWire     int64
	// WirePerItem is the wire bytes per post or comment parsed, over the
	// OK requests: what a variant costs for the same data.
	WirePerItem float64
	MedianItems int
	// Delta is WirePerItem against the baseline's (-0.4: 40% cheaper).
	Delta float64
	// Missing are the required fields any request lacked; LostOptional
	// the optional fields it lacked that the baseline had.
	Missing, LostOptional []string
	// Size: the variant changes the page size, so fewer items per request
	// is expected; it is judged on cost per item only.
	Size bool
}

// Usable reports whether the variant served the same data: every request
// OK, no required field missing, and (unless it changes the page size) as
// many items as the baseline.
func (t TrimRow) Usable(base TrimRow) bool {
	return t.Requests > 0 && t.OK == t.Requests && len(t.Missing) == 0 && (t.Size || t.MedianItems >= base.MedianItems)
}

func sizeVariant(name string) bool {
	return strings.HasPrefix(name, "count=") || strings.HasPrefix(name, "after_count=")
}

// TrimSummary groups records by query and variant, baseline first.
func TrimSummary(recs []TrimRecord) []TrimRow {
	type key struct{ q, v string }
	groups := map[key][]TrimRecord{}
	var order []key
	for _, r := range recs {
		k := key{r.Query, r.Variant}
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], r)
	}
	rows := make([]TrimRow, 0, len(order))
	opts := map[key]map[string]bool{} // optional fields each variant lacked
	for _, k := range order {
		g := groups[k]
		row := TrimRow{Query: k.q, Variant: k.v, Requests: len(g), Size: sizeVariant(k.v)}
		var wires []int64
		var items []int
		var okWire int64
		var okItems int
		missing, optional := map[string]bool{}, map[string]bool{}
		for _, r := range g {
			for _, m := range r.Missing {
				missing[m] = true
			}
			for _, o := range r.Optional {
				optional[o] = true
			}
			if r.Class != detect.OK.String() {
				continue
			}
			row.OK++
			wires = append(wires, r.Wire)
			items = append(items, r.Items)
			okWire += r.Wire
			okItems += r.Items
		}
		if len(wires) > 0 {
			slices.Sort(wires)
			slices.Sort(items)
			row.MedianWire, row.MedianItems = wires[len(wires)/2], items[len(items)/2]
		}
		if okItems > 0 {
			row.WirePerItem = float64(okWire) / float64(okItems)
		}
		row.Missing = keys(missing)
		opts[k] = optional
		rows = append(rows, row)
	}
	for i := range rows {
		base, ok := findRow(rows, rows[i].Query, BaselineVariant)
		if !ok {
			continue
		}
		if base.WirePerItem > 0 && rows[i].WirePerItem > 0 {
			rows[i].Delta = rows[i].WirePerItem/base.WirePerItem - 1
		}
		for o := range opts[key{rows[i].Query, rows[i].Variant}] {
			if !opts[key{rows[i].Query, BaselineVariant}][o] {
				rows[i].LostOptional = append(rows[i].LostOptional, o)
			}
		}
		sort.Strings(rows[i].LostOptional)
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].Query != rows[j].Query {
			return queryOrder(rows[i].Query) < queryOrder(rows[j].Query)
		}
		if (rows[i].Variant == BaselineVariant) != (rows[j].Variant == BaselineVariant) {
			return rows[i].Variant == BaselineVariant
		}
		return rows[i].Delta < rows[j].Delta
	})
	return rows
}

// queryOrder lists the timeline first, then the comment queries.
func queryOrder(q string) string {
	if q == "timeline" {
		return ""
	}
	return q
}

func findRow(rows []TrimRow, query, variant string) (TrimRow, bool) {
	for _, r := range rows {
		if r.Query == query && r.Variant == variant {
			return r, true
		}
	}
	return TrimRow{}, false
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// FormatTrim renders the summary: per query, each variant's cost for the
// same data against the baseline, and whether it is usable.
func FormatTrim(rows []TrimRow) string {
	var b strings.Builder
	query := ""
	for _, r := range rows {
		if r.Query != query {
			query = r.Query
			fmt.Fprintf(&b, "\n== %s ==\n%-44s %6s %10s %9s %6s %8s  %s\n", query, "variant", "ok", "wire/req", "wire/item", "items", "vs base", "verdict")
		}
		base, _ := findRow(rows, r.Query, BaselineVariant)
		verdict := "same data"
		switch {
		case r.Variant == BaselineVariant:
			verdict = "production request"
		case !r.Usable(base):
			verdict = "NOT usable"
			if len(r.Missing) > 0 {
				verdict += ": missing " + strings.Join(r.Missing, ",")
			} else if r.OK < r.Requests {
				verdict += fmt.Sprintf(": %d of %d requests not ok", r.Requests-r.OK, r.Requests)
			} else {
				verdict += ": fewer items"
			}
		case len(r.LostOptional) > 0:
			verdict = "loses optional " + strings.Join(r.LostOptional, ",")
		}
		delta := "-"
		if r.Variant != BaselineVariant && r.Delta != 0 {
			delta = fmt.Sprintf("%+.0f%%", r.Delta*100)
		}
		fmt.Fprintf(&b, "%-44s %3d/%-2d %10s %9s %6d %8s  %s\n", r.Variant, r.OK, r.Requests,
			human(float64(r.MedianWire)), human(r.WirePerItem), r.MedianItems, delta, verdict)
	}
	b.WriteString("\nwire/item is what a variant costs for the same posts or comments. A variant is usable when every request was ok, " +
		"no required field went missing and (except for page sizes, count= and after_count=) it returned as many items as the baseline. " +
		"A bigger page that is usable saves requests: compare items per request.\n")
	return b.String()
}
