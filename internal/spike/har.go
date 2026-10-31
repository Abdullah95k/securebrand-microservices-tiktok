package spike

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/parse"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fixtures"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
)

// HAR import: a browser recording of a logged-out session is the most
// reliable source of current doc_ids and variable shapes when the bundle
// scan (DocIDs) misses lazily loaded queries. Only request bodies and
// response bodies are read; headers and cookies are never copied.

type harFile struct {
	Log struct {
		Entries []harEntry `json:"entries"`
	} `json:"log"`
}

type harEntry struct {
	Request struct {
		Method   string `json:"method"`
		URL      string `json:"url"`
		PostData *struct {
			Text   string `json:"text"`
			Params []struct {
				Name  string `json:"name"`
				Value string `json:"value"`
			} `json:"params"`
		} `json:"postData"`
	} `json:"request"`
	Response struct {
		Status  int `json:"status"`
		Content struct {
			MimeType string `json:"mimeType"`
			Text     string `json:"text"`
			Encoding string `json:"encoding"`
		} `json:"content"`
	} `json:"response"`
}

// HAROperation is one GraphQL request found in a HAR file.
type HAROperation struct {
	FriendlyName string
	DocID        string
	Variables    map[string]any
	Status       int
	Body         []byte // response body, when the HAR was saved with content
}

// HARPage is an HTML page load found in a HAR file.
type HARPage struct {
	URL  string
	HTML []byte
}

// HARCapture is what ReadHAR extracts.
type HARCapture struct {
	Operations []HAROperation
	Pages      []HARPage
}

// ReadHAR extracts GraphQL operations and Facebook HTML page loads.
func ReadHAR(r io.Reader) (HARCapture, error) {
	var h harFile
	if err := json.NewDecoder(r).Decode(&h); err != nil {
		return HARCapture{}, fmt.Errorf("spike: har: %w", err)
	}
	var out HARCapture
	for _, e := range h.Log.Entries {
		u, err := url.Parse(e.Request.URL)
		if err != nil {
			continue
		}
		body := harContent(e)
		switch {
		case e.Request.Method == "POST" && strings.TrimSuffix(u.Path, "/") == "/api/graphql" && e.Request.PostData != nil:
			form := harForm(e)
			op := HAROperation{FriendlyName: form.Get("fb_api_req_friendly_name"), DocID: form.Get("doc_id"),
				Status: e.Response.Status, Body: body}
			if op.FriendlyName == "" || op.DocID == "" {
				continue
			}
			if v := form.Get("variables"); v != "" {
				_ = json.Unmarshal([]byte(v), &op.Variables)
			}
			out.Operations = append(out.Operations, op)
		case e.Request.Method == "GET" && e.Response.Status == 200 && strings.Contains(e.Response.Content.MimeType, "html") &&
			IsFacebook(u.Scheme+"://"+u.Host) && len(body) > 0:
			out.Pages = append(out.Pages, HARPage{URL: e.Request.URL, HTML: body})
		}
	}
	return out, nil
}

func harContent(e harEntry) []byte {
	c := e.Response.Content
	if c.Text == "" {
		return nil
	}
	if c.Encoding == "base64" {
		b, err := base64.StdEncoding.DecodeString(c.Text)
		if err != nil {
			return nil
		}
		return b
	}
	return []byte(c.Text)
}

func harForm(e harEntry) url.Values {
	pd := e.Request.PostData
	if pd.Text != "" {
		if v, err := url.ParseQuery(pd.Text); err == nil {
			return v
		}
	}
	// Some exporters only fill params, with values still percent-encoded.
	v := url.Values{}
	for _, p := range pd.Params {
		val := p.Value
		if strings.Contains(val, "%") {
			if d, err := url.QueryUnescape(val); err == nil {
				val = d
			}
		}
		v.Add(p.Name, val)
	}
	return v
}

// HARQueryReport compares one registry query with what the HAR showed.
type HARQueryReport struct {
	Query        string   `json:"query"`
	FriendlyName string   `json:"friendly_name"`
	DocID        string   `json:"doc_id,omitempty"`      // observed; empty when not seen
	Kept         string   `json:"kept_doc_id,omitempty"` // not seen; the registry's real doc_id stays
	Requests     int      `json:"requests"`
	Added        []string `json:"variables_added,omitempty"`   // observed, not in the template
	Removed      []string `json:"variables_removed,omitempty"` // in the template, not observed
	IntentTokens []string `json:"intent_tokens,omitempty"`     // commentsIntentToken values seen
	// IntentMissing names the query's comment ordering when no request in
	// the HAR used it; the diff above then covers the other orderings.
	IntentMissing string `json:"intent_missing,omitempty"`
	// Set holds the variables written to the template: added ones, and
	// fixed values that differ from it, which every request sent with
	// the same small value (a flag, an enum, null).
	Set map[string]any `json:"variables_set,omitempty"`
	// Unset lists added variables left out of the template: their value
	// differed between requests or is too large to be a setting.
	Unset []string `json:"variables_unset,omitempty"`
}

// HARResult is the outcome of ImportHAR.
type HARResult struct {
	Seed     registry.Seed    `json:"-"`
	DocIDs   DocIDResult      `json:"doc_ids"`
	Queries  []HARQueryReport `json:"queries"`
	Fixtures []string         `json:"fixtures,omitempty"`
	Other    []string         `json:"other_operations,omitempty"` // friendly names the registry does not use
}

const intentKey = "commentsIntentToken"

// ImportHAR merges the doc_ids seen in c into seed, compares variable
// shapes with the registry templates and, when fixturesDir is set, writes
// the response bodies as scrubbed parser fixtures.
func ImportHAR(c HARCapture, seed registry.Seed, fixturesDir, name string) (HARResult, error) {
	byName := map[string][]HAROperation{}
	for _, op := range c.Operations {
		byName[op.FriendlyName] = append(byName[op.FriendlyName], op)
	}
	found := map[string]string{}
	for fn, ops := range byName {
		counts := map[string]int{}
		best := ""
		for _, op := range ops {
			counts[op.DocID]++
			if counts[op.DocID] >= counts[best] {
				best = op.DocID
			}
		}
		found[fn] = best
	}
	var res HARResult
	res.Seed, res.DocIDs = MergeDocIDs(seed, found)
	// A query this recording lacks is only missing when the registry has
	// no real doc_id for it either: a second HAR (say, of replies only)
	// merged with --registry keeps what the first one found.
	placeholder := map[string]bool{}
	for _, q := range seed.Queries {
		if registry.IsPlaceholderDocID(q.DocID) || q.DocID == "" {
			placeholder[q.FriendlyName] = true
		}
	}
	missing := res.DocIDs.Missing[:0]
	for _, fn := range res.DocIDs.Missing {
		if placeholder[fn] {
			missing = append(missing, fn)
		}
	}
	res.DocIDs.Missing = missing
	used := map[string]bool{}
	for i, q := range seed.Queries {
		used[q.FriendlyName] = true
		r := compareQuery(q, byName[q.FriendlyName], found[q.FriendlyName])
		if r.DocID == "" && !placeholder[q.FriendlyName] {
			r.Kept = q.DocID
		}
		res.Queries = append(res.Queries, r)
		if len(r.Set) > 0 {
			vars, err := setVariables(q.Variables, r.Set)
			if err != nil {
				return res, fmt.Errorf("spike: %s variables: %w", q.Name, err)
			}
			res.Seed.Queries[i].Variables = vars
		}
	}
	for fn := range byName {
		if !used[fn] {
			res.Other = append(res.Other, fn)
		}
	}
	sort.Strings(res.Other)
	if fixturesDir != "" {
		dirs, err := harFixtures(c, seed, fixturesDir, name)
		res.Fixtures = dirs
		if err != nil {
			return res, err
		}
	}
	return res, nil
}

// matching returns the operations of q's friendly name with q's comment
// ordering, or all of them when q has no ordering or none matched; missing
// is q's ordering in the latter case.
func matching(q registry.Query, ops []HAROperation) (out []HAROperation, missing string) {
	var tmpl map[string]any
	_ = json.Unmarshal(q.Variables, &tmpl)
	want, _ := tmpl[intentKey].(string)
	if want == "" {
		return ops, ""
	}
	for _, op := range ops {
		if got, _ := op.Variables[intentKey].(string); got == want {
			out = append(out, op)
		}
	}
	if len(out) == 0 {
		return ops, want
	}
	return out, ""
}

func compareQuery(q registry.Query, ops []HAROperation, docID string) HARQueryReport {
	r := HARQueryReport{Query: q.Name, FriendlyName: q.FriendlyName, DocID: docID}
	ops, missing := matching(q, ops)
	r.Requests = len(ops)
	if len(ops) == 0 {
		return r
	}
	r.IntentMissing = missing
	var tmpl map[string]any
	_ = json.Unmarshal(q.Variables, &tmpl)
	seen := map[string]bool{}
	intents := map[string]bool{}
	for _, op := range ops {
		for k := range op.Variables {
			seen[k] = true
		}
		if t, _ := op.Variables[intentKey].(string); t != "" {
			intents[t] = true
		}
	}
	// What the caller fills in (page ids, feedback ids, cursors): a new
	// variable carrying one of these is per-request data, not a setting.
	callerValues := map[string]bool{}
	for k, tv := range tmpl {
		if s, _ := tv.(string); strings.HasPrefix(s, "{{") {
			for _, op := range ops {
				if v := op.Variables[k]; v != nil {
					b, _ := json.Marshal(v)
					callerValues[string(b)] = true
				}
			}
		}
	}
	isSetting := func(v any) bool {
		b, _ := json.Marshal(v)
		return setting(v) && !callerValues[string(b)]
	}
	for k := range seen {
		v, same := sameValue(ops, k)
		old, inTemplate := tmpl[k]
		if !inTemplate {
			r.Added = append(r.Added, k)
			if same && isSetting(v) {
				r.setVar(k, v)
			} else {
				r.Unset = append(r.Unset, k)
			}
			continue
		}
		// A fixed template value the browser sent differently. Caller
		// values ("{{name}}") and the ordering stay as they are, and so
		// does everything else when the requests are another ordering's
		// (the fallback above).
		if s, _ := old.(string); strings.HasPrefix(s, "{{") || k == intentKey || r.IntentMissing != "" {
			continue
		}
		switch {
		case same && jsonEqual(v, old):
		case same && isSetting(v):
			r.setVar(k, v)
		default:
			r.Unset = append(r.Unset, k)
		}
	}
	for k := range tmpl {
		if !seen[k] {
			r.Removed = append(r.Removed, k)
		}
	}
	sort.Strings(r.Unset)
	for t := range intents {
		r.IntentTokens = append(r.IntentTokens, t)
	}
	sort.Strings(r.Added)
	sort.Strings(r.Removed)
	sort.Strings(r.IntentTokens)
	return r
}

func (r *HARQueryReport) setVar(k string, v any) {
	if r.Set == nil {
		r.Set = map[string]any{}
	}
	r.Set[k] = v
}

// sameValue returns k's value when every request in ops sent k, all with
// the same value. A variable only some requests send is conditional.
func sameValue(ops []HAROperation, k string) (any, bool) {
	first, ok := ops[0].Variables[k]
	if !ok {
		return nil, false
	}
	for _, op := range ops[1:] {
		if v, ok := op.Variables[k]; !ok || !jsonEqual(v, first) {
			return nil, false
		}
	}
	return first, true
}

var idLike = regexp.MustCompile(`\d{6,}`)

// setting reports whether v looks like a client setting (a flag, a small
// number, a short enum or null) rather than per-request or per-session data
// such as an id.
func setting(v any) bool {
	switch v := v.(type) {
	case nil, bool:
		return true
	case float64:
		return math.Abs(v) < 1e6
	case string:
		return len(v) < 40 && !idLike.MatchString(v)
	}
	return false
}

func jsonEqual(a, b any) bool {
	x, err1 := json.Marshal(a)
	y, err2 := json.Marshal(b)
	return err1 == nil && err2 == nil && bytes.Equal(x, y)
}

// setVariables returns the template tmpl with set's keys replaced or added.
func setVariables(tmpl json.RawMessage, set map[string]any) (json.RawMessage, error) {
	vars := map[string]any{}
	if len(tmpl) > 0 {
		d := json.NewDecoder(bytes.NewReader(tmpl))
		d.UseNumber() // numbers the template already has stay exact
		if err := d.Decode(&vars); err != nil {
			return nil, err
		}
	}
	for k, v := range set {
		vars[k] = v
	}
	return json.Marshal(vars)
}

func varString(vars map[string]any, k string) string {
	switch v := vars[k].(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', 0, 64)
	}
	return ""
}

func harFixtures(c HARCapture, seed registry.Seed, root, name string) ([]string, error) {
	date := time.Now().UTC().Format("20060102")
	var pageID int64
	var written []string
	// A recording holds many HTML responses; the page is the first one the
	// page parser recognises.
	var pageHTML []byte
	for _, p := range c.Pages {
		if pg, _ := parse.Page(p.HTML, 0, 0); pg != nil {
			pageID, pageHTML = pg.PageId, p.HTML
			break
		}
	}
	if pageHTML == nil && len(c.Pages) > 0 {
		pageHTML = c.Pages[0].HTML
	}
	// The timeline's id variable names the page when the HTML is absent.
	for _, op := range c.Operations {
		if pageID != 0 {
			break
		}
		for _, q := range seed.Queries {
			if q.Parser == "timeline" && q.FriendlyName == op.FriendlyName {
				pageID, _ = strconv.ParseInt(varString(op.Variables, "id"), 10, 64)
			}
		}
	}
	keep := []string{strconv.FormatInt(pageID, 10)}
	if pageHTML != nil {
		d, err := fixtures.Write(root, "page_html", date+"_"+name, "response.html", pageHTML,
			map[string]any{"page_id": pageID, "source": "har"}, keep)
		if err != nil {
			return written, err
		}
		written = append(written, d)
	}
	done := map[string]bool{}
	for _, q := range seed.Queries {
		if done[q.Parser] {
			continue
		}
		ops, _ := matching(q, byFriendly(c.Operations, q.FriendlyName))
		op, ok := largest(ops)
		if !ok {
			continue
		}
		meta := map[string]any{"page_id": pageID, "source": "har", "doc_id": op.DocID}
		post, comment := parse.FeedbackTarget(varString(op.Variables, "id"))
		switch q.Parser {
		case "comments":
			meta["post_id"] = post
		case "replies":
			meta["post_id"], meta["parent_comment_id"] = post, comment
		}
		d, err := fixtures.Write(root, q.Parser, date+"_"+name, "response.ndjson", op.Body, meta, keep)
		if err != nil {
			return written, err
		}
		written = append(written, d)
		done[q.Parser] = true
	}
	return written, nil
}

// largest returns the successful operation with the largest response that
// carries data: a page of posts or comments, not an error (Facebook sends
// rate limits with HTTP 200) or an empty last page.
func largest(ops []HAROperation) (HAROperation, bool) {
	var best HAROperation
	for _, op := range ops {
		if op.Status == 200 && len(op.Body) > len(best.Body) && graphql.HasData(graphql.SplitParts(op.Body)) {
			best = op
		}
	}
	return best, len(best.Body) > 0
}

func byFriendly(ops []HAROperation, name string) []HAROperation {
	var out []HAROperation
	for _, op := range ops {
		if op.FriendlyName == name {
			out = append(out, op)
		}
	}
	return out
}
