package spike

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/parse"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
)

const harCookie = "datr=HARCOOKIEVALUE123"

func fixture(t *testing.T, query, file string) []byte {
	t.Helper()
	name := "fakefb_first_page"
	if query == "page_html" {
		name = "fakefb_basic"
	}
	b, err := os.ReadFile(filepath.Join("../../testdata/fb", query, name, file))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func harEntryJSON(method, rawURL string, form url.Values, paramsOnly bool, body []byte, b64 bool) map[string]any {
	req := map[string]any{"method": method, "url": rawURL,
		"headers": []map[string]string{{"name": "cookie", "value": harCookie}, {"name": "x-fb-lsd", "value": "LSDTOKEN"}},
		"cookies": []map[string]string{{"name": "datr", "value": "HARCOOKIEVALUE123"}}}
	if form != nil {
		pd := map[string]any{"mimeType": "application/x-www-form-urlencoded"}
		if paramsOnly {
			var ps []map[string]string
			for k, vs := range form {
				ps = append(ps, map[string]string{"name": k, "value": url.QueryEscape(vs[0])})
			}
			pd["params"] = ps
		} else {
			pd["text"] = form.Encode()
		}
		req["postData"] = pd
	}
	content := map[string]any{"mimeType": "text/html; charset=utf-8", "text": string(body)}
	if b64 {
		content["text"], content["encoding"] = base64.StdEncoding.EncodeToString(body), "base64"
	}
	return map[string]any{"request": req, "response": map[string]any{"status": 200, "content": content,
		"headers": []map[string]string{{"name": "set-cookie", "value": harCookie}}}}
}

func gqlForm(friendly, docID string, vars map[string]any) url.Values {
	vb, _ := json.Marshal(vars)
	return url.Values{"fb_api_req_friendly_name": {friendly}, "doc_id": {docID}, "variables": {string(vb)}, "lsd": {"LSDTOKEN"}}
}

func sampleHAR(t *testing.T) []byte {
	t.Helper()
	feedback := base64.StdEncoding.EncodeToString([]byte("feedback:1200001008975"))
	replyFeedback := base64.StdEncoding.EncodeToString([]byte("feedback:1200001008975_1200001013840"))
	gql := "https://www.facebook.com/api/graphql/"
	entries := []map[string]any{
		harEntryJSON("GET", "https://www.facebook.com/ajax/bz", nil, false, []byte("<html>not a page</html>"), false),
		harEntryJSON("GET", "https://www.facebook.com/brand0", nil, false, fixture(t, "page_html", "response.html"), false),
		harEntryJSON("GET", "https://static.xx.fbcdn.net/rsrc.php/v3/x.js", nil, false, []byte("js"), false),
		// An empty last page first: the fixture must be the full one.
		harEntryJSON("POST", gql, gqlForm("ProfileCometTimelineFeedRefetchQuery", "111", map[string]any{
			"count": 3, "cursor": "AQHtail", "id": "100064000000000", "renderLocation": "profile", "newKnob": true,
			"scale": 2, "trackingCode": "a", "userID": "100064000000000", "onlyFirst": true,
		}), false, []byte(`{"data":{"node":{"timeline_list_feed_units":{"edges":[]}}}}`), false),
		// A rate-limit error, larger than the empty page above.
		harEntryJSON("POST", gql, gqlForm("ProfileCometTimelineFeedRefetchQuery", "111", map[string]any{
			"count": 3, "cursor": "AQHtail", "id": "100064000000000", "renderLocation": "timeline", "newKnob": true,
			"scale": 2, "trackingCode": "a", "userID": "100064000000000",
		}), false, []byte(`{"errors":[{"message":"Rate limit exceeded","severity":"CRITICAL","code":1675004}],"extensions":{"is_final":true}}`), false),
		harEntryJSON("POST", gql, gqlForm("ProfileCometTimelineFeedRefetchQuery", "111", map[string]any{
			"count": 3, "cursor": nil, "id": "100064000000000", "renderLocation": "timeline", "newKnob": true,
			"scale": 2, "trackingCode": "b", "userID": "100064000000000",
		}), false, fixture(t, "timeline", "response.ndjson"), true),
		harEntryJSON("POST", gql, gqlForm("CommentsListComponentsPaginationQuery", "222", map[string]any{
			"commentsIntentToken": "RANKED_UNFILTERED_CHRONOLOGICAL_REPLIES_INTENT_V1", "id": feedback, "commentsAfterCursor": nil,
			"feedLocation": "COMET_MEDIA_VIEWER",
		}), true, fixture(t, "comments", "response.ndjson"), false),
		harEntryJSON("POST", gql, gqlForm("CommentsListComponentsPaginationQuery", "222", map[string]any{
			"commentsIntentToken": "RANKED_FILTERED_INTENT_V1", "id": feedback,
		}), false, nil, false),
		harEntryJSON("POST", gql, gqlForm("Depth1CommentsListPaginationQuery", "333", map[string]any{
			"expansionToken": "tok", "id": replyFeedback,
		}), false, fixture(t, "replies", "response.ndjson"), false),
		harEntryJSON("POST", gql, gqlForm("SomethingElseQuery", "444", map[string]any{}), false, []byte("{}"), false),
	}
	b, err := json.Marshal(map[string]any{"log": map[string]any{"version": "1.2", "entries": entries}})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestHARImport(t *testing.T) {
	c, err := ReadHAR(bytes.NewReader(sampleHAR(t)))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Operations) != 7 || len(c.Pages) != 2 {
		t.Fatalf("operations %d pages %d", len(c.Operations), len(c.Pages))
	}
	root := t.TempDir()
	res, err := ImportHAR(c, embeddedSeed(t), root, "brand0")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.DocIDs.Missing) != 0 || res.DocIDs.Found["CommentsListComponentsPaginationQuery"] != "222" {
		t.Fatalf("doc_ids: %+v", res.DocIDs)
	}
	for _, q := range res.Seed.Queries {
		if q.DocID == "" || strings.HasPrefix(q.DocID, "9000") {
			t.Fatalf("%s not updated: %s", q.Name, q.DocID)
		}
	}
	if len(res.Other) != 1 || res.Other[0] != "SomethingElseQuery" {
		t.Fatalf("other: %v", res.Other)
	}
	by := map[string]HARQueryReport{}
	for _, q := range res.Queries {
		by[q.Query] = q
	}
	// Settings sent by every request are set. A value that changes, a
	// variable only some requests send and one carrying the page id are
	// not; neither is a template value the requests disagree on.
	if tl := by["timeline"]; strings.Join(tl.Added, ",") != "newKnob,onlyFirst,trackingCode,userID" || !contains(tl.Removed, "feedLocation") ||
		len(tl.Set) != 2 || tl.Set["newKnob"] != true || tl.Set["scale"] != float64(2) ||
		strings.Join(tl.Unset, ",") != "onlyFirst,renderLocation,trackingCode,userID" {
		t.Fatalf("timeline diff: %+v", tl)
	}
	if c := by["comments"]; c.Set["feedLocation"] != "COMET_MEDIA_VIEWER" {
		t.Fatalf("comments diff: %+v", c)
	}
	// The registry gets the settings the browser sent; caller values and
	// the comment ordering stay templates.
	vars := map[string]map[string]any{}
	for _, q := range res.Seed.Queries {
		v := map[string]any{}
		if err := json.Unmarshal(q.Variables, &v); err != nil {
			t.Fatal(err)
		}
		vars[q.Name] = v
	}
	if tl := vars["timeline"]; tl["newKnob"] != true || tl["scale"] != float64(2) || tl["count"] != "{{count}}" ||
		tl["cursor"] != "{{cursor}}" || tl["id"] != "{{page_id}}" || tl["trackingCode"] != nil {
		t.Fatalf("timeline template: %v", tl)
	}
	// comments_newest was not recorded: the other orderings' requests
	// change neither its ordering nor its fixed values.
	if n := vars["comments_newest"]; n[intentKey] != "RECENT_ACTIVITY_INTENT_V1" || n["feedLocation"] != "DEDICATED_COMMENTING_SURFACE" {
		t.Fatalf("comments_newest changed: %v", n)
	}
	if c := by["comments"]; c.Requests != 1 || strings.Join(c.IntentTokens, ",") != "RANKED_UNFILTERED_CHRONOLOGICAL_REPLIES_INTENT_V1" {
		t.Fatalf("comments: %+v", c)
	}
	if c := by["comments_newest"]; c.Requests != 2 || c.IntentMissing != "RECENT_ACTIVITY_INTENT_V1" { // falls back to every ordering
		t.Fatalf("comments_newest: %+v", c)
	}

	if len(res.Fixtures) != 4 {
		t.Fatalf("fixtures: %v", res.Fixtures)
	}
	checks, err := CheckFixtures(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range checks {
		if !c.OK() {
			t.Fatalf("fixture from HAR does not parse: %+v", c)
		}
	}
	err = filepathWalk(root, func(p string, b []byte) {
		if bytes.Contains(b, []byte("HARCOOKIEVALUE123")) {
			t.Errorf("%s contains the HAR cookie", p)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	var meta map[string]any
	b, _ := os.ReadFile(filepath.Join(res.Fixtures[3], "meta.json"))
	_ = json.Unmarshal(b, &meta)
	if meta["parent_comment_id"] != float64(1200001013840) || meta["__scrubbed"] != true {
		t.Fatalf("replies meta: %v", meta)
	}
}

// A second recording of replies only, merged into the registry the first
// one wrote, keeps the doc_ids it lacks and is not missing anything.
func TestHARMergeSecondRecording(t *testing.T) {
	base, _ := MergeDocIDs(embeddedSeed(t), map[string]string{
		"ProfileCometTimelineFeedRefetchQuery": "111", "CommentsListComponentsPaginationQuery": "222"})
	replyFeedback := base64.StdEncoding.EncodeToString([]byte("feedback:1200001008975_1200001013840"))
	b, _ := json.Marshal(map[string]any{"log": map[string]any{"entries": []map[string]any{
		harEntryJSON("POST", "https://www.facebook.com/api/graphql/", gqlForm("Depth1CommentsListPaginationQuery", "333",
			map[string]any{"expansionToken": "tok", "id": replyFeedback}), false, fixture(t, "replies", "response.ndjson"), false),
	}}})
	c, err := ReadHAR(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	res, err := ImportHAR(c, base, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.DocIDs.Missing) != 0 {
		t.Fatalf("missing: %v", res.DocIDs.Missing)
	}
	by := map[string]HARQueryReport{}
	for _, q := range res.Queries {
		by[q.Query] = q
	}
	if by["timeline"].Kept != "111" || by["comments_newest"].Kept != "222" || by["replies"].DocID != "333" {
		t.Fatalf("reports: %+v", res.Queries)
	}
	for _, q := range res.Seed.Queries {
		if registry.IsPlaceholderDocID(q.DocID) {
			t.Fatalf("%s still has a placeholder", q.Name)
		}
	}
	// Against the embedded seed the same recording misses the rest.
	res, _ = ImportHAR(c, embeddedSeed(t), "", "")
	if len(res.DocIDs.Missing) != 2 {
		t.Fatalf("missing against the seed: %v", res.DocIDs.Missing)
	}
}

// A rate-limit error (HTTP 200, as Facebook sends it) never becomes the
// fixture, even when it is larger than the only page with data.
func TestLargestSkipsErrors(t *testing.T) {
	empty := HAROperation{Status: 200, Body: []byte(`{"data":{"node":{"edges":[]}}}`)}
	limited := HAROperation{Status: 200, Body: []byte(`{"errors":[{"message":"Rate limit exceeded","severity":"CRITICAL","code":1675004}],"extensions":{"is_final":true}}`)}
	if op, ok := largest([]HAROperation{limited, empty, {Status: 500, Body: []byte(`{"data":{"node":{"edges":[1,2,3,4,5,6,7,8,9]}}}`)}}); !ok || string(op.Body) != string(empty.Body) {
		t.Fatalf("got %s, %v", op.Body, ok)
	}
	if _, ok := largest([]HAROperation{limited}); ok {
		t.Fatal("an error alone is no fixture")
	}
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func filepathWalk(root string, fn func(string, []byte)) error {
	return filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		fn(p, b)
		return nil
	})
}

func TestFeedbackIDs(t *testing.T) {
	enc := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
	if p, c := parse.FeedbackTarget(enc("feedback:12_34")); p != 12 || c != 34 {
		t.Fatal(p, c)
	}
	if p, c := parse.FeedbackTarget(enc("feedback:12")); p != 12 || c != 0 {
		t.Fatal(p, c)
	}
	if p, _ := parse.FeedbackTarget("not base64!"); p != 0 {
		t.Fatal(p)
	}
}
