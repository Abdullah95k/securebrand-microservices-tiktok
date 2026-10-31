package htmlx

import (
	"maps"
	"testing"

	scrapehtml "github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/htmlx"
)

const sample = `<html><script type="application/json">{"require":[["ScheduledServerJS","handle",null,[{"__bbox":{"define":[["LSD",[],{"token":"AVqTok3n"},323],["SiteData",[],{"server_revision":1017000000,"haste_session":"20390.HYP:comet","hsi":"7431","__spin_r":1017000000,"__spin_b":"trunk","__spin_t":1730000000},317]]}}]],["CookieCoreConfig","set",null,[["_js_datr","DATR123",1]]]]}</script>
<meta property="al:android:url" content="fb://page/100064000000000"/></html>`

func TestExtractTokens(t *testing.T) {
	tok := ExtractTokens([]byte(sample))
	want := map[string]string{"lsd": "AVqTok3n", "js_datr": "DATR123", "__spin_r": "1017000000", "__spin_b": "trunk",
		"__spin_t": "1730000000", "__hsi": "7431", "__rev": "1017000000", "__hs": "20390.HYP:comet"}
	if !maps.Equal(tok, want) {
		t.Fatalf("got %v\nwant %v", tok, want)
	}
	if !Valid(tok) || ConsentWall([]byte(sample)) {
		t.Fatal("tokens should be valid")
	}
	// "2" + sum of char codes of the lsd.
	sum := 0
	for _, r := range "AVqTok3n" {
		sum += int(r)
	}
	if Jazoest(tok) != "2"+itoa(sum) {
		t.Fatalf("jazoest = %s", Jazoest(tok))
	}
	if id := PageID([]byte(sample)); id != 100064000000000 {
		t.Fatalf("page id = %d", id)
	}
	if n := len(scrapehtml.JSONBlobs([]byte(sample))); n != 1 {
		t.Fatalf("json blobs = %d", n)
	}
}

// A page in the new Pages experience names its older Page object first; the
// profile's id is the one with a timeline.
func TestPageIDPrefersTheProfile(t *testing.T) {
	html := `<script>{"delegate_page":{"id":"104000000000123","category_name":"News"},"pageID":"104000000000123"}</script>` +
		`<script>{"props":{"userID":"100064000000777"}}</script>` +
		`<script>{"CurrentUserInitialData":{"USER_ID":"0"}}</script>`
	if id := PageID([]byte(html)); id != 100064000000777 {
		t.Fatalf("page id = %d, want the profile's", id)
	}
	// A logged-out viewer's own id is never taken for the page's.
	if id := PageID([]byte(`{"userID":"0"},{"pageID":"104000000000123"}`)); id != 104000000000123 {
		t.Fatalf("page id = %d", id)
	}
	// Several userIDs: some are other people's, so none is trusted.
	if id := PageID([]byte(`{"userID":"100012345678901"},{"userID":"100064000000777"},{"pageID":"104000000000123"}`)); id != 104000000000123 {
		t.Fatalf("page id = %d with several userIDs", id)
	}
	// The same userID repeated is still the one.
	if id := PageID([]byte(`{"userID":"100064000000777"},{"userID":"100064000000777"},{"pageID":"104000000000123"}`)); id != 100064000000777 {
		t.Fatalf("page id = %d with a repeated userID", id)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

func TestPageIDHints(t *testing.T) {
	html := `<meta property="al:android:url" content="fb://profile/100064000000777"/>` +
		`<script>{"userID":"100012345678901"},{"userID":"100064000000777"},{"userID":"100064000000777"},{"userID":"0"}</script>` +
		`<script>{"profile_owner":{"id":"100064000000777"},"actorID":"0"}</script>`
	got := PageIDHints([]byte(html))
	want := map[string]int{"userID": 2, "fb://profile/": 1, "al:android:url": 1, "profile_owner": 1, "actorID": 0, "pageID": 0, "delegate_page": 0}
	for k, n := range want {
		if got[k] != n {
			t.Errorf("%s: %d, want %d (all: %v)", k, got[k], n, got)
		}
	}
	// Counts only: no id leaves.
	for k := range got {
		if len(k) > 20 {
			t.Errorf("unexpected key %q", k)
		}
	}
	if n := len(PageIDHints([]byte(`<html></html>`))); n != len(pageIDPatterns)+len(hintPatterns)+1 {
		t.Fatalf("an empty page reports %d keys", n)
	}
}

// The token sources the sample above lacks: DTSG, the second _js_datr
// shape, the lsd form input, client_revision as a fallback for __rev,
// __comet_req and the consent wall.
func TestExtractTokensOtherSources(t *testing.T) {
	html := `{"client_revision":1016999999,"__comet_req":15} ["DTSGInitialData",[],{"token":"DT1"}]` +
		` "_js_datr":{"value":"D2"} <input name="lsd" value="L3"> <div data-cookiebanner="x">`
	want := map[string]string{"lsd": "L3", "fb_dtsg": "DT1", "js_datr": "D2", "__rev": "1016999999", "__comet_req": "15"}
	if got := ExtractTokens([]byte(html)); !maps.Equal(got, want) || !ConsentWall([]byte(html)) {
		t.Fatalf("got %v\nwant %v", got, want)
	}
	// The CSRF source of jazoest is fb_dtsg when present.
	sum := 0
	for _, r := range "DT1" {
		sum += int(r)
	}
	if Jazoest(want) != "2"+itoa(sum) {
		t.Fatalf("jazoest = %s", Jazoest(want))
	}
	// server_revision wins over client_revision wherever each stands.
	if got := ExtractTokens([]byte(`"client_revision":1 "server_revision":2`)); got["__rev"] != "2" || Valid(got) {
		t.Fatalf("got %v", got)
	}
	// The last lsd source in the table wins when several match.
	if got := ExtractTokens([]byte(`["LSD",[],{"token":"A"}] <input name="lsd" value="B">`)); got["lsd"] != "B" {
		t.Fatalf("lsd = %q", got["lsd"])
	}
}
