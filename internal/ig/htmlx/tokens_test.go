package htmlx

import (
	"maps"
	"strconv"
	"testing"
)

// A synthetic page in the shapes the patterns expect (no recorded data).
const page = `<html><script type="application/json" data-sjs>{"require":[["ScheduledServerJS","handle",null,[{"__bbox":{"define":[` +
	`["LSD",[],{"token":"AVlsd1"},323],["InstagramSecurityConfig",[],{"csrf_token":"csrfX1"},1],` +
	`["SiteData",[],{"server_revision":1029000000,"haste_session":"20400.HYP:instagram_web_pkg.2.1...0","hsi":"7560000000000000001",` +
	`"__spin_r":1029000000,"__spin_b":"trunk","__spin_t":1761800000},317],["RelayAPIConfigDefaults",[],{"customHeaders":{"X-IG-App-ID":"936619743392459"}},1],` +
	`["PolarisSiteData",[],{"rollout_hash":"1029000000"},1],["SprinkleConfig",[],{"jazoest":"22001"},1]]}}]]],"__comet_req":7}</script></html>`

func TestExtractTokens(t *testing.T) {
	tok := ExtractTokens([]byte(page))
	want := map[string]string{"lsd": "AVlsd1", "csrf_token": "csrfX1", "app_id": "936619743392459", "jazoest": "22001",
		"__spin_r": "1029000000", "__spin_b": "trunk", "__spin_t": "1761800000", "__rev": "1029000000",
		"__hs": "20400.HYP:instagram_web_pkg.2.1...0", "__hsi": "7560000000000000001", "__comet_req": "7", "rollout_hash": "1029000000"}
	if !maps.Equal(tok, want) {
		t.Fatalf("got  %v\nwant %v", tok, want)
	}
	if !Valid(tok) || Jazoest(tok) != "22001" {
		t.Fatalf("valid %v, jazoest %s", Valid(tok), Jazoest(tok))
	}
	for k, ok := range Found(tok) {
		if !ok {
			t.Errorf("%s not found", k)
		}
	}
}

// Without an LSD nothing can be sent; without a page jazoest the LSD
// checksum stands in; Found names every key, values never.
func TestTokensMissing(t *testing.T) {
	tok := ExtractTokens([]byte(`<html>{"csrf_token":"c"}</html>`))
	if Valid(tok) {
		t.Fatal("no LSD must not be valid")
	}
	found := Found(tok)
	if len(found) != len(Keys) || !found["csrf_token"] || found["lsd"] {
		t.Fatalf("found %v", found)
	}
	sum := 0
	for _, r := range "AB" {
		sum += int(r)
	}
	if got := Jazoest(map[string]string{"lsd": "AB"}); got != "2"+strconv.Itoa(sum) {
		t.Fatalf("jazoest = %s", got)
	}
}
