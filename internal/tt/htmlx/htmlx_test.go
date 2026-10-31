package htmlx

import "testing"

const page = `<html><head></head><body><script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">` +
	`{"__DEFAULT_SCOPE__":{"webapp.app-context":{"language":"en","region":"IQ","wid":"7300000000000000001","webIdCreatedTime":"1761900000",` +
	`"odinId":"7300000000000000002","botType":"others","csrfToken":"x"},` +
	`"webapp.user-detail":{"userInfo":{"user":{"id":"6800000000000000001","uniqueId":"someone","secUid":"MS4wLjABAAAA"},` +
	`"statsV2":{"followerCount":"123"}},"statusCode":0}}}</script></body></html>`

func TestExtract(t *testing.T) {
	tok, ok := Extract([]byte(page))
	if !ok {
		t.Fatal("no tokens")
	}
	want := map[string]string{"device_id": "7300000000000000001", "odinId": "7300000000000000002", "WebIdLastTime": "1761900000",
		"region": "IQ", "language": "en", "botType": "others", "csrfToken": "x"}
	for k, v := range want {
		if tok[k] != v {
			t.Errorf("%s = %q, want %q", k, tok[k], v)
		}
	}
	ud, ok := Scope([]byte(page), ScopeUserDetail)
	if !ok || ud.Get("userInfo.user.secUid").String() != "MS4wLjABAAAA" || ud.Get("userInfo.statsV2.followerCount").String() != "123" {
		t.Fatalf("user detail %v %s", ok, ud.Raw)
	}
}

func TestShellAndChallenge(t *testing.T) {
	if _, ok := Extract([]byte(`<html><body><div id="app"></div></body></html>`)); ok {
		t.Fatal("a shell page has no tokens")
	}
	if !IsChallenge([]byte(`<html><title>Please wait...</title><script>document.cookie="_wafchallengeid=1"</script></html>`)) {
		t.Fatal("challenge not recognised")
	}
	if IsChallenge([]byte(page)) {
		t.Fatal("a profile page is not a challenge")
	}
}
