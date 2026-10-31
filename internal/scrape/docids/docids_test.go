package docids

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"
)

func TestScan(t *testing.T) {
	out := map[string]string{}
	Scan([]byte(`__d("ProfileCometTimelineFeedRefetchQuery_facebookRelayOperation",[],(function(a,b,c,d,e,f){e.exports="7316000000000001"}),null);
		{"CommentsListComponentsPaginationQuery",{"queryID":"7316000000000002"}}`), out)
	want := map[string]string{"ProfileCometTimelineFeedRefetchQuery": "7316000000000001", "CommentsListComponentsPaginationQuery": "7316000000000002"}
	if !reflect.DeepEqual(out, want) {
		t.Fatalf("%v", out)
	}
}

func TestBundles(t *testing.T) {
	page, _ := url.Parse("https://www.facebook.com/brand")
	html := []byte(`<script src="https://static.xx.fbcdn.net/rsrc.php/v3/yA/r/abc.js?_nc_x=1"></script>
		<script src="https://static.xx.fbcdn.net/rsrc.php/v3/yA/r/abc.js?_nc_x=1"></script>
		{"src":"https:\/\/static.xx.fbcdn.net\/rsrc.php\/v3\/yB\/r\/def.js"}
		<script src="//www.facebook.com/rsrc.php/v3/own.js"></script>
		<script src="https://evil.example/rsrc.php/v3/x.js"></script>
		<script src="https://evilfbcdn.net/rsrc.php/v3/y.js"></script>`)
	got := Bundles(html, page)
	want := []string{"https://static.xx.fbcdn.net/rsrc.php/v3/yA/r/abc.js?_nc_x=1", "https://static.xx.fbcdn.net/rsrc.php/v3/yB/r/def.js",
		"https://www.facebook.com/rsrc.php/v3/own.js"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%q", got)
	}
}

func TestScannerFetchesEachBundleOnce(t *testing.T) {
	page, _ := url.Parse("https://www.facebook.com/brand")
	html := []byte(`<script src="https://static.xx.fbcdn.net/rsrc.php/v3/a.js"></script>
		<script src="https://static.xx.fbcdn.net/rsrc.php/v3/b.js"></script>
		<script src="https://static.xx.fbcdn.net/rsrc.php/v3/c.js"></script>`)
	fetched := map[string]int{}
	s := &Scanner{MaxBundles: 2, Fetch: func(_ context.Context, src string) ([]byte, error) {
		fetched[src]++
		if src == "https://static.xx.fbcdn.net/rsrc.php/v3/b.js" && fetched[src] == 1 {
			return nil, errors.New("reset")
		}
		return []byte(`__d("AQuery_facebookRelayOperation",[],(function(a,b,c,d,e,f){e.exports="123456789"}),null);`), nil
	}}
	out := map[string]string{}
	if n := s.Page(context.Background(), html, page, out); n != 2 || out["AQuery"] != "123456789" {
		t.Fatalf("first: %d %v", n, out)
	}
	// a.js is remembered; b.js failed and is fetched again, then c.js.
	out = map[string]string{}
	if n := s.Page(context.Background(), html, page, out); n != 2 || out["AQuery"] != "123456789" {
		t.Fatalf("second: %d %v", n, out)
	}
	if fetched["https://static.xx.fbcdn.net/rsrc.php/v3/a.js"] != 1 || fetched["https://static.xx.fbcdn.net/rsrc.php/v3/b.js"] != 2 ||
		fetched["https://static.xx.fbcdn.net/rsrc.php/v3/c.js"] != 1 {
		t.Fatalf("fetches %v", fetched)
	}
	if n := s.Page(context.Background(), html, page, map[string]string{}); n != 0 {
		t.Fatalf("everything is cached: %d", n)
	}
}

func TestHTTPFetcher(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/missing.js" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte("bundle"))
	}))
	defer srv.Close()
	fetch := HTTPFetcher(srv.Client())
	if b, err := fetch(context.Background(), srv.URL+"/a.js"); err != nil || string(b) != "bundle" {
		t.Fatalf("%q %v", b, err)
	}
	if _, err := fetch(context.Background(), srv.URL+"/missing.js"); err == nil {
		t.Fatal("a 404 must be an error")
	}
}
