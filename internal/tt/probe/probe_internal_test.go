package probe

import (
	"testing"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/graphql"
	ttdetect "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/detect"
)

// Answers read from the page or replayed are classed as the client would:
// an error statusCode is not "ok", a captcha is counted as one.
func TestClassifyAnswer(t *testing.T) {
	r := &runner{o: Options{Client: &graphql.Client{Rules: ttdetect.Rules()}}}
	for body, want := range map[string]detect.Class{
		`{"status_code":0,"comments":[{"cid":"1"}],"total":1}`: detect.OK,
		`{"status_code":0,"comments":[],"total":0}`:            detect.OK,
		``: detect.RegistryStale,
		`{"userInfo":{"user":{},"stats":{},"shareMeta":{}}}`:                  detect.RegistryStale,
		`{"statusCode":10000,"statusMsg":"","verify_event":{"type":"slide"}}`: detect.Checkpoint,
	} {
		if got, reason := r.classifyAnswer(200, "", "application/json", []byte(body)); got != want {
			t.Errorf("%q: %v (%s), want %v", body, got, reason, want)
		}
	}
	if got, _ := r.classifyAnswer(200, "", "application/json", []byte(`{"statusCode":10201,"statusMsg":"invalid count parameter"}`)); got == detect.OK {
		t.Error("an error statusCode must not read as ok")
	}
}

// Only the parameter asked for changes; the others keep their place and
// their encoding.
func TestEditQuery(t *testing.T) {
	for _, c := range []struct {
		in   string
		set  map[string]string
		drop []string
		want string
	}{
		{"https://x/api/a/?b=1&cursor=0&a=%20x&X-Gnarly=g", map[string]string{"cursor": "20"}, nil,
			"https://x/api/a/?b=1&cursor=20&a=%20x&X-Gnarly=g"},
		{"https://x/api/a/?b=1&cursor=0&X-Bogus=1&msToken=m&X-Gnarly=g", map[string]string{"cursor": "16"}, signatureParams,
			"https://x/api/a/?b=1&cursor=16"},
		{"https://x/api/a/?b=1", map[string]string{"cursor": "5"}, nil, "https://x/api/a/?b=1&cursor=5"},
	} {
		if got := editQuery(c.in, c.set, c.drop); got != c.want {
			t.Errorf("editQuery(%s) = %s, want %s", c.in, got, c.want)
		}
	}
}

// tks's in-page URLs say what the browser is.
func TestPageParams(t *testing.T) {
	base := map[string]string{"tz_name": "Asia/Baghdad", "screen_width": "1920", "screen_height": "1080", "history_len": "3", "aid": "1988"}
	got := pageParams(base, map[string]any{"time_zone": "UTC", "screen": "800x600", "history": float64(2)})
	if got["tz_name"] != "UTC" || got["screen_width"] != "800" || got["screen_height"] != "600" || got["history_len"] != "2" || got["aid"] != "1988" {
		t.Fatalf("page params %v", got)
	}
	if base["tz_name"] != "Asia/Baghdad" {
		t.Fatal("the base params must not change")
	}
	if got := pageParams(base, nil); got["tz_name"] != "Asia/Baghdad" {
		t.Fatalf("no env keeps the defaults: %v", got)
	}
}

// A comment read twice (plain and browser) is kept once.
func TestAddComment(t *testing.T) {
	a := &Account{}
	addComment(a, Comment{ID: "1"})
	addComment(a, Comment{ID: "1"})
	addComment(a, Comment{ID: "2"})
	if len(a.Comments) != 2 {
		t.Fatalf("comments %v", a.Comments)
	}
}
