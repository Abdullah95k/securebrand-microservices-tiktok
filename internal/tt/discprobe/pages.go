package discprobe

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"

	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/phase0"
)

// PageCapture is what TikTok's own page called on one source's page, with
// a few scrolls: for the calls not recorded yet (T36).
type PageCapture struct {
	Page     string     `json:"page"`
	Error    string     `json:"error,omitempty"`
	Scrolls  int        `json:"scrolls"`
	MemoryMB float64    `json:"memory_mb"`
	Calls    []PageCall `json:"calls"`
}

// PageCall is one of the page's API calls: its path, its parameters'
// names, the values of those that name nothing (counts, cursors, scenes,
// flags), and its answer's size and shape.
type PageCall struct {
	Path    string            `json:"path"`
	Params  []string          `json:"params"`
	Values  map[string]string `json:"values,omitempty"`
	Signed  bool              `json:"signed"`
	Status  int               `json:"status"`
	Bytes   int               `json:"bytes"`
	Code    int64             `json:"code,omitempty"`
	Keys    []string          `json:"keys,omitempty"`
	Posts   int               `json:"posts"`
	Users   int               `json:"users"`
	HasMore *bool             `json:"has_more,omitempty"`
}

// safeParams name nothing personal: their values are kept.
var safeParams = map[string]bool{"count": true, "cursor": true, "offset": true, "scene": true, "from_page": true, "search_source": true,
	"type": true, "minCursor": true, "maxCursor": true, "device_type": true, "is_non_personalized_search": true, "web_search_code": true,
	"pullType": true, "coverFormat": true, "needPinnedItemIds": true, "post_item_list_request_type": true, "region": true,
	"tz_name": true, "user_is_login": true, "aid": true, "app_name": true, "channel": true, "device_platform": true, "language": true,
	"enter_from": true, "is_landing": true, "sourceType": true}

// capturePages opens each source's page in a browser on its own identity,
// scrolls it a few times, and keeps the page's own API calls.
func (k *Kit) capturePages(ctx context.Context) {
	if k.Stack.Browser == nil {
		k.issue("pages: no browser, so TikTok's own calls on the discovery pages were not captured (run with --signer browser)")
		return
	}
	l := &lane{k: k, n: 2}
	defer l.release()
	if !l.bootstrap(ctx) {
		k.issue("pages: no identity could load a page")
		return
	}
	base := k.Stack.BaseURL
	type target struct{ name, url string }
	var targets []target
	if len(k.Cfg.Keywords) > 0 {
		kw := url.QueryEscape(k.Cfg.Keywords[0].Term)
		targets = append(targets, target{"search", base + "/search?q=" + kw}, target{"search_user", base + "/search/user?q=" + kw})
	}
	targets = append(targets, target{"foryou", base + "/foryou"})
	for _, kw := range k.Cfg.Keywords {
		if h := hashtagOf(kw.Term); h != "" {
			targets = append(targets, target{"tag", base + "/tag/" + url.PathEscape(h)})
			break
		}
	}
	posts := k.snapshot()
	for _, p := range posts {
		if p.MusicID != "" {
			targets = append(targets, target{"music", base + "/music/sound-" + p.MusicID})
			break
		}
	}
	for _, p := range posts {
		if p.ID != "" && p.Author.Username != "" {
			targets = append(targets, target{"post", base + "/@" + p.Author.Username + "/video/" + p.ID})
			break
		}
	}
	targets = append(targets, target{"profile", l.profile})

	nav, err := k.Stack.Browser(ctx, l.id, targets[0].url)
	if err != nil {
		k.issue("pages: the browser did not open: " + phase0.Clean(err.Error()))
		return
	}
	defer nav.Close()
	var urls []string
	for i, t := range targets {
		if ctx.Err() != nil {
			return
		}
		pc := PageCapture{Page: t.name}
		mark := 0
		if i > 0 {
			mark = nav.Mark()
			if err := nav.Navigate(ctx, t.url); err != nil {
				pc.Error = phase0.Clean(err.Error())
			}
		}
		for pc.Error == "" && pc.Scrolls < k.Cfg.Limits.PageScrolls {
			if mb := nav.MemoryMB(); k.Cfg.Limits.PageMB > 0 && mb > k.Cfg.Limits.PageMB {
				pc.Error = "memory limit: scrolling stopped"
				break
			}
			if err := nav.Scroll(ctx); err != nil {
				pc.Error = phase0.Clean(err.Error())
				break
			}
			pc.Scrolls++
		}
		pc.MemoryMB = nav.MemoryMB()
		for _, x := range nav.Captured(ctx, mark) {
			u, err := url.Parse(x.URL)
			if err != nil || !strings.HasPrefix(u.Path, "/api/") {
				continue
			}
			urls = append(urls, t.name+"\t"+x.URL)
			pc.Calls = append(pc.Calls, pageCall(u, x.Status, x.Body))
		}
		k.mu.Lock()
		k.pages = append(k.pages, pc)
		k.mu.Unlock()
		k.logf("pages: %s: %d API calls, %d scrolls, %.0f MB %s", t.name, len(pc.Calls), pc.Scrolls, pc.MemoryMB, pc.Error)
	}
	if len(urls) > 0 {
		// The calls' own URLs (keywords, secUids, the page's device id):
		// raw/ only.
		_ = os.WriteFile(filepath.Join(k.Cfg.Out, DirRaw, "pages-urls.txt"), []byte(strings.Join(urls, "\n")+"\n"), 0o600)
	}
}

func pageCall(u *url.URL, status int, body []byte) PageCall {
	c := PageCall{Path: u.Path, Status: status, Bytes: len(body), Values: map[string]string{}}
	for name, vs := range u.Query() {
		c.Params = append(c.Params, name)
		if safeParams[name] && len(vs) > 0 {
			c.Values[name] = vs[0]
		}
		if name == "X-Gnarly" || name == "X-Bogus" {
			c.Signed = true
		}
	}
	sort.Strings(c.Params)
	if parts, errs, _ := ttclient.Decode(body); len(parts) > 0 {
		f := Extract(parts[0])
		c.Keys, c.Posts, c.Users, c.HasMore = Keys(parts[0]), len(f.Posts), len(f.Users), f.HasMore
		if len(errs) > 0 {
			c.Code = errs[0].Code
		}
	}
	return c
}
