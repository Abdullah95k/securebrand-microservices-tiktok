package parse

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"

	fbv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/htmlx"
	scrapehtml "github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/htmlx"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/walk"
)

// PageVersion is recorded in Observation.parser_version.
const PageVersion = 1

var (
	titleRe   = regexp.MustCompile(`(?s)<title[^>]*>(.*?)</title>`)
	ogTitleRe = regexp.MustCompile(`<meta property="og:title" content="([^"]*)"`)
	ogURLRe   = regexp.MustCompile(`<meta property="og:url" content="([^"]*)"`)
)

// Page parses the page profile from its HTML. pageID may be 0, in which case
// it is extracted from the HTML, and failing that is requested: the numeric
// id the page was loaded by (0 when it was loaded by its vanity). The HTML
// comes first because an old-style id redirects to the page's profile, whose
// id is the one with the timeline.
func Page(html []byte, pageID, requested int64) (*fbv1.Page, Report) {
	var rep Report
	if pageID == 0 {
		pageID = htmlx.PageID(html)
	}
	if pageID == 0 {
		pageID = requested
	}
	if pageID == 0 {
		rep.AddMissing("page.id")
		return nil, rep
	}
	idStr := strconv.FormatInt(pageID, 10)
	p := &fbv1.Page{PageId: pageID, Status: fbv1.PageStatus_PAGE_STATUS_ACTIVE, Contact: map[string]string{}}

	blobs := scrapehtml.JSONBlobs(html)
	nodes := walk.Collect(blobs, "User", "Page")
	// The profile's nodes, then those of its older Page object (a
	// new-style page's delegate_page), which may hold more of its fields.
	ids := map[string]bool{idStr: true}
	for _, n := range nodes {
		if n.V.Get("id").String() == idStr {
			if d := n.V.Get("delegate_page.id").String(); d != "" {
				ids[d] = true
			}
		}
	}
	var mine []gjson.Result
	for _, n := range nodes {
		if n.V.Get("id").String() == idStr {
			mine = append(mine, n.V)
		}
	}
	for _, n := range nodes {
		if id := n.V.Get("id").String(); id != idStr && ids[id] {
			mine = append(mine, n.V)
		}
	}
	str := func(paths ...string) string {
		for _, m := range mine {
			if s := walk.First(m, paths...).String(); s != "" {
				return s
			}
		}
		return ""
	}
	p.Name = str("name")
	p.Vanity = str("vanity", "username")
	p.Url = str("url", "profile_url")
	p.Intro = str("profile_intro_card.bio.text", "intro_card.bio.text", "bio.text", "about.text")
	if pic := str("profilePicLarge.uri", "profile_picture.uri", "profilePic160.uri", "profilePicNormal.uri"); pic != "" {
		p.ProfilePicture = &fbv1.ImageRef{Url: pic}
	}
	if cover := str("cover_photo.photo.image.uri", "cover_photo.photo.viewer_image.uri", "cover_photo.image.uri"); cover != "" {
		p.CoverPhoto = &fbv1.ImageRef{Url: cover}
	}
	for _, m := range mine {
		if v := walk.First(m, "is_verified", "is_blue_verified"); v.Exists() && p.Verified == nil {
			b := v.Bool()
			p.Verified = &b
		}
		if c := walk.First(m, "delegate_page.category_name", "category_name", "page_category"); c.Exists() && len(p.Categories) == 0 {
			p.Categories = []string{c.String()}
		}
		walk.First(m, "category_type_list", "categories").ForEach(func(_, x gjson.Result) bool {
			if s := walk.First(x, "text", "name").String(); s != "" {
				p.Categories = appendUnique(p.Categories, s)
			} else if x.Type == gjson.String {
				p.Categories = appendUnique(p.Categories, x.Str)
			}
			return true
		})
		if f := walk.First(m, "follower_count", "followers_count", "subscribers.count"); f.Exists() && p.FollowersCount == nil {
			v := walk.Int64(f)
			p.FollowersCount = &v
		}
		if l := walk.First(m, "page_likers.global_likers_count", "likes_count", "fan_count"); l.Exists() && p.LikesCount == nil {
			v := walk.Int64(l)
			p.LikesCount = &v
		}
		// Social context lines such as "1.2M followers" / "980K likes".
		for _, t := range walk.FindAll(m, "text") {
			s := strings.ToLower(t.String())
			switch {
			case strings.Contains(s, "follower") && p.FollowersCount == nil:
				if v, ok := walk.ParseCount(s); ok {
					p.FollowersCount = &v
				}
			case strings.Contains(s, "like") && !strings.Contains(s, "talking") && p.LikesCount == nil:
				if v, ok := walk.ParseCount(s); ok {
					p.LikesCount = &v
				}
			}
		}
		for key, paths := range map[string][]string{
			"email":   {"email", "page_about_fields.email"},
			"phone":   {"phone_number", "formatted_phone_number", "page_about_fields.phone"},
			"website": {"website", "websites.0", "page_about_fields.website"},
			"address": {"address.full_address", "single_line_address"},
		} {
			if _, ok := p.Contact[key]; ok {
				continue
			}
			if v := walk.First(m, paths...).String(); v != "" {
				p.Contact[key] = v
			}
		}
	}
	if p.Name == "" {
		if m := ogTitleRe.FindSubmatch(html); m != nil {
			p.Name = string(m[1])
		} else if m := titleRe.FindSubmatch(html); m != nil {
			p.Name = strings.TrimSuffix(strings.TrimSpace(string(m[1])), " | Facebook")
		}
	}
	if p.Url == "" {
		if m := ogURLRe.FindSubmatch(html); m != nil {
			p.Url = string(m[1])
		}
	}
	if p.Name == "" {
		rep.AddMissing("page.name")
	}
	if p.FollowersCount == nil {
		rep.AddOptional("page.followers")
	}
	return p, rep
}

func appendUnique(xs []string, s string) []string {
	for _, x := range xs {
		if x == s {
			return xs
		}
	}
	return append(xs, s)
}
