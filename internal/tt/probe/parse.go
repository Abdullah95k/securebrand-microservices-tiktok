package probe

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/tidwall/gjson"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
)

func renderVars(tmpl json.RawMessage, vars map[string]any) ([]byte, error) {
	return registry.Render(tmpl, vars)
}

// count prefers statsV2's exact string over stats' rounded number (the
// recordings show stats rounded to 3 significant digits).
func count(v2, v1 gjson.Result) int64 {
	if v2.Exists() {
		if n, err := strconv.ParseInt(v2.String(), 10, 64); err == nil {
			return n
		}
	}
	return v1.Int()
}

// accountData reads a userInfo object (the page's or the account call's).
func accountData(ui gjson.Result, source string) *AccountData {
	u, s, s2 := ui.Get("user"), ui.Get("stats"), ui.Get("statsV2")
	return &AccountData{Source: source, ID: u.Get("id").String(), SecUID: u.Get("secUid").String(), Username: u.Get("uniqueId").String(),
		Nickname: u.Get("nickname").String(), Bio: u.Get("signature").String(), BioLink: u.Get("bioLink.link").String(),
		Verified: u.Get("verified").Bool(), Private: u.Get("privateAccount").Bool(),
		Followers: count(s2.Get("followerCount"), s.Get("followerCount")), Following: count(s2.Get("followingCount"), s.Get("followingCount")),
		Likes: count(s2.Get("heartCount"), s.Get("heartCount")), Videos: count(s2.Get("videoCount"), s.Get("videoCount")),
		LikesGiven: count(s2.Get("diggCount"), s.Get("diggCount")), CreatedAt: u.Get("createTime").Int(),
		RenamedAt: u.Get("uniqueIdModifyTime").Int(), AvatarURL: u.Get("avatarMedium").String()}
}

// posts reads list items.
func (r *runner) posts(username string, items []gjson.Result) []Post {
	out := make([]Post, 0, len(items))
	for _, it := range items {
		id := it.Get("id").String()
		p := Post{ID: id, Kind: "video", Caption: it.Get("desc").String(), PostedAt: it.Get("createTime").Int(), IDTime: IDTime(id).Unix(),
			Duration: it.Get("video.duration").Int(), CoverURL: it.Get("video.cover").String(), VideoURL: smallestVideo(it.Get("video")),
			MusicID: it.Get("music.id").String(), MusicTitle: it.Get("music.title").String(), MusicAuthor: it.Get("music.authorName").String(),
			MusicOrig: it.Get("music.original").Bool(), Pinned: it.Get("isPinnedItem").Bool(), Ad: it.Get("isAd").Bool(),
			AILabel: it.Get("AIGCDescription").String(), HasStats: it.Get("stats").Exists() || it.Get("statsV2").Exists()}
		st, st2 := it.Get("stats"), it.Get("statsV2")
		p.Plays, p.Likes = count(st2.Get("playCount"), st.Get("playCount")), count(st2.Get("diggCount"), st.Get("diggCount"))
		p.Comments, p.Shares = count(st2.Get("commentCount"), st.Get("commentCount")), count(st2.Get("shareCount"), st.Get("shareCount"))
		p.Saves, p.Reposts = count(st2.Get("collectCount"), st.Get("collectCount")), st2.Get("repostCount").Int()
		if ip := it.Get("imagePost"); ip.Exists() {
			p.Kind = "photo"
			if c := ip.Get("cover.imageURL.urlList.0"); c.Exists() {
				p.CoverURL = c.String()
			}
		}
		for _, te := range it.Get("textExtra").Array() {
			if h := te.Get("hashtagName").String(); h != "" {
				p.Hashtags = append(p.Hashtags, h)
			}
			if m := te.Get("userUniqueId").String(); m != "" {
				p.Mentions = append(p.Mentions, m)
			}
		}
		kind := "video"
		if p.Kind == "photo" {
			kind = "photo"
		}
		owner := it.Get("author.uniqueId").String()
		if owner == "" {
			owner = username
		}
		p.Link = r.o.BaseURL + "/@" + owner + "/" + kind + "/" + id
		out = append(out, p)
	}
	return out
}

// smallestVideo picks the lowest-bitrate rendition, else playAddr.
func smallestVideo(v gjson.Result) string {
	best, rate := v.Get("playAddr").String(), int64(-1)
	for _, b := range v.Get("bitrateInfo").Array() {
		u := b.Get("PlayAddr.UrlList.0").String()
		if br := b.Get("Bitrate").Int(); u != "" && (rate < 0 || br < rate) {
			best, rate = u, br
		}
	}
	return best
}

func comment(postID, parent string, c gjson.Result, page int) Comment {
	return Comment{PostID: postID, ID: c.Get("cid").String(), ParentID: parent, AuthorID: c.Get("user.uid").String(),
		AuthorUsername: c.Get("user.unique_id").String(), AuthorNickname: c.Get("user.nickname").String(), Text: c.Get("text").String(),
		CreatedAt: c.Get("create_time").Int(), Likes: c.Get("digg_count").Int(), Replies: c.Get("reply_comment_total").Int(), Page: page}
}

func (r *runner) summarise() {
	s := &r.rep.Summary
	s.Accounts = len(r.rep.Accounts)
	s.PageForms, s.Cookies, s.Tokens, s.BotTypes = map[string]int{}, map[string]int{}, map[string]int{}, map[string]int{}
	s.WalkStops, s.OrderVerdicts = map[string]int{}, map[string]int{}
	s.Captchas = r.captchas
	for _, a := range r.rep.Accounts {
		if a.Stopped {
			continue // a stop counts for no question
		}
		if a.Page.Class == "ok" && len(a.Tokens) > 0 {
			s.Bootstrapped++
		}
		s.PageForms[a.PageForm]++
		for _, c := range a.Cookies {
			s.Cookies[c]++
		}
		for _, t := range a.Tokens {
			s.Tokens[t]++
		}
		if a.BotType != "" {
			s.BotTypes[a.BotType]++
		}
		for _, p := range a.Posts {
			s.Posts++
			if p.HasStats {
				s.PostsWithStats++
			}
			if p.Pinned {
				s.PinnedSeen++
			}
			if p.Ad {
				s.AdSeen++
			}
			if p.Kind == "photo" {
				s.PhotoSeen++
			}
			gap := p.PostedAt - p.IDTime
			if gap > 60 {
				s.IDTimeScheduled++
			}
			if gap < 0 {
				gap = -gap
			}
			if gap <= 60 {
				s.IDTimeMatches++
			} else {
				s.IDTimeMismatches++
			}
			s.IDTimeMaxGap = max(s.IDTimeMaxGap, gap)
		}
		if a.Walk == nil || a.Walk.Variant != "post_list" { // a post_list walk counted them already
			s.PinnedSeen += a.AppListPinned
		}
		if w := a.Walk; w != nil {
			s.WalkStops[w.Stop]++
			s.FilteredPages += w.Filtered
			s.WalkRepeats += w.Repeats
			if !w.Oldest.IsZero() && (s.OldestReached.IsZero() || w.Oldest.Before(s.OldestReached)) {
				s.OldestReached = w.Oldest
			}
		}
		for _, d := range a.Depth {
			if s.DepthServed == nil {
				s.DepthServed = map[string]int{}
			}
			if d.Step.Items > 0 {
				s.DepthServed[strconv.Itoa(d.Days)]++
			} else {
				s.DepthServed[strconv.Itoa(d.Days)] += 0
			}
		}
		for _, c := range a.CommentReads {
			s.CommentReads++
			s.CommentsServed += c.Read
			if s.CommentsByVia == nil {
				s.CommentsByVia = map[string]int{}
			}
			s.CommentsByVia[c.Via] += c.Read
			if c.Order != "" {
				s.OrderVerdicts[c.Order]++
			}
			if c.Ended && c.Total > 0 {
				s.CompleteRatios = append(s.CompleteRatios, float64(c.ReplySum)/float64(c.Total))
			}
		}
		for _, rr := range a.ReplyReads {
			s.RepliesServed += rr.Read
		}
		if a.CursorPortable != nil {
			if *a.CursorPortable {
				s.CursorPortable++
			} else {
				s.CursorNotPortable++
			}
		}
	}
	s.Requests = map[string]Cost{}
	for k, c := range r.cost {
		cc := *c
		if cc.N > 0 {
			cc.MeanMs, cc.MeanBytes = cc.sumMs/int64(cc.N), cc.sumBytes/int64(cc.N)
		}
		s.Requests[k] = cc
	}
}

func unix(t int64) string {
	if t <= 0 {
		return ""
	}
	return time.Unix(t, 0).UTC().Format(time.RFC3339)
}
