package parse

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"

	fbv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/walk"
)

// Version of the timeline parser, recorded in Observation.parser_version.
const TimelineVersion = 1

// PostBundle is one parsed post and its media.
type PostBundle struct {
	Post  *fbv1.Post
	Media []*fbv1.Media
}

// TimelineResult is one parsed timeline page.
type TimelineResult struct {
	Posts    []PostBundle
	PageInfo walk.PageInfo
	NodeNull bool
	// NoFeed: the node has no feed at all, which is not an empty timeline.
	// The id named something without a timeline (a new-style page's older
	// Page object), or Facebook shows no feed of it. It concerns this page
	// only: the query and the parser are fine.
	NoFeed bool
}

var timelineConnections = []string{"timeline_list_feed_units", "timeline_feed_units", "posts"}

// Timeline parses a timeline pagination response.
func Timeline(parts []gjson.Result, pageID int64, baseURL string) (TimelineResult, Report) {
	var rep Report
	res := TimelineResult{PageInfo: walk.FindPageInfo(parts, timelineConnections, "Story")}
	nodes := walk.Collect(parts, append([]string{"Story", "Comment", "Feedback"}, mediaTypenames...)...)
	for _, n := range nodes {
		if n.Kind != "Story" || n.Ancestor("Story") != nil {
			continue
		}
		b, r := story(n, nodes, pageID, baseURL)
		rep.Merge(r)
		if b.Post != nil {
			res.Posts = append(res.Posts, b)
		}
	}
	if len(res.Posts) == 0 && allNodesNull(parts) {
		res.NodeNull = true
	}
	if len(parts) > 0 && len(res.Posts) == 0 && !res.NodeNull && !hasFeed(parts) {
		res.NoFeed = true
	}
	if !res.PageInfo.Found && len(res.Posts) > 0 {
		rep.AddOptional("page_info")
	}
	return res, rep
}

func hasFeed(parts []gjson.Result) bool {
	for _, p := range parts {
		for _, c := range timelineConnections {
			if walk.Find(p, c, "Story").Exists() {
				return true
			}
		}
	}
	return false
}

func allNodesNull(parts []gjson.Result) bool {
	if len(parts) == 0 {
		return false
	}
	for _, p := range parts {
		if d := p.Get("data"); d.Exists() {
			for _, k := range []string{"node", "user", "page"} {
				if v := d.Get(k); v.Exists() && v.Type != gjson.Null {
					return false
				}
			}
		}
	}
	return true
}

func story(n *walk.Node, all []*walk.Node, pageID int64, baseURL string) (PostBundle, Report) {
	var rep Report
	v := n.V
	skip := []string{"Story", "Comment"}

	postID := walk.Int64(v.Get("post_id"))
	if postID == 0 {
		postID = PostIDFromStoryID(v.Get("id").String())
	}
	if postID == 0 {
		postID = walk.Int64(walk.Find(v, "post_id", skip...))
	}
	if postID == 0 {
		rep.AddMissing("story.post_id")
		return PostBundle{}, rep
	}
	created := walk.Unix(walk.First(v, "creation_time", "comet_sections.context_layout.story.comet_sections.metadata.0.story.creation_time"))
	if created == nil {
		created = walk.Unix(walk.Find(v, "creation_time", skip...))
	}
	if created == nil {
		rep.AddMissing("story.creation_time")
		return PostBundle{}, rep
	}

	p := &fbv1.Post{
		PostId:    postID,
		PageId:    pageID,
		CreatedAt: created,
		StoryId:   v.Get("id").String(),
		Status:    fbv1.PostStatus_POST_STATUS_VISIBLE,
	}
	p.Permalink = walk.First(v, "permalink_url", "url", "comet_sections.context_layout.story.comet_sections.metadata.0.story.url").String()
	if p.Permalink == "" {
		p.Permalink = walk.Find(v, "permalink_url", skip...).String()
	}
	if p.Permalink == "" {
		rep.AddOptional("story.permalink")
		p.Permalink = strings.TrimRight(baseURL, "/") + "/" + strconv.FormatInt(pageID, 10) + "/posts/" + strconv.FormatInt(postID, 10)
	}

	hasText := func(r gjson.Result) bool { return r.Get("text").Exists() }
	msg := walk.First(v, "message", "comet_sections.content.story.message")
	if !hasText(msg) {
		msg = walk.FindWhere(v, "message", hasText, skip...)
	}
	if msg.IsObject() {
		p.Text = msg.Get("text").String()
		p.TextRanges = Ranges(msg.Get("ranges"))
	} else if msg.Type == gjson.String {
		p.Text = msg.Str
	}
	p.Hashtags = Hashtags(p.Text, p.TextRanges)

	if actor := walk.First(v, "actors.0", "comet_sections.context_layout.story.comet_sections.actor_photo.story.actors.0"); actor.Exists() {
		p.AuthorId = actor.Get("id").String()
		p.AuthorName = actor.Get("name").String()
	}

	// Feedback: merge every Feedback node owned by this story.
	var fbs []gjson.Result
	for _, x := range all {
		if x.Kind == "Feedback" && x.Ancestor("Story", "Comment") == n {
			fbs = append(fbs, x.V)
		}
	}
	if len(fbs) == 0 {
		if f := walk.Find(v, "feedback", skip...); f.IsObject() {
			fbs = append(fbs, f)
		}
	}
	applyFeedback(p, fbs, &rep)
	if p.ViewCount == nil {
		if vc := walk.First(v, "video_view_count", "play_count"); vc.Exists() {
			c := walk.Int64(vc)
			p.ViewCount = &c
		} else if vc := walk.Find(v, "play_count", skip...); vc.Exists() {
			c := walk.Int64(vc)
			p.ViewCount = &c
		}
	}

	// Shared post.
	for _, x := range all {
		if x.Kind == "Story" && x.Parent == n {
			sid := walk.Int64(x.V.Get("post_id"))
			if sid == 0 {
				sid = PostIDFromStoryID(x.V.Get("id").String())
			}
			if sid != 0 {
				p.SharedPostId = &sid
			}
			p.SharedPostUrl = walk.First(x.V, "permalink_url", "url").String()
			break
		}
	}

	if pin := walk.Find(v, "is_pinned", skip...); pin.Exists() {
		b := pin.Bool()
		p.IsPinned = &b
	}
	if paid := walk.Find(v, "is_paid_partnership", skip...); paid.Exists() && paid.Bool() {
		t := true
		p.IsPaidPartnership = &t
	} else if bc := walk.Find(v, "branded_content_post_info", skip...); bc.IsObject() {
		t := true
		p.IsPaidPartnership = &t
	}

	// Media and links.
	reel := strings.Contains(p.Permalink, "/reel/")
	owner := Owner{Type: "post", ID: postID, PostID: postID, PageID: pageID, Reel: reel}
	var media []*fbv1.Media
	photos, videos := 0, 0
	for _, x := range all {
		if !isMedia(x.Kind) || x.Ancestor("Story", "Comment") != n {
			continue
		}
		switch x.Kind {
		case "Photo":
			photos++
		case "Video":
			videos++
		}
		for _, m := range MediaFromNode(x.V, x.Kind, owner) {
			media = append(media, m)
			if m.Variant == "original" || m.Variant == "sd" || m.Variant == "link_preview" {
				p.MediaIds = append(p.MediaIds, m.MediaId)
			}
		}
	}
	for _, att := range walk.FindAll(v, "attachments", skip...) {
		att.ForEach(func(_, a gjson.Result) bool {
			if l := linkAttachment(a); l != nil {
				p.Links = append(p.Links, l)
			}
			return true
		})
	}

	p.Type = postType(v, p, photos, videos, reel)
	return PostBundle{Post: p, Media: media}, rep
}

func applyFeedback(p *fbv1.Post, fbs []gjson.Result, rep *Report) {
	if len(fbs) == 0 {
		rep.AddOptional("story.feedback")
		return
	}
	for _, fb := range fbs {
		if p.FeedbackId == "" {
			p.FeedbackId = fb.Get("id").String()
		}
		if p.Reactions == nil || p.Reactions.Total == 0 {
			if r := Reactions(fb); r.Total > 0 || walk.First(fb, "reaction_count", "reactors").Exists() {
				p.Reactions = r
			}
		}
		if p.CommentCount == nil {
			if c := walk.First(fb, "comment_rendering_instance.comments.total_count",
				"comments_count_summary_renderer.feedback.comment_rendering_instance.comments.total_count",
				"total_comment_count", "comment_count.total_count", "comments.total_count"); c.Exists() {
				v := walk.Int64(c)
				p.CommentCount = &v
			}
		}
		if p.ShareCount == nil {
			if s := walk.First(fb, "share_count.count", "i18n_share_count", "share_count"); s.Exists() {
				v := walk.Int64(s)
				p.ShareCount = &v
			}
		}
		if p.ViewCount == nil {
			if s := walk.First(fb, "video_view_count", "video_view_count_renderer.feedback.video_view_count", "play_count"); s.Exists() {
				v := walk.Int64(s)
				p.ViewCount = &v
			}
		}
	}
	if p.Reactions == nil {
		rep.AddOptional("story.reactions")
		p.Reactions = &fbv1.Reactions{ByType: map[string]int64{}}
	}
	if p.CommentCount == nil {
		rep.AddOptional("story.comment_count")
	}
}

func linkAttachment(a gjson.Result) *fbv1.LinkAttachment {
	att := walk.First(a, "styles.attachment", "attachment", "style_type_renderer.attachment")
	if !att.Exists() {
		att = a
	}
	raw := walk.First(att, "story_attachment_link_renderer.attachment.web_link.url", "web_link.url", "url", "target.url").String()
	if raw == "" {
		return nil
	}
	raw = unwrapLinkShim(raw)
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || isFacebookHost(u.Hostname()) {
		return nil
	}
	return &fbv1.LinkAttachment{
		Url:         raw,
		Title:       walk.First(att, "title_with_entities.text", "title", "name").String(),
		Description: walk.First(att, "description.text", "description").String(),
		Source:      walk.First(att, "source.text", "source").String(),
		ImageUrl:    walk.First(att, "media.image.uri", "media.large_share_image.uri").String(),
	}
}

func unwrapLinkShim(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	if (u.Hostname() == "l.facebook.com" || u.Hostname() == "lm.facebook.com") && u.Path == "/l.php" {
		if t := u.Query().Get("u"); t != "" {
			return t
		}
	}
	return raw
}

func isFacebookHost(h string) bool {
	h = strings.ToLower(h)
	return h == "facebook.com" || strings.HasSuffix(h, ".facebook.com") || h == "fb.com" || strings.HasSuffix(h, ".fbcdn.net") || h == "fb.watch"
}

func postType(v gjson.Result, p *fbv1.Post, photos, videos int, reel bool) fbv1.PostType {
	if live := walk.Find(v, "is_live_streaming", "Story", "Comment"); live.Bool() {
		return fbv1.PostType_POST_TYPE_LIVE
	}
	if ev := walk.Find(v, "event", "Story", "Comment"); ev.IsObject() && ev.Get("__typename").String() == "Event" {
		return fbv1.PostType_POST_TYPE_EVENT
	}
	switch {
	case reel:
		return fbv1.PostType_POST_TYPE_REEL
	case videos > 0:
		return fbv1.PostType_POST_TYPE_VIDEO
	case photos > 1:
		return fbv1.PostType_POST_TYPE_ALBUM
	case photos == 1:
		return fbv1.PostType_POST_TYPE_PHOTO
	case p.SharedPostId != nil:
		return fbv1.PostType_POST_TYPE_SHARE
	case len(p.Links) > 0:
		return fbv1.PostType_POST_TYPE_LINK
	default:
		return fbv1.PostType_POST_TYPE_STATUS
	}
}
