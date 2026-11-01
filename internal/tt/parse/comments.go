package parse

import (
	"strings"

	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
)

// CommentContext tells the parser which post a page belongs to. Zero
// fields are taken from the comments themselves where they carry them
// (aweme_id).
type CommentContext struct {
	PostID        int64
	UserID        int64 // the post's account
	PostCreatedAt *timestamppb.Timestamp
	// Replies is set for /api/comment/list/reply/; ParentCommentID is the
	// comment whose replies these are.
	Replies         bool
	ParentCommentID int64
}

// CommentBundle is one comment and its attached images.
type CommentBundle struct {
	Comment *ttv1.Comment
	Media   []*ttv1.Media
}

// ThreadRef is a top-level comment with replies to read.
type ThreadRef struct {
	CommentID  int64
	ReplyCount int64
	// Inline is how many replies the page quoted under it (reply_comment).
	Inline int
}

// CommentsResult is one parsed page of comments or replies.
type CommentsResult struct {
	Comments []CommentBundle
	Threads  []ThreadRef
	// Total is the post's comment total TikTok reports (top-level only on
	// the comments call).
	Total   int64
	HasMore bool
	// Cursor is the next page's offset.
	Cursor     int64
	StatusCode int64
	Dropped    Dropped
}

// Comments parses a page of /api/comment/list/ or /api/comment/list/reply/.
// Top-level comments come in TikTok's ranking order (T13), with a few of
// their replies quoted inline; those are returned too, at depth 1. An
// empty body (a refusal, T4) is reported missing.
func Comments(body []byte, cc CommentContext, o Options) (CommentsResult, Report) {
	var rep Report
	res := CommentsResult{Dropped: Dropped{}}
	if len(strings.TrimSpace(string(body))) == 0 || !gjson.ValidBytes(body) {
		rep.AddMissing("body")
		return res, rep
	}
	doc := gjson.ParseBytes(body)
	code, ok := statusCode(doc)
	res.StatusCode = code
	if !ok {
		rep.AddMissing("status_code")
	}
	if code != 0 {
		return res, rep
	}
	res.Total, res.HasMore, res.Cursor = doc.Get("total").Int(), doc.Get("has_more").Bool(), doc.Get("cursor").Int()
	if !doc.Get("has_more").Exists() {
		rep.AddMissing("has_more")
	}
	seen := map[int64]bool{}
	add := func(c gjson.Result, parent int64) (int64, bool) {
		if id, ok, _ := ID(c.Get("cid")); ok && seen[id] {
			return 0, false // quoted twice (inline and in the list): read once
		}
		b, ok := comment(c, cc, parent, o, &rep, res.Dropped)
		if !ok {
			return 0, false
		}
		seen[b.Comment.CommentId] = true
		res.Comments = append(res.Comments, b)
		return b.Comment.CommentId, true
	}
	for _, c := range doc.Get("comments").Array() {
		id, ok := add(c, cc.ParentCommentID)
		if !ok || cc.Replies {
			continue
		}
		inline := 0
		for _, r := range c.Get("reply_comment").Array() {
			if _, ok := add(r, id); ok {
				inline++
			}
		}
		if n := c.Get("reply_comment_total").Int(); n > 0 {
			res.Threads = append(res.Threads, ThreadRef{CommentID: id, ReplyCount: n, Inline: inline})
		}
	}
	return res, rep
}

// comment builds one comment; parent is the top-level comment of a reply
// (the context's, or the quoting comment's for an inline reply).
func comment(c gjson.Result, cc CommentContext, parent int64, o Options, rep *Report, dropped Dropped) (CommentBundle, bool) {
	id, ok, inRange := ID(c.Get("cid"))
	if !ok {
		if inRange {
			dropped.add(DropNoID)
		} else {
			dropped.add(DropIDRange)
		}
		return CommentBundle{}, false
	}
	uid, ok, _ := ID(c.Get("user.uid"))
	if !ok {
		// No numeric id: no author key, so no erasure by author. Dropped and
		// counted, never keyed on sec_uid or the username (ADR 0007 §1).
		dropped.add(DropNoUserID)
		return CommentBundle{}, false
	}
	postID := cc.PostID
	if postID == 0 {
		postID, _, _ = ID(c.Get("aweme_id"))
	}
	if postID == 0 {
		rep.AddMissing("comment.aweme_id")
	}
	if p, ok, _ := ID(c.Get("reply_id")); ok {
		parent = p // a reply names its top-level comment itself
	}
	u := c.Get("user")
	author := &ttv1.Person{AuthorKey: o.key(uid), UserId: proto.Int64(uid), SecUid: u.Get("sec_uid").String(),
		Username: u.Get("unique_id").String(), Nickname: u.Get("nickname").String(),
		AvatarUrl: firstOf(u, "avatar_thumb.url_list.0", "avatar_medium.url_list.0").String()}
	cm := &ttv1.Comment{Obs: o.obs(), CommentId: id, PostId: postID, UserId: cc.UserID, PostCreatedAt: cc.PostCreatedAt,
		Author: author, Text: c.Get("text").String(), CreatedAt: unix(c.Get("create_time")),
		LikeCount: count(gjson.Result{}, c.Get("digg_count")), ReplyCount: count(gjson.Result{}, c.Get("reply_comment_total")),
		LikedByCreator: optBool(c.Get("is_author_digged")), PinnedByCreator: optBool(c.Get("author_pin")),
		StickPosition: int32(c.Get("stick_position").Int()), Language: c.Get("comment_language").String(),
		Status: ttv1.CommentStatus_COMMENT_STATUS_VISIBLE}
	if cm.CreatedAt == nil {
		rep.AddMissing("comment.create_time")
	}
	if parent != 0 {
		cm.ParentCommentId, cm.Depth = proto.Int64(parent), 1
		if r, ok, _ := ID(c.Get("reply_to_reply_id")); ok && r != parent {
			cm.ReplyToCommentId = proto.Int64(r)
		}
	}
	for _, l := range c.Get("label_list").Array() {
		if t := firstOf(l, "text", "type").String(); t != "" {
			cm.Labels = append(cm.Labels, t)
		}
	}
	cm.TextRanges = commentRanges(c.Get("text_extra"), o, dropped)
	b := CommentBundle{Comment: cm}
	for i, img := range c.Get("image_list").Array() {
		src := firstOf(img, "origin_url.url_list.0", "crop_url.url_list.0").String()
		if src == "" {
			continue
		}
		m := &ttv1.Media{Obs: o.obs(), MediaId: MediaID(id, ttv1.MediaKind_MEDIA_KIND_COMMENT_IMAGE, "original-"+itoa(int64(i))),
			Kind: ttv1.MediaKind_MEDIA_KIND_COMMENT_IMAGE, Variant: "original", UserId: cc.UserID, CommentId: proto.Int64(id),
			Position: int32(i), SourceUrl: src, SourceExpiresAt: URLExpiry(src), Width: int32(img.Get("origin_url.width").Int()),
			Height: int32(img.Get("origin_url.height").Int()), PostCreatedAt: cc.PostCreatedAt, AuthorKey: author.AuthorKey}
		if postID != 0 {
			m.PostId = proto.Int64(postID)
		}
		cm.ImageMediaIds = append(cm.ImageMediaIds, m.MediaId)
		b.Media = append(b.Media, m)
	}
	return b, true
}

// commentRanges reads a comment's text_extra: mentions (user_id) and
// hashtags (hashtag_name), start to end in UTF-16 units.
func commentRanges(te gjson.Result, o Options, dropped Dropped) []*ttv1.TextRange {
	var out []*ttv1.TextRange
	for _, x := range te.Array() {
		start, end := x.Get("start").Int(), x.Get("end").Int()
		r := &ttv1.TextRange{Offset: int32(start), Length: int32(end - start)}
		switch {
		case x.Get("hashtag_name").String() != "":
			r.Type, r.Name = "hashtag", x.Get("hashtag_name").String()
		case x.Get("user_id").String() != "" || x.Get("sec_uid").String() != "":
			uid, ok, _ := ID(x.Get("user_id"))
			if !ok {
				dropped.add(DropNoUserID)
				continue
			}
			r.Type = "mention"
			r.Person = &ttv1.Person{AuthorKey: o.key(uid), UserId: proto.Int64(uid), SecUid: x.Get("sec_uid").String()}
		default:
			continue
		}
		out = append(out, r)
	}
	return out
}
