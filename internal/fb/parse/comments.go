package parse

import (
	"sort"

	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/types/known/timestamppb"

	fbv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/walk"
)

// CommentsVersion is recorded in Observation.parser_version.
const CommentsVersion = 1

// CommentContext tells the parser where a comment page belongs.
type CommentContext struct {
	PostID        int64
	PageID        int64
	PostCreatedAt *timestamppb.Timestamp
	PageAuthorID  string
	// BaseDepth is the depth of comments returned at the top of this
	// response: 0 for the comments query, d for a replies query.
	BaseDepth       int32
	ParentCommentID int64
	RootCommentID   int64
	Replies         bool
}

// CommentBundle is one comment and its attachment media.
type CommentBundle struct {
	Comment *fbv1.Comment
	Media   []*fbv1.Media
}

// ThreadRef is a reply thread that needs its own pagination.
type ThreadRef struct {
	CommentID      int64
	RootCommentID  int64
	Depth          int32 // depth of the parent comment
	FeedbackID     string
	ExpansionToken string
	ReplyCount     int64
	InlineReplies  int64
}

// CommentsResult is one parsed comments/replies page.
type CommentsResult struct {
	Comments   []CommentBundle
	Threads    []ThreadRef
	PageInfo   walk.PageInfo
	TotalCount int64
	NodeNull   bool
}

var (
	commentConnections = []string{"comments", "comment_rendering_instance_for_feed_location", "display_comments"}
	replyConnections   = []string{"replies_connection", "replies"}
)

// Comments parses a comments or replies pagination response.
func Comments(parts []gjson.Result, cc CommentContext) (CommentsResult, Report) {
	var rep Report
	keys := commentConnections
	if cc.Replies {
		keys = replyConnections
	}
	res := CommentsResult{PageInfo: walk.FindPageInfo(parts, keys, "Comment")}
	for _, p := range parts {
		if t := walk.First(p, "data.node.comment_rendering_instance_for_feed_location.comments.total_count",
			"data.node.display_comments.count", "data.node.comments.total_count"); t.Exists() {
			res.TotalCount = walk.Int64(t)
			break
		}
	}
	nodes := walk.Collect(parts, append([]string{"Comment", "Feedback"}, mediaTypenames...)...)
	byNode := map[*walk.Node]*fbv1.Comment{}
	inline := map[*walk.Node]int64{}
	// The same comment can be quoted twice in a response (highlighted, and in
	// the list). It is emitted from its first copy; replies nested under a
	// later copy belong to that first one.
	first := map[int64]*walk.Node{}
	canon := map[*walk.Node]*walk.Node{}
	for _, n := range nodes {
		if n.Kind != "Comment" {
			continue
		}
		parent := n.Ancestor("Comment")
		if f, ok := canon[parent]; ok {
			parent = f
		}
		c, r := comment(n, parent, byNode[parent], cc)
		if c != nil {
			if f, ok := first[c.CommentId]; ok {
				canon[n] = f
				continue
			}
		}
		rep.Merge(r)
		if parent != nil {
			inline[parent]++
		}
		if c == nil {
			continue
		}
		first[c.CommentId] = n
		byNode[n] = c
		b := CommentBundle{Comment: c}
		owner := Owner{Type: "comment", ID: c.CommentId, PostID: cc.PostID, PageID: cc.PageID, CommentID: c.CommentId, Comment: true}
		for _, x := range nodes {
			if isMedia(x.Kind) && x.Ancestor("Comment") == n {
				for _, m := range MediaFromNode(x.V, x.Kind, owner) {
					b.Media = append(b.Media, m)
					c.Attachments = append(c.Attachments, &fbv1.Attachment{
						Type: attachmentType(m.Kind), MediaId: m.MediaId, Url: m.SourceUrl,
						Width: m.Width, Height: m.Height,
					})
				}
			}
		}
		res.Comments = append(res.Comments, b)
	}
	// Reply threads that need their own pagination.
	for n, c := range byNode {
		if c.ReplyCount == nil || *c.ReplyCount == 0 {
			continue
		}
		fb := n.V.Get("feedback")
		token := walk.First(fb, "expansion_info.expansion_token", "replies_connection.expansion_token").String()
		if token == "" {
			token = walk.First(n.V, "expansion_info.expansion_token").String()
		}
		root := c.CommentId
		if c.RootCommentId != nil {
			root = *c.RootCommentId
		}
		if *c.ReplyCount > inline[n] {
			res.Threads = append(res.Threads, ThreadRef{
				CommentID: c.CommentId, RootCommentID: root, Depth: c.Depth, FeedbackID: c.FeedbackId,
				ExpansionToken: token, ReplyCount: *c.ReplyCount, InlineReplies: inline[n],
			})
		}
	}
	sort.Slice(res.Threads, func(i, j int) bool { return res.Threads[i].CommentID < res.Threads[j].CommentID })
	if len(res.Comments) == 0 && allNodesNull(parts) {
		res.NodeNull = true
	}
	return res, rep
}

func comment(n, parentNode *walk.Node, parent *fbv1.Comment, cc CommentContext) (*fbv1.Comment, Report) {
	var rep Report
	v := n.V
	id := walk.Int64(walk.First(v, "legacy_fbid", "comment_id"))
	if id == 0 {
		id = CommentIDFromGraphID(v.Get("id").String())
	}
	if id == 0 {
		rep.AddMissing("comment.id")
		return nil, rep
	}
	created := walk.Unix(walk.First(v, "created_time", "creation_time"))
	if created == nil {
		rep.AddMissing("comment.created_time")
	}
	c := &fbv1.Comment{
		CommentId:     id,
		PostId:        cc.PostID,
		PageId:        cc.PageID,
		PostCreatedAt: cc.PostCreatedAt,
		GraphId:       v.Get("id").String(),
		CreatedAt:     created,
		Url:           walk.First(v, "url", "comment_action_links.0.comment.url").String(),
	}

	switch {
	case parent != nil:
		c.Depth = parent.Depth + 1
		pid := parent.CommentId
		c.ParentCommentId = &pid
		root := parent.CommentId
		if parent.RootCommentId != nil {
			root = *parent.RootCommentId
		}
		c.RootCommentId = &root
	default:
		c.Depth = cc.BaseDepth
		if d := v.Get("depth"); d.Exists() && !cc.Replies {
			c.Depth = int32(d.Int())
		}
		pid := cc.ParentCommentID
		if pid == 0 {
			pid = walk.Int64(walk.First(v, "comment_direct_parent.legacy_fbid", "comment_parent.legacy_fbid"))
		}
		if pid != 0 && c.Depth > 0 {
			c.ParentCommentId = &pid
			root := cc.RootCommentID
			if root == 0 {
				root = pid
			}
			c.RootCommentId = &root
		}
	}

	if a := v.Get("author"); a.IsObject() {
		c.Author = &fbv1.Author{
			Id:        a.Get("id").String(),
			Name:      a.Get("name").String(),
			Url:       walk.First(a, "url", "profile_url", "uri").String(),
			AvatarUrl: walk.First(a, "profile_picture_depth_0.uri", "profile_picture_depth_1.uri", "profile_picture.uri").String(),
			Type:      a.Get("__typename").String(),
		}
		if ver := walk.First(a, "is_verified", "is_blue_verified"); ver.Exists() {
			b := ver.Bool()
			c.Author.Verified = &b
		}
	} else {
		rep.AddOptional("comment.author")
	}

	body := walk.First(v, "body", "preferred_body", "body_renderer")
	if body.IsObject() {
		c.Text = body.Get("text").String()
		c.TextRanges = Ranges(body.Get("ranges"))
	}
	if e := walk.First(v, "edit_history.count", "is_edited"); e.Exists() {
		b := e.Bool() || e.Int() > 0
		c.Edited = &b
	}
	if op := walk.First(v, "is_author_original_poster", "author_original_poster"); op.Exists() {
		b := op.Bool()
		c.IsOp = &b
	} else if cc.PageAuthorID != "" && c.Author != nil {
		b := c.Author.Id == cc.PageAuthorID
		c.IsOp = &b
	}

	fb := v.Get("feedback")
	if fb.IsObject() {
		c.FeedbackId = fb.Get("id").String()
		c.Reactions = Reactions(fb)
		if rc := walk.First(fb, "replies_fields.total_count", "replies_fields.count", "total_reply_count",
			"replies_connection.count", "display_replies_count"); rc.Exists() {
			x := walk.Int64(rc)
			c.ReplyCount = &x
		}
	} else {
		rep.AddOptional("comment.feedback")
		c.Reactions = &fbv1.Reactions{ByType: map[string]int64{}}
	}
	return c, rep
}

func attachmentType(k fbv1.MediaKind) string {
	switch k {
	case fbv1.MediaKind_MEDIA_KIND_COMMENT_GIF:
		return "gif"
	case fbv1.MediaKind_MEDIA_KIND_STICKER:
		return "sticker"
	case fbv1.MediaKind_MEDIA_KIND_COMMENT_VIDEO, fbv1.MediaKind_MEDIA_KIND_VIDEO, fbv1.MediaKind_MEDIA_KIND_THUMBNAIL:
		return "video"
	default:
		return "photo"
	}
}
