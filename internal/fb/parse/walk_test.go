package parse

import (
	"testing"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/walk"
	"github.com/tidwall/gjson"
)

// A page that shares one of its own posts: the share's attached story has
// the same id as the shared post's own story. Whichever comes first, both
// posts are parsed, the share points at the shared post, and the shared
// post keeps its media.
func TestTimelineSharedPostInSamePage(t *testing.T) {
	share := `{"__typename":"Story","id":"S:share","post_id":"11","creation_time":1700000200,
		"attached_story":{"__typename":"Story","id":"S:shared","post_id":"22","url":"https://www.facebook.com/p/22",
			"attachments":[{"media":{"__typename":"Photo","id":"900","image":{"uri":"https://scontent.example/900.jpg","width":10,"height":10}}}]}}`
	shared := `{"__typename":"Story","id":"S:shared","post_id":"22","creation_time":1700000100,
		"attachments":[{"media":{"__typename":"Photo","id":"900","image":{"uri":"https://scontent.example/900.jpg","width":10,"height":10}}}]}`
	for name, edges := range map[string]string{"share first": share + "," + shared, "shared first": shared + "," + share} {
		t.Run(name, func(t *testing.T) {
			body := `{"data":{"node":{"__typename":"Page","timeline_list_feed_units":{"edges":[` + wrapEdges(edges) +
				`],"page_info":{"has_next_page":false,"end_cursor":""}}}}}`
			res, rep := Timeline([]gjson.Result{gjson.Parse(body)}, 5, "https://www.facebook.com")
			if !rep.OK() {
				t.Fatalf("missing %v", rep.Missing)
			}
			byID := map[int64]PostBundle{}
			for _, b := range res.Posts {
				byID[b.Post.PostId] = b
			}
			if len(res.Posts) != 2 || byID[11].Post == nil || byID[22].Post == nil {
				t.Fatalf("posts %v, want 11 and 22", res.Posts)
			}
			if sp := byID[11].Post.SharedPostId; sp == nil || *sp != 22 {
				t.Fatalf("share points at %v, want 22", sp)
			}
			if byID[22].Post.SharedPostId != nil {
				t.Fatalf("the shared post is not a share: %v", byID[22].Post)
			}
			if len(byID[22].Media) != 1 {
				t.Fatalf("shared post media %v, want its photo", byID[22].Media)
			}
		})
	}
}

// wrapEdges turns comma-separated story objects into timeline edges.
func wrapEdges(stories string) string {
	var out string
	for i, s := range gjson.Parse("[" + stories + "]").Array() {
		if i > 0 {
			out += ","
		}
		out += `{"node":` + s.Raw + `}`
	}
	return out
}

// The same comment quoted twice in one response, outside and inside the
// post's Feedback (a highlighted copy, and the copy in the list), is
// returned once, whichever comes first. A reply nested only under the
// second copy keeps its parent and counts as inline.
func TestCommentsQuotedTwice(t *testing.T) {
	reply := `{"__typename":"Comment","id":"Y29tbWVudDoxXzM=","legacy_fbid":"3","body":{"text":"reply"},"created_time":1700000300,
		"author":{"__typename":"User","id":"8","name":"B"}}`
	bare := `{"__typename":"Comment","id":"Y29tbWVudDoxXzI=","legacy_fbid":"2","body":{"text":"top"},"created_time":1700000200,
		"author":{"__typename":"User","id":"7","name":"A"}}`
	full := `{"__typename":"Comment","id":"Y29tbWVudDoxXzI=","legacy_fbid":"2","body":{"text":"top"},"created_time":1700000200,
		"author":{"__typename":"User","id":"7","name":"A"},
		"feedback":{"__typename":"Feedback","id":"ZmVlZGJhY2s6MV8y","replies_fields":{"total_count":1},
			"replies_connection":{"edges":[{"node":` + reply + `}]}}}`
	list := `"node":{"__typename":"Feedback","id":"ZmVlZGJhY2s6MQ==",
		"comment_rendering_instance_for_feed_location":{"comments":{"edges":[{"node":` + full + `}],
			"page_info":{"has_next_page":false},"total_count":2}}}`
	highlighted := `"highlighted_comment":{"node":` + bare + `}`
	for name, body := range map[string]string{
		"highlighted first": `{"data":{` + highlighted + `,` + list + `}}`,
		"list first":        `{"data":{` + list + `,` + highlighted + `}}`,
	} {
		t.Run(name, func(t *testing.T) {
			n := 0
			for _, x := range walk.Collect([]gjson.Result{gjson.Parse(body)}, "Comment", "Feedback") {
				if x.Kind == "Comment" {
					n++
				}
			}
			if n != 3 {
				t.Fatalf("Collect returned %d comments, want both copies and the reply", n)
			}
			res, _ := Comments([]gjson.Result{gjson.Parse(body)}, CommentContext{PostID: 1, PageID: 5})
			byID := map[int64]int{}
			for _, b := range res.Comments {
				byID[b.Comment.CommentId]++
				if b.Comment.CommentId == 3 {
					if p := b.Comment.ParentCommentId; p == nil || *p != 2 || b.Comment.Depth != 1 {
						t.Fatalf("reply parent %v depth %d, want 2 and 1", p, b.Comment.Depth)
					}
				}
			}
			if len(res.Comments) != 2 || byID[2] != 1 || byID[3] != 1 {
				t.Fatalf("comments %v, want 2 and 3 once each", byID)
			}
			if len(res.Threads) != 0 {
				t.Fatalf("threads %v: the one reply is inline", res.Threads)
			}
		})
	}
}

// Asked with a new-style page's older Page id, Facebook returns a bare Page
// node: no feed at all, which is not an empty timeline. A real timeline
// that has nothing (more) to show still parses clean.
func TestTimelineWithoutFeed(t *testing.T) {
	bare := `{"data":{"node":{"__typename":"Page","id":"104000000000123"}},"extensions":{"is_final":true}}`
	// Not a parser failure (that would count against the query's breaker):
	// the page's own problem, flagged.
	res, rep := Timeline([]gjson.Result{gjson.Parse(bare)}, 104000000000123, "https://www.facebook.com")
	if len(res.Posts) != 0 || res.NodeNull || !res.NoFeed || !rep.OK() {
		t.Fatalf("bare page node: %+v %+v", res, rep)
	}
	empty := `{"data":{"node":{"__typename":"User","id":"1","timeline_list_feed_units":{"edges":[],"page_info":{"has_next_page":false}}}}}`
	if res, rep := Timeline([]gjson.Result{gjson.Parse(empty)}, 1, "https://www.facebook.com"); res.NoFeed || !rep.OK() {
		t.Fatalf("empty feed: %+v %+v", res, rep)
	}
	gone := `{"data":{"node":null},"extensions":{"is_final":true}}`
	if res, rep := Timeline([]gjson.Result{gjson.Parse(gone)}, 1, "https://www.facebook.com"); !res.NodeNull || res.NoFeed || !rep.OK() {
		t.Fatalf("deleted page: %+v %+v", res, rep)
	}
	if res, _ := Timeline(nil, 1, "https://www.facebook.com"); res.NoFeed {
		t.Fatal("no parts is not a page without a feed")
	}
}

// A new-style page's profile names its older Page object, whose node may
// hold fields the profile's does not.
func TestPageReadsItsPageObject(t *testing.T) {
	html := `<script type="application/json">{"user":{"__typename":"User","id":"100064000000777","name":"News Iraq",` +
		`"delegate_page":{"id":"104000000000123","category_name":"Media/news company"}}}</script>` +
		`<script type="application/json">{"page":{"__typename":"Page","id":"104000000000123","page_likers":{"global_likers_count":5000},` +
		`"website":"https://example.org"}}</script>` +
		`<script type="application/json">{"other":{"__typename":"Page","id":"999","website":"https://wrong.example"}}</script>` +
		`<script>{"props":{"userID":"100064000000777"}}</script>`
	p, rep := Page([]byte(html), 0, 0)
	if p == nil || p.PageId != 100064000000777 || p.Name != "News Iraq" || p.GetLikesCount() != 5000 ||
		p.Contact["website"] != "https://example.org" || len(p.Categories) != 1 {
		t.Fatalf("page %+v, report %+v", p, rep)
	}
}

// A page loaded by its numeric id whose HTML names none (no userID, or
// several) takes the id it was loaded by; the HTML's own id wins otherwise.
func TestPageFallsBackToTheRequestedID(t *testing.T) {
	bare := `<html><head><title>News Iraq | Facebook</title></head></html>`
	if p, rep := Page([]byte(bare), 0, 0); p != nil || rep.OK() {
		t.Fatalf("no id anywhere: %+v %+v", p, rep)
	}
	p, rep := Page([]byte(bare), 0, 100067000000001)
	if p == nil || p.PageId != 100067000000001 || p.Name != "News Iraq" || !rep.OK() {
		t.Fatalf("requested id: %+v %+v", p, rep)
	}
	named := bare + `<script>{"props":{"userID":"100064000000777"}}</script>`
	if p, _ := Page([]byte(named), 0, 104000000000123); p == nil || p.PageId != 100064000000777 {
		t.Fatalf("the HTML's id comes first: %+v", p)
	}
}
