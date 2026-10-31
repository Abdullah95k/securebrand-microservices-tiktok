package walk

import (
	"fmt"
	"testing"

	"github.com/tidwall/gjson"
)

// Instagram objects carry no __typename: a kind function recognises them by
// their fields. A carousel's children are collected under it, and the same
// media repeated at the same place is returned once.
func TestCollectByKind(t *testing.T) {
	body := `{"data":{"xig_user_by_username":{"pk":"10","username":"u","timeline":{"edges":[
		{"node":{"pk":"1","id":"1_10","code":"A","media_type":8,"carousel_media":[
			{"pk":"11","id":"11_10","media_type":1},{"pk":"12","id":"12_10","media_type":2}]}},
		{"node":{"pk":"2","id":"2_10","code":"B","media_type":1}},
		{"node":{"pk":"2","id":"2_10","code":"B","media_type":1}}]}}}}`
	kind := func(o gjson.Result) string {
		switch {
		case o.Get("username").Exists() && o.Get("pk").Exists():
			return "user"
		case o.Get("media_type").Exists() && o.Get("pk").Exists():
			return "media"
		}
		return ""
	}
	nodes := CollectBy([]gjson.Result{gjson.Parse(body)}, kind)
	var got []string
	for _, n := range nodes {
		got = append(got, n.Kind+":"+n.V.Get("pk").String())
	}
	want := "[user:10 media:1 media:11 media:12 media:2]"
	if s := fmt.Sprint(got); s != want {
		t.Fatalf("got %s, want %s", s, want)
	}
	child := nodes[2]
	if p := child.Ancestor("media"); p == nil || p.V.Get("pk").String() != "1" {
		t.Fatalf("carousel child's parent: %+v", p)
	}
	if u := child.Ancestor("user"); u == nil || u.V.Get("pk").String() != "10" {
		t.Fatalf("carousel child's user: %+v", u)
	}
	if child.Ancestor("comment") != nil {
		t.Fatal("found an ancestor kind that is not there")
	}
}

// Collect is CollectBy over __typename.
func TestCollectByTypename(t *testing.T) {
	body := `[{"__typename":"Story","id":"s1","x":{"__typename":"Comment","id":"c1"}},{"__typename":"Other","id":"o"}]`
	nodes := Collect([]gjson.Result{gjson.Parse(body)}, "Story", "Comment")
	if len(nodes) != 2 || nodes[0].Kind != "Story" || nodes[1].Kind != "Comment" || nodes[1].Parent != nodes[0] {
		t.Fatalf("nodes %+v", nodes)
	}
}

func TestFindPageInfoAndReport(t *testing.T) {
	body := `{"data":{"node":{"polaris_ordered_timeline_connection":{"edges":[],
		"page_info":{"has_next_page":true,"end_cursor":"CUR"}}}}}`
	pi := FindPageInfo([]gjson.Result{gjson.Parse(body)}, []string{"polaris_ordered_timeline_connection"})
	if !pi.Found || !pi.HasNext || pi.EndCursor != "CUR" {
		t.Fatalf("page info %+v", pi)
	}
	var r Report
	r.AddOptional("caption")
	if !r.OK() {
		t.Fatal("an optional field makes no drift")
	}
	r.Merge(Report{Missing: []string{"media.pk"}})
	if r.OK() || len(r.Missing) != 1 || len(r.Optional) != 1 {
		t.Fatalf("report %+v", r)
	}
}
