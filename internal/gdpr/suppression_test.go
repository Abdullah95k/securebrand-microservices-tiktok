package gdpr

import (
	"encoding/json"
	"testing"

	fbv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/fb/v1"
)

// Person mentions lose their id, profile URL and name; hashtags, pages and
// links keep theirs, and the offsets stay so the text reads the same.
func TestScrubMentions(t *testing.T) {
	rs := []*fbv1.TextRange{
		{Offset: 0, Length: 4, EntityType: "User", EntityId: "100001", Url: "https://www.facebook.com/someone", Name: "Some One"},
		{Offset: 5, Length: 3, EntityType: "Hashtag", EntityId: "h1", Url: "https://www.facebook.com/hashtag/x", Name: "#x"},
		{Offset: 9, Length: 3, EntityType: "Page", EntityId: "p1", Url: "https://www.facebook.com/nike", Name: "Nike"},
		{Offset: 13, Length: 2, EntityType: "SomethingNew", EntityId: "n1", Url: "u", Name: "n"},
	}
	ScrubMentions(rs)
	if u := rs[0]; u.EntityId != "" || u.Url != "" || u.Name != "" || u.Offset != 0 || u.Length != 4 || u.EntityType != "User" {
		t.Errorf("user mention: %+v", u)
	}
	if rs[1].EntityId != "h1" || rs[2].Name != "Nike" {
		t.Errorf("non-person entities were scrubbed: %+v %+v", rs[1], rs[2])
	}
	if rs[3].EntityId != "" || rs[3].Name != "" {
		t.Errorf("unknown entity types count as people: %+v", rs[3])
	}
}

func TestScrubMentionsJSON(t *testing.T) {
	raw := []byte(`[{"offset":0,"length":4,"entity_type":"User","entity_id":"100001","url":"https://www.facebook.com/someone","name":"Some One"},` +
		`{"offset":5,"length":3,"entity_type":"Hashtag","entity_id":"h1","name":"#x"},{"offset":9,"length":2,"entity_id":"9","name":"untyped"}]`)
	var got []map[string]any
	if err := json.Unmarshal(ScrubMentionsJSON(raw), &got); err != nil || len(got) != 3 {
		t.Fatalf("%v %v", got, err)
	}
	for _, k := range []string{"entity_id", "url", "name"} {
		if _, ok := got[0][k]; ok {
			t.Errorf("user mention kept %s", k)
		}
		if _, ok := got[2][k]; ok && k != "url" {
			t.Errorf("untyped range kept %s", k)
		}
	}
	if got[0]["offset"] != float64(0) || got[0]["length"] != float64(4) || got[1]["entity_id"] != "h1" || got[1]["name"] != "#x" {
		t.Errorf("wrong fields kept: %v", got)
	}
	if ScrubMentionsJSON(nil) != nil || string(ScrubMentionsJSON([]byte{})) != "" {
		t.Error("empty input comes back empty")
	}
	if ScrubMentionsJSON([]byte(`{"not":"a list"}`)) != nil {
		t.Error("unreadable input must not pass through")
	}
}
