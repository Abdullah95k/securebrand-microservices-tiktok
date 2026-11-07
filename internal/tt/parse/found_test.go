package parse_test

import (
	"testing"

	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/parse"
)

// foundItem is a synthetic post item in the shape every list carries.
func foundItem(id, author string) string {
	return `{"id":"` + id + `","createTime":1762000000,"desc":"x #tag","textLanguage":"ar",
	  "author":{"id":"` + author + `","uniqueId":"u` + author[len(author)-2:] + `","secUid":"MS4wLjABAAAA` + author[len(author)-2:] + `",
	    "nickname":"N","signature":"","privateAccount":false},
	  "authorStats":{"followerCount":10,"videoCount":4},
	  "stats":{"playCount":5,"diggCount":1,"commentCount":0,"shareCount":0,"collectCount":0},
	  "music":{"id":"7000000000000000099","title":"s"},
	  "textExtra":[{"hashtagName":"tag","hashtagId":"123","start":2,"end":6,"type":1}]}`
}

// Synthetic answers in the shapes of the owner's search recording and the
// discovery probe (T36), all invented.
func TestFoundSearchShape(t *testing.T) {
	body := `{"status_code":0,"has_more":1,"cursor":12,"extra":{"search_request_id":"sid-1"},"log_pb":{"impr_id":"imp"},"data":[
	  {"type":4,"user_list":[{"user_info":{"uid":"6900000000000000031","unique_id":"card","sec_uid":"MS4wLjABAAAAc","follower_count":7}}]},
	  {"type":1,"item":` + foundItem("7400000000000000001", "6900000000000000011") + `},
	  {"type":1,"item":` + foundItem("7400000000000000002", "6900000000000000011") + `},
	  {"type":1,"item":` + foundItem("7400000000000000001", "6900000000000000011") + `},
	  {"type":1,"item":` + foundItem("7400000000000000003", "6900000000000000012") + `}]}`
	res, rep := parse.Found([]byte(body), ttv1.DiscoveredVia_DISCOVERED_VIA_SEARCH, parse.Options{AuthorKey: key, Query: "search_general"})
	if !rep.OK() || !res.HasMore || res.Cursor != "12" || res.SearchID != "sid-1" || res.Shape != "data.item" || res.StatusCode != 0 {
		t.Fatalf("%+v %+v", rep, res)
	}
	if len(res.Posts) != 3 {
		t.Fatalf("%d posts, want 3 (a repeated item read once)", len(res.Posts))
	}
	p := res.Posts[0].Post
	if p.DiscoveredVia != ttv1.DiscoveredVia_DISCOVERED_VIA_SEARCH || p.UserId != 6900000000000000011 || p.Pinned != nil ||
		len(p.Hashtags) != 1 || p.Hashtags[0].Name != "tag" {
		t.Fatalf("post %v", p)
	}
	if len(res.Authors) != 2 || res.Authors[0].SecUid != "MS4wLjABAAAA11" || res.Authors[1].UserId != 6900000000000000012 {
		t.Fatalf("authors %v", res.Authors)
	}
	if len(res.Cards) != 1 || res.Cards[0].Person.GetUserId() != 6900000000000000031 {
		t.Fatalf("cards %v", res.Cards)
	}
}

func TestFoundItemListShape(t *testing.T) {
	body := `{"statusCode":0,"hasMore":false,"cursor":"60","itemList":[` + foundItem("7400000000000000004", "6900000000000000013") + `]}`
	res, rep := parse.Found([]byte(body), ttv1.DiscoveredVia_DISCOVERED_VIA_HASHTAG, parse.Options{})
	if !rep.OK() || res.HasMore || res.Cursor != "60" || res.Shape != "itemList" || len(res.Posts) != 1 || len(res.Authors) != 1 {
		t.Fatalf("%+v %+v", rep, res)
	}
	if res.Posts[0].Post.DiscoveredVia != ttv1.DiscoveredVia_DISCOVERED_VIA_HASHTAG || res.Posts[0].Music == nil {
		t.Fatalf("post %v", res.Posts[0])
	}
	// The end of a list: no items and hasMore false is not drift.
	if res, rep := parse.Found([]byte(`{"statusCode":0,"hasMore":false}`), ttv1.DiscoveredVia_DISCOVERED_VIA_SOUND, parse.Options{}); !rep.OK() || len(res.Posts) != 0 {
		t.Fatalf("%+v %+v", rep, res)
	}
}

func TestFoundRefusedAndDrift(t *testing.T) {
	// Search from a US exit (C6 run 1): 2483 is a status, not drift.
	res, rep := parse.Found([]byte(`{"status_code":2483,"status_msg":"Please login your account first"}`), ttv1.DiscoveredVia_DISCOVERED_VIA_SEARCH, parse.Options{})
	if !rep.OK() || res.StatusCode != 2483 {
		t.Fatalf("%+v %+v", rep, res)
	}
	for _, body := range []string{``, `{}`, `not json`} {
		if _, rep := parse.Found([]byte(body), ttv1.DiscoveredVia_DISCOVERED_VIA_FEED, parse.Options{}); rep.OK() {
			t.Errorf("%q: no drift reported", body)
		}
	}
}

func TestChallengeDetail(t *testing.T) {
	res, rep := parse.ChallengeDetail([]byte(`{"statusCode":0,"challengeInfo":{"challenge":{"id":"1234567890","title":"tag"},"stats":{"videoCount":42}}}`))
	if !rep.OK() || res.ID != 1234567890 || res.Title != "tag" || res.Posts == nil || *res.Posts != 42 {
		t.Fatalf("%+v %+v", rep, res)
	}
	if res, rep := parse.ChallengeDetail([]byte(`{"statusCode":10205,"statusMsg":"challenge not exist"}`)); !rep.OK() || res.ID != 0 || res.StatusCode != 10205 {
		t.Fatalf("%+v %+v", rep, res)
	}
	if _, rep := parse.ChallengeDetail([]byte(`{"other":1}`)); rep.OK() {
		t.Fatal("drift not reported")
	}
}
