package parse

import "testing"

func TestGraphIDs(t *testing.T) {
	if got := CommentIDFromGraphID("Y29tbWVudDo1NTVfODg4"); got != 888 { // comment:555_888
		t.Fatalf("comment id = %d", got)
	}
	if got := PostIDFromStoryID("UzpfSTEwMDo1NTU6NTU1"); got != 555 { // S:_I100:555:555
		t.Fatalf("post id = %d", got)
	}
	if FeedbackID(5) != "ZmVlZGJhY2s6NQ==" {
		t.Fatalf("feedback id = %s", FeedbackID(5))
	}
	first, again, other := MediaID("1", "sd"), MediaID("1", "sd"), MediaID("1", "hd")
	if first != again || first == other {
		t.Fatal("media ids must be deterministic per variant")
	}
}
