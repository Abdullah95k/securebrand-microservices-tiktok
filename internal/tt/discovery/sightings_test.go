package discovery

import (
	"slices"
	"testing"
)

func TestBioMentions(t *testing.T) {
	for _, c := range []struct {
		bio, own string
		want     []string
	}{
		{"", "me", nil},
		{"follow @Friend.One and @friend.one", "me", []string{"friend.one"}},
		{"@me is me, @other_2 too", "me", []string{"other_2"}},
		{"mail me: name@example.org", "me", nil},
		{"ends with a period @someone.", "me", []string{"someone"}},
		{"arabic around @iq_news، تابعونا", "me", []string{"iq_news"}},
		{"too short @a, word@b, @@double", "me", nil},
		{"(@paren) [@bracket]", "me", []string{"paren", "bracket"}},
		{"@.... only periods", "me", nil},
	} {
		if got := BioMentions(c.bio, c.own); !slices.Equal(got, c.want) {
			t.Errorf("BioMentions(%q) = %q, want %q", c.bio, got, c.want)
		}
	}
	var long string
	for i := range 30 {
		long += " @user_" + string(rune('a'+i%26)) + string(rune('a'+i/26))
	}
	if got := BioMentions(long, ""); len(got) != maxBioMentions {
		t.Errorf("%d usernames from a long bio, want %d", len(got), maxBioMentions)
	}
}
