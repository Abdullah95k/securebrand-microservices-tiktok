package e2e

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/discovery"
)

// TestDiscoveryPartB runs discovery part B end to end against fakett
// (docs/tiktok/discovery-plan.md §7): a monitored account's first read
// brings in the account its caption mentions, the one its bio names, the
// person who commented, the one it answered and the one a comment names;
// the persister records each once, the leader counts them, and each found
// account gets one entry check (the bio's by its username alone) and is
// added or kept as not Iraqi (yet). The account's own comments and replies
// bring nobody in; an entry check's posts bring nobody in; a re-read
// records nothing again; a new comment by a known commenter adds evidence
// with no read; and a source switched off records nothing.
func TestDiscoveryPartB(t *testing.T) {
	s := Start(t, Options{Spec: fakett.Spec{Seed: 43, Accounts: 1, PostsPerAccount: 3, PostSpacing: 30 * time.Hour},
		Config: func(c *config.Config) {
			c.TT.Discovery.Enabled = true
			c.TT.Discovery.Sources = map[string]bool{"mention": true, "reply": true, "commenter": true}
			c.Limits.IncludeComments = true
		}})
	w := s.World
	ctx := context.Background()
	dc := newDisco(s)
	owner := w.Accounts()[0]
	day := 24 * time.Hour

	tagged := w.AddAccount("pb_tagged", "", 4, 2*day, day) // in a caption; a post made in Iraq
	friend := w.AddAccount("pb_friend", "", 2, day, day)   // named in the bio; a post made in Iraq
	fan := w.AddAccount("pb_fan", "", 3, day, day)         // comments; the owner answers
	named := w.AddAccount("pb_named", "", 2, day, day)     // named in the fan's comment
	ghost := fakett.Person{UID: 7_000_000_000_000_000_123, SecUID: "MS4wLjABAAAAghost", Username: "pb_ghost"}
	w.Mutate(func() {
		tagged.Posts[0].Region = "IQ"
		tagged.Posts[1].Mentions = []fakett.Person{ghost} // read by tagged's entry check only
		friend.Posts[0].Region = "IQ"
		owner.Bio = "news every day, follow @pb_friend and @" + owner.Username
		owner.Posts[0].Mentions = []fakett.Person{tagged.Person, owner.Person}
	})
	c := w.AddComment(owner.Posts[1], fan.Person, named.Person)
	w.AddReply(c, owner.Person)                            // the owner answers the fan
	w.AddComment(owner.Posts[1], owner.Person)             // the owner's own comment
	w.AddComment(owner.Posts[2], owner.Person, fan.Person) // the owner names the fan in a comment

	dc.sc.register(t, owner)
	dc.sc.tick(t) // the owner's first read: posts, bio, comments and replies
	s.WaitPersisted(t, ctx)
	via := owner.UID
	sightings := func(how, in string) int64 {
		return count(t, s, `SELECT count(*) FROM discovery_sightings WHERE how = $1 AND found_in = $2 AND via_user_id = $3`, how, in, via)
	}
	for _, c := range []struct {
		how, in string
		want    int64
	}{
		{"mention", "caption", 1}, // tagged; not the owner itself
		{"mention", "bio", 1},     // pb_friend; not the owner's own name
		{"mention", "comment", 2}, // named in the fan's comment; the fan in the owner's
		{"commenter", "comment", 1},
		{"reply", "comment", 1},
	} {
		if n := sightings(c.how, c.in); n != c.want {
			t.Errorf("%s in %s: %d sightings, want %d", c.how, c.in, n, c.want)
		}
	}
	if n := count(t, s, `SELECT count(*) FROM discovery_sightings`); n != 6 {
		t.Errorf("%d sightings, want 6", n)
	}
	if n := count(t, s, `SELECT count(*) FROM discovery_sightings WHERE user_id = $1`, owner.UID); n != 0 {
		t.Errorf("the owner's own posts and comments brought the owner in %d times", n)
	}
	var bioUser *string
	var bioUID *int64
	if err := s.DB.QueryRow(ctx, `SELECT username, user_id FROM discovery_sightings WHERE found_in = 'bio'`).Scan(&bioUser, &bioUID); err != nil {
		t.Fatal(err)
	}
	if bioUser == nil || *bioUser != friend.Username || bioUID != nil {
		t.Errorf("bio sighting: username %v, user id %v", bioUser, bioUID)
	}
	var reply int64
	if err := s.DB.QueryRow(ctx, `SELECT user_id FROM discovery_sightings WHERE how = 'reply'`).Scan(&reply); err != nil {
		t.Fatal(err)
	}
	if reply != fan.UID {
		t.Errorf("the reply brought in %d, want the fan %d", reply, fan.UID)
	}

	// The leader counts them and sends each found account to one entry
	// check: the bio's by its username, the others by their secUid. The
	// one named in a comment carries no username (a comment's mention has
	// only ids, and tks stored none for it): on an identity that has not
	// loaded a page yet its check fails once and is tried again.
	checks := dc.round(t)
	for range 3 {
		if count(t, s, `SELECT count(*) FROM discovery_candidates WHERE state IN ('pending', 'checking')`) == 0 {
			break
		}
		checks += dc.round(t)
	}
	if checks < 4 || checks > 5 {
		t.Fatalf("%d entry checks, want 4 (5 with the retry)", checks)
	}
	dc.want(t, tagged, discovery.StateAdded, "")
	dc.want(t, friend, discovery.StateAdded, "")
	dc.want(t, fan, discovery.StateNotIraqi, "") // reply (medium) and commenter (weak) and mention (medium, same signal)
	dc.want(t, named, discovery.StateNotIraqi, "")
	if n := count(t, s, `SELECT count(*) FROM discovery_sightings WHERE counted_at IS NULL`); n != 0 {
		t.Errorf("%d sightings left uncounted", n)
	}
	sources := func(acc *fakett.Account) map[string]int {
		rows, err := s.DB.Query(ctx, `SELECT how, seen FROM account_sources WHERE user_id = $1 AND via = $2`, acc.UID, strconv.FormatInt(via, 10))
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		out := map[string]int{}
		for rows.Next() {
			var how string
			var seen int
			if err := rows.Scan(&how, &seen); err != nil {
				t.Fatal(err)
			}
			out[how] = seen
		}
		return out
	}
	if got := sources(fan); got["reply"] != 1 || got["commenter"] != 1 || got["mention"] != 1 {
		t.Errorf("the fan's sources: %v", got)
	}
	var ev string
	if err := s.DB.QueryRow(ctx, `SELECT evidence::text FROM account_sources WHERE user_id = $1 AND how = 'commenter'`, fan.UID).Scan(&ev); err != nil {
		t.Fatal(err)
	}
	if !contains(ev, `"found_in": "comment"`) || !contains(ev, `"post_id": "`+strconv.FormatInt(owner.Posts[1].ID, 10)+`"`) {
		t.Errorf("commenter evidence %s", ev)
	}
	if n := count(t, s, `SELECT count(*) FROM discovery_candidates WHERE user_id = $1`, ghost.UID); n != 0 {
		t.Error("an entry check's post brought someone in")
	}
	if n := count(t, s, `SELECT count(*) FROM discovery_sightings WHERE user_id = $1`, ghost.UID); n != 0 {
		t.Error("an entry check's post was recorded")
	}

	// The owner's next check re-reads the same posts and comments, and the
	// added accounts' backfills read theirs: nothing is recorded again.
	w.Advance(3 * time.Hour)
	dc.sc.tick(t)
	s.WaitPersisted(t, ctx)
	if n := count(t, s, `SELECT count(*) FROM discovery_sightings WHERE via_user_id = $1`, via); n != 6 {
		t.Errorf("after a re-read: %d sightings, want 6", n)
	}

	// A new comment by the fan: one more sighting, counted as evidence,
	// with no read.
	p := w.AddPost(owner, 0)
	w.AddComment(p, fan.Person)
	w.Advance(3 * time.Hour)
	dc.sc.tick(t)
	s.WaitPersisted(t, ctx)
	if n := sightings("commenter", "comment"); n != 2 {
		t.Errorf("after a new comment: %d commenter sightings, want 2", n)
	}
	if n := dc.round(t); n != 0 {
		t.Errorf("a known commenter was read again (%d checks)", n)
	}
	if got := sources(fan); got["commenter"] != 2 {
		t.Errorf("the fan's commenter source seen %d times, want 2", got["commenter"])
	}

	// A source switched off records nothing.
	s.Cfg.TT.Discovery.Sources["commenter"] = false
	s.RestartPersister(t)
	p = w.AddPost(owner, 0)
	w.AddComment(p, fan.Person)
	w.Advance(3 * time.Hour)
	dc.sc.tick(t)
	s.WaitPersisted(t, ctx)
	if n := sightings("commenter", "comment"); n != 2 {
		t.Errorf("with commenter off: %d commenter sightings, want 2", n)
	}
}
