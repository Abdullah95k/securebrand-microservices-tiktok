package stage

import (
	"context"
	"fmt"
	"testing"

	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/persist"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/task"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/tttest"
)

// listState is a follow_lists row.
type listState struct {
	state, stop, deep string
	done              bool
	people, pages     int64
}

func readList(t *testing.T, h *harness, user int64, dir string) listState {
	t.Helper()
	var s listState
	var deep *string
	if err := h.d.DB.QueryRow(context.Background(), `SELECT state, coalesce(last_stop, ''), deep_cursor, deep_done, people_seen, pages_read
		FROM follow_lists WHERE user_id = $1 AND direction = $2`, user, dir).Scan(&s.state, &s.stop, &deep, &s.done, &s.people, &s.pages); err != nil {
		t.Fatal(err)
	}
	if deep != nil {
		s.deep = *deep
	}
	return s
}

// readFollows runs one list read as a job of its own, from what
// tt.follow_lists says, and stores its links as the persister would; it
// returns the job's status and the people it published.
func readFollows(t *testing.T, h *harness, job string, acc *fakett.Account, dir ttv1.FollowDirection, maxPages int32) (jobs.Status, []*ttv1.Follow) {
	t.Helper()
	ctx := context.Background()
	name := "follower"
	if dir == ttv1.FollowDirection_FOLLOW_DIRECTION_FOLLOWING {
		name = "following"
	}
	sc := &tkstaskv1.FollowScope{UserId: acc.UID, SecUid: acc.SecUID, Username: acc.Username, Direction: dir, MaxPages: maxPages}
	var deep *string
	err := h.d.DB.QueryRow(ctx, `SELECT deep_cursor, deep_done, people_seen > 0 FROM follow_lists WHERE user_id = $1 AND direction = $2`,
		acc.UID, name).Scan(&deep, &sc.DeepDone, &sc.ReadBefore)
	if err == nil && deep != nil {
		sc.DeepCursor = *deep
	}
	if err := h.d.spawn(ctx, task.NewFollows(job, sc)); err != nil {
		t.Fatal(err)
	}
	st := h.drain(job)
	fs := records(h, topics.Follow, job, func() *ttv1.Follow { return &ttv1.Follow{} })
	w := &persist.Writer{DB: h.d.DB}
	if err := w.Follows(ctx, fs); err != nil {
		t.Fatal(err)
	}
	return st, fs
}

// A first read walks the followers list from the top to the read's cap,
// across a yield; a re-read takes the new people at the top down to those
// stored, then finishes the deep walk from where the first stopped; a third
// read stops at the first page of known people. Every person is published
// with what the list showed of them, never the account itself.
func TestFollowersReads(t *testing.T) {
	h := newHarness(t, tttest.Options{})
	w := h.env.World
	acc := w.Accounts()[0]
	var fans []*fakett.Account
	for i := range 100 {
		f := w.AddAccount(fmt.Sprintf("fan%03d", i), "", i%3, 10*day, day)
		fans = append(fans, f)
	}
	w.Mutate(func() { fans[7].Private = true })
	for i := len(fans) - 1; i >= 0; i-- {
		w.AddFollower(acc, fans[i]) // fans[0] newest
	}
	w.AddFollower(acc, acc) // TikTok would not; tks drops it

	st, fs := readFollows(t, h, "followers-1", acc, ttv1.FollowDirection_FOLLOW_DIRECTION_FOLLOWER, 3)
	if st.Counters["follow_pages"] != 3 || st.Counters["follow_stop_cap"] != 1 || st.Counters["tasks_failed"] != 0 {
		t.Fatalf("first read: counters %v", st.Counters)
	}
	if st.Spawned < 2 {
		t.Errorf("first read: %d tasks: no continuation", st.Spawned)
	}
	if len(fs) != 89 { // 90 listed, the account itself dropped
		t.Fatalf("first read published %d people", len(fs))
	}
	byID := map[int64]*ttv1.Follow{}
	for _, f := range fs {
		if f.UserId != acc.UID || f.Direction != ttv1.FollowDirection_FOLLOW_DIRECTION_FOLLOWER || f.Person.GetUserId() == acc.UID ||
			len(f.Person.AuthorKey) == 0 {
			t.Fatalf("follow %v", f)
		}
		byID[f.Person.GetUserId()] = f
	}
	if f := byID[fans[7].UID]; f == nil || !f.GetPrivate() || f.GetVideoCount() != int64(7%3) {
		t.Fatalf("fan 7: %v", f)
	}
	if f := byID[fans[0].UID]; f == nil || f.Page != 0 || f.Position != 1 {
		t.Fatalf("fan 0 (newest): %v", f)
	}
	if ls := readList(t, h, acc.UID, "follower"); ls.state != "ok" || ls.stop != FollowStopCap || ls.deep != "90" || ls.done || ls.people != 89 || ls.pages != 3 {
		t.Fatalf("after the first read: %+v", ls)
	}

	// Five new followers at the top.
	var news []*fakett.Account
	for i := range 5 {
		f := w.AddAccount(fmt.Sprintf("new%02d", i), "", 1, day, day)
		news = append(news, f)
		w.AddFollower(acc, f)
	}
	st, fs = readFollows(t, h, "followers-2", acc, ttv1.FollowDirection_FOLLOW_DIRECTION_FOLLOWER, 3)
	if st.Counters["follow_pages"] != 3 || st.Counters["follow_stop_end"] != 1 {
		t.Fatalf("second read: counters %v", st.Counters)
	}
	seen := map[int64]bool{}
	for _, f := range fs {
		seen[f.Person.GetUserId()] = true
	}
	for _, f := range append(news, fans[90:]...) {
		if !seen[f.UID] {
			t.Errorf("second read missed %s", f.Username)
		}
	}
	var links int
	if err := h.d.DB.QueryRow(context.Background(), `SELECT count(*) FROM follows WHERE user_id = $1 AND direction = 'follower'`, acc.UID).
		Scan(&links); err != nil || links != 105 {
		t.Fatalf("%d links stored (%v), want 105", links, err)
	}
	if ls := readList(t, h, acc.UID, "follower"); ls.stop != FollowStopEnd || !ls.done || ls.deep != "" || ls.pages != 6 {
		t.Fatalf("after the second read: %+v", ls)
	}

	st, _ = readFollows(t, h, "followers-3", acc, ttv1.FollowDirection_FOLLOW_DIRECTION_FOLLOWER, 3)
	if st.Counters["follow_pages"] != 1 || st.Counters["follow_stop_known"] != 1 {
		t.Fatalf("third read: counters %v", st.Counters)
	}
}

// A following list the profile hides answers 10222: the read ends, nothing
// is published, the list is recorded hidden. A list TikTok will not serve
// a logged-out visitor (fakett's login answer) is tried once more and then
// recorded refused, with its code; no job fails.
func TestFollowListsRefused(t *testing.T) {
	h := newHarness(t, tttest.Options{})
	w := h.env.World
	acc := w.Accounts()[1]
	w.Mutate(func() { acc.FollowingHidden = true })
	st, fs := readFollows(t, h, "following-hidden", acc, ttv1.FollowDirection_FOLLOW_DIRECTION_FOLLOWING, 5)
	if len(fs) != 0 || st.Counters["follow_stop_hidden"] != 1 || st.Counters["tasks_failed"] != 0 {
		t.Fatalf("hidden: %d people, counters %v", len(fs), st.Counters)
	}
	if ls := readList(t, h, acc.UID, "following"); ls.state != "hidden" {
		t.Fatalf("hidden list: %+v", ls)
	}

	h.env.Fake.SetFaults(fakett.Faults{FollowListsNeedLogin: true})
	st, fs = readFollows(t, h, "followers-login", acc, ttv1.FollowDirection_FOLLOW_DIRECTION_FOLLOWER, 5)
	if len(fs) != 0 || st.Counters["follow_stop_refused"] != 1 || st.Counters["soft_errors"] != 1 || st.Counters["tasks_failed"] != 0 {
		t.Fatalf("refused: %d people, counters %v", len(fs), st.Counters)
	}
	var code int64
	if err := h.d.DB.QueryRow(context.Background(), `SELECT status_code FROM follow_lists WHERE user_id = $1 AND direction = 'follower' AND state = 'refused'`,
		acc.UID).Scan(&code); err != nil || code != fakett.LoginCode {
		t.Fatalf("refused list: code %d (%v)", code, err)
	}
}
