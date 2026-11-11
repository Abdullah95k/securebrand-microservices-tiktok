package report

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/ratelimit"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/store"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

func TestCheck(t *testing.T) {
	calm := &Traffic{RequestsTotal: 10000, Blocks: 100, Challenges: 10, SignedAnswered: 900, SignedRefused: 100,
		Restarts: []Count{{Labels: map[string]string{"container": "persister"}, Value: 1}}}
	s := Store{NewPosts: 100, Within3h: 1, CommentReads: map[string]int64{"complete": 99, "partial": 1}}
	if v := Check(DefaultRules, calm, s, 0.001); len(v) != 0 {
		t.Fatalf("calm run stopped: %v", v)
	}
	bad := *calm
	bad.Blocks, bad.Challenges, bad.SignedRefused = 900, 80, 600
	bad.OOMKills = []Count{{Labels: map[string]string{"container": "signer"}, Value: 1}}
	r := DefaultRules
	r.BudgetPerAccount = 0.0019
	v := Check(r, &bad, Store{NewPosts: 10, Within3h: 0.9}, 0.003)
	var stops, warns int
	for _, x := range v {
		if x.Level == "STOP" {
			stops++
		} else {
			warns++
		}
	}
	if stops != 5 || warns != 1 {
		t.Fatalf("verdicts %v", v)
	}
	bad.CommentsNew24h = []Count{{Labels: map[string]string{"post_age": "recent"}, Value: 1200},
		{Labels: map[string]string{"post_age": "older"}, Value: 34000}}
	rep := Report{Window: Window{From: time.Date(2026, 11, 7, 6, 0, 0, 0, time.UTC), To: time.Date(2026, 11, 8, 6, 0, 0, 0, time.UTC)},
		Traffic: &bad, Verdicts: v, Projections: Project(0.00126, 1000, 10000), Store: Store{PendingBackfills: 12}}
	var b bytes.Buffer
	if err := rep.Write(&b); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{"2026-11-07 09:00 to 2026-11-08 09:00 Iraq time", "VERDICT: STOP", "OOM kill", "   1000 accounts: $     1.26 a day",
		"New comments and replies stored, last 24 h: 35200 (posts up to 30 days old 1200, older posts 34000)",
		"Backfills still to run (admitted accounts whose list was never read): 12"} {
		if !strings.Contains(out, want) {
			t.Errorf("report lacks %q:\n%s", want, out)
		}
	}
}

func TestReadTraffic(t *testing.T) {
	var queries []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		queries = append(queries, q)
		res := ""
		switch {
		case strings.Contains(q, "tks_tt_requests_total"):
			res = `{"metric":{"query":"creator_list","class":"ok"},"value":[1,"950"]},{"metric":{"query":"creator_list","class":"checkpoint"},"value":[1,"50"]}`
		case strings.Contains(q, "tks_signed_calls_total"):
			res = `{"metric":{"result":"answered"},"value":[1,"40"]},{"metric":{"result":"refused"},"value":[1,"10"]}`
		case strings.Contains(q, "tks_proxy_bytes_total"):
			res = `{"metric":{},"value":[1,"2500000000"]}`
		}
		fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[%s]}}`, res)
	}))
	defer srv.Close()
	w := Window{From: time.Now().Add(-24 * time.Hour), To: time.Now()}
	tr, err := ReadTraffic(context.Background(), Prometheus{URL: srv.URL, Labels: `namespace="tks"`}, w)
	if err != nil {
		t.Fatal(err)
	}
	if tr.RequestsTotal != 1000 || tr.Blocks != 50 || tr.Challenges != 50 || tr.SignedRefused != 10 || tr.ProxyGB != 2.5 {
		t.Fatalf("traffic %+v", tr)
	}
	if !strings.Contains(queries[0], `{namespace="tks"}[86400s]`) {
		t.Fatalf("query %s", queries[0])
	}
	if !strings.Contains(strings.Join(tr.Missing, ","), "signatures") {
		t.Fatalf("missing %v", tr.Missing)
	}
}

func TestReadStore(t *testing.T) {
	ctx := context.Background()
	db, err := ttstore.Open(ctx, testenv.EmptyPostgres(t), 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := ttstore.MigrateUp(ctx, db); err != nil {
		t.Fatal(err)
	}
	// Two complete comment reads (9 of 10 and 2 of 4 collected), one
	// partial and one of a post without comments: the ratio is the median of
	// the two complete reads of posts with a count.
	now := time.Now()
	if _, err := ttstore.EnsurePartitions(ctx, db, now, store.PartitionWindow{MonthsBack: 7, MonthsAhead: 1, DaysBack: 1, DaysAhead: 1}); err != nil {
		t.Fatal(err)
	}
	for i, p := range []struct {
		state                     string
		count, collected, replies int64
	}{{"complete", 10, 8, 1}, {"complete", 4, 2, 0}, {"partial", 10, 0, 0}, {"complete", 0, 0, 0}} {
		if _, err := db.Exec(ctx, `INSERT INTO posts (created_at, post_id, user_id, kind, content_hash, first_seen_at, last_seen_at,
			comments_state, comment_count, comments_collected, replies_collected, comments_synced_at)
			VALUES ($1, $2, 7, 'video', '\x00', $1, $1, $3, $4, $5, $6, $1)`,
			now.Add(-time.Minute), i+1, p.state, p.count, p.collected, p.replies); err != nil {
			t.Fatal(err)
		}
	}
	// Checks' first finds: one new post, stored 2 h after it was published,
	// and one published 154 days ago, which is an older find, not new.
	for i, created := range []time.Time{now.Add(-150 * time.Minute), now.Add(-154 * 24 * time.Hour)} {
		if _, err := db.Exec(ctx, `INSERT INTO posts (created_at, post_id, user_id, kind, content_hash, first_seen_at, last_seen_at,
			first_mode, stored_at) VALUES ($1, $2, 8, 'video', '\x00', $3, $3, 'incremental', $3)`,
			created, 100+i, now.Add(-30*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	// Media held now: two ready videos (one without delete_after: stored +
	// 72 h), a ready cover, and a deleted video that is not held.
	for i, m := range []struct {
		kind, status string
		size         int64
		stored       time.Time
		deleteAfter  *time.Time
	}{
		{"video", "ready", 3_000_000, now.Add(-70 * time.Hour), nil},
		{"video", "ready", 2_000_000, now.Add(-time.Hour), ptr(now.Add(71 * time.Hour))},
		{"cover", "ready", 50_000, now.Add(-time.Hour), ptr(now.Add(71 * time.Hour))},
		{"video", "deleted", 9_000_000, now.Add(-80 * time.Hour), ptr(now.Add(-8 * time.Hour))},
	} {
		if _, err := db.Exec(ctx, `INSERT INTO media (media_id, source_media_id, kind, variant, post_id, status, size_bytes, stored_at, delete_after)
			VALUES (gen_random_uuid(), $1::bigint::text, $2, 'original', $1::bigint, $3, $4, $5, $6)`,
			int64(7_400_000_000_000_000_001+i), m.kind, m.status, m.size, m.stored, m.deleteAfter); err != nil {
			t.Fatal(err)
		}
	}
	// Three admitted accounts, one read (its list's cursor), one never
	// read, and a disabled one never read: one backfill to run.
	for i, a := range []struct {
		enabled, read bool
	}{{true, true}, {true, false}, {false, false}} {
		uid := int64(500 + i)
		if _, err := db.Exec(ctx, `INSERT INTO account_schedule (account_ref, user_id, interval_seconds, next_run_at, admitted_at, enabled)
			VALUES ($1, $2, 9900, now(), now(), $3)`, fmt.Sprintf("id:%d", uid), uid, a.enabled); err != nil {
			t.Fatal(err)
		}
		if a.read {
			if _, err := db.Exec(ctx, `INSERT INTO crawl_cursors (scope_type, scope_id, state) VALUES ('account_list', $1, 'complete')`, uid); err != nil {
				t.Fatal(err)
			}
		}
	}
	s, err := ReadStore(ctx, db, Window{From: now.Add(-time.Hour), To: now})
	if err != nil {
		t.Fatal(err)
	}
	if s.NewPosts != 1 || s.OlderFinds != 1 || s.Within3h != 1 || s.DatabaseGB <= 0 {
		t.Fatalf("store %+v", s)
	}
	if s.CommentRatioReads != 2 || s.CommentRatio < 0.699 || s.CommentRatio > 0.701 {
		t.Fatalf("comment ratio %.3f over %d reads, want 0.70 over 2", s.CommentRatio, s.CommentRatioReads)
	}
	if s.PendingBackfills != 1 {
		t.Errorf("pending backfills %d, want 1 (the enabled account never read)", s.PendingBackfills)
	}
	if len(s.MediaHeld) != 2 {
		t.Fatalf("media held %+v, want cover and video", s.MediaHeld)
	}
	c, v := s.MediaHeld[0], s.MediaHeld[1]
	if c.Kind != "cover" || c.Files != 1 || c.Bytes != 50_000 {
		t.Errorf("cover held %+v", c)
	}
	if v.Kind != "video" || v.Files != 2 || v.Bytes != 5_000_000 ||
		v.NextDeadline.Sub(now.Add(2*time.Hour)).Abs() > time.Second || v.LastDeadline.Sub(now.Add(71*time.Hour)).Abs() > time.Second {
		t.Errorf("video held %+v: want 2 files, 5 MB, next in 2 h (stored + 72 h), last in 71 h", v)
	}
}

func ptr[T any](v T) *T { return &v }

// The budget's pause of the backfill lane shows as a WARN, not a STOP:
// checks go on.
func TestBudgetPause(t *testing.T) {
	if v := BudgetPause(ratelimit.BudgetState{Stretch: 1}); v != nil {
		t.Fatalf("p2 running: %+v", v)
	}
	at := time.Date(2026, 11, 11, 13, 0, 0, 0, time.UTC)
	v := BudgetPause(ratelimit.BudgetState{PauseP2: true, UpdatedAt: at,
		Explanation: "cost $0.001904 per account a day is over the budget $0.001900: backfill paused, checks go on"})
	if v == nil || v.Level != "WARN" || !strings.Contains(v.Rule, "p2 paused by the budget") ||
		!strings.Contains(v.Rule, "$0.001904") || !strings.Contains(v.Rule, "2026-11-11 16:00") {
		t.Fatalf("verdict %+v", v)
	}
	if (Report{Verdicts: []Verdict{*v}}).Stop() {
		t.Error("a budget pause made the report STOP")
	}
}
