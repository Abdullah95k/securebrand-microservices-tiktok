package fbdisc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

func TestLinks(t *testing.T) {
	got := Links("Shop: https://www.facebook.com/shop.page. Also fb.me/xyz,", "facebook.com/shop.page",
		"m.facebook.com/profile.php?id=1 and https://example.com/facebook.com", "no link here")
	want := []string{"https://www.facebook.com/shop.page", "fb.me/xyz", "facebook.com/shop.page", "m.facebook.com/profile.php?id=1"}
	if !slices.Equal(got, want) {
		t.Errorf("Links = %q, want %q", got, want)
	}
}

type collector struct {
	mu     sync.Mutex
	bodies []map[string]string
	status int
	auth   []string
}

func (c *collector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var b map[string]string
	_ = json.NewDecoder(r.Body).Decode(&b)
	c.bodies = append(c.bodies, b)
	c.auth = append(c.auth, r.Header.Get("Authorization"))
	if c.status != 0 {
		w.WriteHeader(c.status)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"result":"queued"}`))
}

// Monitored accounts' Facebook links are sent once each, with the key and
// the profile as evidence; a 503 holds them for later; the day's cap
// holds the rest; a disabled sender, or an account not monitored, sends
// nothing.
func TestSender(t *testing.T) {
	ctx := context.Background()
	db, err := ttstore.Open(ctx, testenv.EmptyPostgres(t), 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := ttstore.MigrateUp(ctx, db); err != nil {
		t.Fatal(err)
	}
	for _, a := range []struct {
		uid             int64
		name, bio, link string
		enabled         bool
	}{
		{1, "brand", "Visit facebook.com/brand.page", "https://www.facebook.com/brand.page", true},
		{2, "other", "fb.me/other and facebook.com/brand.page", "", true},
		{3, "gone", "facebook.com/not.monitored", "", false},
	} {
		if _, err := db.Exec(ctx, `INSERT INTO accounts (user_id, sec_uid, username, author_key, status, bio, bio_link)
			VALUES ($1, $2, $3, '\x01', 'active', $4, NULLIF($5, ''))`, a.uid, "SEC"+a.name, a.name, a.bio, a.link); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(ctx, `INSERT INTO account_schedule (account_ref, user_id, interval_seconds, next_run_at, enabled)
			VALUES ($1, $2, 9900, now(), $3)`, "u:"+a.name, a.uid, a.enabled); err != nil {
			t.Fatal(err)
		}
	}
	col := &collector{}
	srv := httptest.NewServer(col)
	t.Cleanup(srv.Close)
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	s := &Sender{DB: db, Cfg: config.FacebookDiscovery{APIURL: srv.URL, APIKey: "k", MaxPerDay: 3}, Now: func() time.Time { return now }}

	if err := s.Tick(ctx); err != nil || len(col.bodies) != 0 {
		t.Fatalf("disabled: %d sent (%v)", len(col.bodies), err)
	}
	s.Cfg.Enabled = true
	col.status = http.StatusServiceUnavailable
	if err := s.Tick(ctx); err != nil || len(col.bodies) != 1 {
		t.Fatalf("503: %d sent, want 1 then stop (%v)", len(col.bodies), err)
	}
	col.status = 0
	if err := s.Tick(ctx); err != nil || len(col.bodies) != 1 {
		t.Fatalf("within the retry delay: %d sent (%v)", len(col.bodies), err)
	}
	now = now.Add(Retry + time.Minute)
	if err := s.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	refs := map[string]string{}
	for i, b := range col.bodies[1:] {
		if b["source"] != "tks" || col.auth[i+1] != "Bearer k" {
			t.Errorf("body %v auth %q", b, col.auth[i+1])
		}
		refs[b["ref"]] = b["evidence_url"]
	}
	if len(refs) != 3 {
		t.Fatalf("sent %v, want the 3 links of monitored accounts", refs)
	}
	if _, ok := refs["facebook.com/not.monitored"]; ok {
		t.Error("an account not monitored was sent")
	}
	if refs["fb.me/other"] != "https://www.tiktok.com/@other" {
		t.Errorf("evidence %q", refs["fb.me/other"])
	}
	n := len(col.bodies)
	if err := s.Tick(ctx); err != nil || len(col.bodies) != n {
		t.Fatalf("a link was sent twice (%d to %d, %v)", n, len(col.bodies), err)
	}
	// A new link is new; the day's cap (3 sent) holds it until tomorrow.
	if _, err := db.Exec(ctx, `UPDATE accounts SET bio = bio || ' facebook.com/new.page' WHERE user_id = 1`); err != nil {
		t.Fatal(err)
	}
	if err := s.Tick(ctx); err != nil || len(col.bodies) != n {
		t.Fatalf("over the day's cap: %d sent (%v)", len(col.bodies)-n, err)
	}
	now = now.Add(24 * time.Hour)
	if err := s.Tick(ctx); err != nil || len(col.bodies) != n+1 || col.bodies[n]["ref"] != "facebook.com/new.page" {
		t.Fatalf("next day: %v (%v)", col.bodies[n:], err)
	}
}
