package checkpoint

import (
	"context"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
)

// A post read claimed by one job is refused to another until released or
// expired; the same job may claim again, and only the holder releases.
func TestClaimPostRead(t *testing.T) {
	ctx := context.Background()
	s := New(testenv.Redis(t))
	post := time.Now().UnixNano() // unique per run
	if ok, err := s.ClaimPostRead(ctx, post, "job-a", time.Minute); err != nil || !ok {
		t.Fatalf("first claim: %v %v", ok, err)
	}
	if ok, _ := s.ClaimPostRead(ctx, post, "job-a", time.Minute); !ok {
		t.Error("the holder could not claim again")
	}
	if ok, _ := s.ClaimPostRead(ctx, post, "job-b", time.Minute); ok {
		t.Error("another job claimed a held read")
	}
	if err := s.ReleasePostRead(ctx, post, "job-b"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.ClaimPostRead(ctx, post, "job-b", time.Minute); ok {
		t.Error("a release by a non-holder freed the claim")
	}
	if err := s.ReleasePostRead(ctx, post, "job-a"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.ClaimPostRead(ctx, post, "job-b", 50*time.Millisecond); !ok {
		t.Error("the released read could not be claimed")
	}
	time.Sleep(120 * time.Millisecond)
	if ok, _ := s.ClaimPostRead(ctx, post, "job-c", time.Minute); !ok {
		t.Error("an expired claim still held the read")
	}
}

// A done marker lives DoneTTL (48 h), not the week it used to, which
// filled Valkey; it still answers IsDone and ClearDone drops it.
func TestDoneMarkerTTL(t *testing.T) {
	ctx := context.Background()
	r := testenv.Redis(t)
	s := New(r)
	task := "done-ttl-" + time.Now().Format(time.RFC3339Nano)
	if err := s.MarkDone(ctx, task); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.IsDone(ctx, task); err != nil || !ok {
		t.Fatalf("IsDone after MarkDone: %v %v", ok, err)
	}
	ttl, err := r.TTL(ctx, "tks:done:"+task).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 || ttl > DoneTTL || DoneTTL != 48*time.Hour {
		t.Errorf("done marker TTL %v (DoneTTL %v), want at most 48h", ttl, DoneTTL)
	}
	if err := s.ClearDone(ctx, task); err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.IsDone(ctx, task); ok {
		t.Error("marker still there after ClearDone")
	}
}
