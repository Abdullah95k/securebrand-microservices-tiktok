package halt

import (
	"context"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
)

func TestStopAndStart(t *testing.T) {
	ctx := context.Background()
	rdb := testenv.Redis(t)
	g := NewGate(rdb, "t.")
	g.every = 20 * time.Millisecond
	if err := g.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := Stop(ctx, rdb, "t.", "block rate", time.Now()); err != nil {
		t.Fatal(err)
	}
	if st, _ := Get(ctx, rdb, "t."); !st.Stopped || st.Reason != "block rate" {
		t.Fatalf("state %+v", st)
	}
	if err := g.Refresh(ctx); err != nil || !g.Stopped() {
		t.Fatal("gate did not stop")
	}
	done := make(chan error, 1)
	go func() { done <- g.Wait(ctx) }()
	select {
	case <-done:
		t.Fatal("Wait returned while stopped")
	case <-time.After(100 * time.Millisecond):
	}
	if err := Start(ctx, rdb, "t."); err != nil {
		t.Fatal(err)
	}
	go func() { _ = g.Run(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after start")
	}
	// A stopped wait ends with its context.
	_, _ = Stop(ctx, rdb, "t.", "", time.Now())
	_ = g.Refresh(ctx)
	cctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if err := g.Wait(cctx); err == nil {
		t.Fatal("Wait ignored its context")
	}
}
