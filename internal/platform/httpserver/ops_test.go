package httpserver

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Started early, the ops server answers /healthz while the binary is still
// opening its dependencies, and /readyz reports "starting" until Run.
func TestEarly(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	o := NewOps(addr)
	o.Early()
	get := func(path string) int {
		t.Helper()
		var resp *http.Response
		for range 50 {
			if resp, err = http.Get("http://" + addr + path); err == nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if c := get("/healthz"); c != http.StatusOK {
		t.Errorf("/healthz while starting: %d", c)
	}
	if c := get("/readyz"); c != http.StatusServiceUnavailable {
		t.Errorf("/readyz while starting: %d", c)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- o.Run(ctx) }()
	time.Sleep(50 * time.Millisecond)
	if c := get("/readyz"); c != http.StatusOK {
		t.Errorf("/readyz after Run: %d", c)
	}
	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run: %v", err)
	}
}

// /readyz runs its checks at most once per CheckEvery and answers with the
// last result in between, failures included.
func TestReadyzReusesItsResult(t *testing.T) {
	defer func(d time.Duration) { CheckEvery = d }(CheckEvery)
	CheckEvery = 200 * time.Millisecond
	o := NewOps("")
	calls := 0
	var fail error
	o.AddCheck("postgres", func(context.Context) error { calls++; return fail })
	h := o.Handler()
	get := func() int {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		return rec.Code
	}
	for range 5 {
		if c := get(); c != http.StatusOK {
			t.Fatalf("/readyz: %d", c)
		}
	}
	if calls != 1 {
		t.Fatalf("%d checks for 5 probes within CheckEvery, want 1", calls)
	}
	fail = errors.New("down")
	time.Sleep(250 * time.Millisecond)
	if c := get(); c != http.StatusServiceUnavailable || calls != 2 {
		t.Fatalf("after CheckEvery: %d, %d checks", c, calls)
	}
	fail = nil
	if c := get(); c != http.StatusServiceUnavailable || calls != 2 {
		t.Fatalf("a failure is reused too: %d, %d checks", c, calls)
	}
	// /healthz never runs a check.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK || calls != 2 {
		t.Fatalf("/healthz: %d, %d checks", rec.Code, calls)
	}
}
