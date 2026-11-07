package httpserver

import (
	"context"
	"net"
	"net/http"
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
