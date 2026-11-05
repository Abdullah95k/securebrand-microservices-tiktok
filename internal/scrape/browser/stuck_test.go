package browser

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/websocket"
)

// A page event whose answer Chromium never gives must not hold the Session.
// In C6 (2026-11-05) a TikTok For You page sent a request with post data and
// never answered Network.getRequestPostData: the pump waited for good, and
// Close, which waited for the pump before cancelling its call, never
// returned, so the signer pool that closed the page froze the probe's lane.
// Here a fake DevTools endpoint answers nothing: the pump goes on to the next
// event after postDataTimeout, and Close returns within seconds.
func TestSessionSurvivesAnUnansweredEventCall(t *testing.T) {
	asked := make(chan string, 16)
	srv := httptest.NewServer(websocket.Handler(func(ws *websocket.Conn) {
		_ = websocket.JSON.Send(ws, map[string]any{"method": "Network.requestWillBeSent", "params": map[string]any{
			"requestId": "1", "request": map[string]any{"url": "https://www.tiktok.example/api/beacon", "method": "POST", "hasPostData": true}}})
		_ = websocket.JSON.Send(ws, map[string]any{"method": "Page.loadEventFired", "params": map[string]any{}})
		for {
			var m message
			if err := websocket.JSON.Receive(ws, &m); err != nil {
				return
			}
			asked <- m.Method // and never answered
		}
	}))
	defer srv.Close()
	c, err := dial(context.Background(), "ws"+strings.TrimPrefix(srv.URL, "http"))
	if err != nil {
		t.Fatal(err)
	}
	r := &renderer{c: c, s: "page", opts: Options{Capture: func(string) bool { return true }, CallTimeout: time.Second},
		docs: map[string]document{}, auth: map[string]int{}, reqs: map[string]*Exchange{}, inflight: map[string]bool{}}
	sctx, cancel := context.WithCancel(context.Background())
	s := &Session{r: r, stop: func() { _ = c.ws.Close() }, ctx: sctx, cancel: cancel, quit: make(chan struct{}), pumpDone: make(chan struct{})}
	go s.pump()

	select {
	case m := <-asked:
		if m != "Network.getRequestPostData" {
			t.Fatalf("first call %s, want Network.getRequestPostData", m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the pump never asked for the post data")
	}
	// The pump gives up on the answer and handles the next event.
	loaded := func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return r.loaded
	}
	deadline := time.Now().Add(postDataTimeout + 3*time.Second)
	for !loaded() {
		if time.Now().After(deadline) {
			t.Fatal("the pump is still waiting for an answer Chromium never gives")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Close's own Browser.close goes unanswered too: Close still returns.
	done := make(chan struct{})
	start := time.Now()
	go func() {
		s.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(closeWait + 5*time.Second):
		t.Fatal("Close did not return")
	}
	if d := time.Since(start); d > closeWait {
		t.Errorf("Close took %v, want it to cancel the call in flight rather than wait for it", d)
	}
}

// Close returns within closeWait even when the pump is stuck in a call
// whose context it cannot see (a handler that ignored cancellation).
func TestCloseIsBoundedWhenThePumpIsStuck(t *testing.T) {
	srv := httptest.NewServer(websocket.Handler(func(ws *websocket.Conn) {
		for {
			var m message
			if err := websocket.JSON.Receive(ws, &m); err != nil {
				return
			}
		}
	}))
	defer srv.Close()
	c, err := dial(context.Background(), "ws"+strings.TrimPrefix(srv.URL, "http"))
	if err != nil {
		t.Fatal(err)
	}
	stuck := make(chan struct{})
	defer close(stuck)
	sctx, cancel := context.WithCancel(context.Background())
	s := &Session{r: &renderer{c: c}, stop: func() { _ = c.ws.Close() }, ctx: sctx, cancel: cancel, quit: make(chan struct{}),
		pumpDone: make(chan struct{})}
	go func() { // a pump that never finishes
		<-stuck
		close(s.pumpDone)
	}()
	start := time.Now()
	s.Close()
	if d := time.Since(start); d > closeWait+3*time.Second {
		t.Errorf("Close took %v, want at most about %v", d, closeWait+2*time.Second)
	}
}
