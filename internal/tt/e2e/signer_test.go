package e2e

import (
	"fmt"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
)

// TestRemoteSignerDownAndBack runs the crawler as the chart deploys it
// (M9d): tt.signer.mode remote, each comment and reply call signed by
// tks-signer over HTTP. tks-signer is unreachable when the backfills
// start: the comment reads wait in their retries (no request is sent
// unsigned, nothing is lost), and once it is back every job completes with
// the store equal to fakett's world (A6).
func TestRemoteSignerDownAndBack(t *testing.T) {
	s := Start(t, Options{Spec: fakett.Spec{Seed: 11, Accounts: 2, PostsPerAccount: 8, MaxComments: 25, MaxReplies: 4,
		PostSpacing: 20 * time.Hour}, RemoteSigner: true})
	s.SignerDown.Store(true)
	accs := s.World.Accounts()
	ids := make([]string, len(accs))
	for i, a := range accs {
		ids[i] = fmt.Sprintf("remote-%d", i)
		s.Queue(t, ids[i], "u:"+a.Username, full)
	}
	for deadline := time.Now().Add(time.Minute); s.SignerDropped.Load() == 0; {
		if time.Now().After(deadline) {
			t.Fatal("no call reached tks-signer")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if n := s.Signer.Calls.Load(); n != 0 {
		t.Fatalf("%d calls signed while tks-signer was down", n)
	}
	time.Sleep(time.Second)
	s.SignerDown.Store(false)
	signErrors := int64(0)
	for id, st := range s.Wait(t, 3*time.Minute, ids...) {
		if st.Counters["tasks_failed"] != 0 {
			t.Fatalf("job %s: %v", id, st.Counters)
		}
		signErrors += st.Counters["sign_errors"]
	}
	if signErrors == 0 {
		t.Error("no sign error counted while tks-signer was down")
	}
	for _, a := range accs {
		sameStore(t, s, a, truth{})
	}
	if s.Signer.Calls.Load() == 0 {
		t.Error("no comment call signed through tks-signer")
	}
	t.Logf("%d connections dropped while down, %d sign errors retried, %d calls signed once back",
		s.SignerDropped.Load(), signErrors, s.Signer.Calls.Load())
}
