package rawsink

import (
	"context"
	"testing"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
)

func keys(s *Sink) map[string]int {
	out := map[string]int{}
	for k, b := range s.bufs {
		out[k] = b.Len()
	}
	return out
}

// Production's layout (M18): failures and the sample under tks/keep/, the
// other answers under tks/30d/ with capture_raw all, dropped with sample.
func TestPrefixes(t *testing.T) {
	ctx := context.Background()
	for _, keepRest := range []bool{true, false} {
		s := New(nil, "b")
		s.KeepPrefix, s.RestPrefix, s.KeepRest = "tks/keep/", "tks/30d/", keepRest
		s.Capture(ctx, "creator_list", detect.RateLimited, []byte(`{}`), nil)
		s.Capture(ctx, "creator_list", detect.OK, []byte(`{}`), nil)
		k := keys(s)
		if k["tks/keep/creator_list"] == 0 {
			t.Fatalf("failure not kept: %v", k)
		}
		if _, ok := k["tks/30d/creator_list"]; ok != keepRest {
			t.Fatalf("keepRest %v: %v", keepRest, k)
		}
		// Every good answer sampled: kept for good, marked.
		s = New(nil, "b")
		s.KeepPrefix, s.RestPrefix, s.KeepRest, s.Sample = "tks/keep/", "tks/30d/", keepRest, 1
		s.Capture(ctx, "q", detect.OK, []byte(`{}`), nil)
		if b := s.bufs["tks/keep/q"]; b == nil || !containsSample(b.String()) || len(s.bufs) != 1 {
			t.Fatalf("sample: %v", keys(s))
		}
	}
}

func containsSample(s string) bool {
	for i := 0; i+len(`"sample":true`) <= len(s); i++ {
		if s[i:i+len(`"sample":true`)] == `"sample":true` {
			return true
		}
	}
	return false
}
