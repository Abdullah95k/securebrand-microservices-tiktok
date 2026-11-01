package media

import "testing"

// The raw bucket's rule covers every object (raw/ and raw-failures/), the
// media bucket's only media/.
func TestLifecycleRules(t *testing.T) {
	raw := rawLifecycleConfig(7).Rules[0]
	if raw.RuleFilter.Prefix != "" || raw.Expiration.Days != 7 || raw.Status != "Enabled" {
		t.Fatalf("raw rule: %+v", raw)
	}
	for _, key := range []string{"raw/timeline/2026/11/01/x.ndjson", "raw-failures/comments/x.ndjson"} {
		if p := raw.RuleFilter.Prefix; len(key) < len(p) || key[:len(p)] != p {
			t.Errorf("%s not covered by prefix %q", key, p)
		}
	}
}
