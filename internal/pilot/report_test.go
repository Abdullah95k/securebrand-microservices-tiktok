package pilot

import (
	"strings"
	"testing"
	"time"
)

func TestFormatProjects(t *testing.T) {
	since := time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)
	r := Report{Since: since, Until: since.Add(48 * time.Hour), Posts: 200, MeanCompleteness: 0.93, SettledPosts: 120, SettledCompleteness: 0.97,
		Rows: []Row{
			{Tier: "T0", Kind: "backfill", Pages: 50, Jobs: 50, Requests: 14000, WireBytes: 1_100_000_000},
			{Tier: "T0", Kind: "scheduled", Pages: 50, Jobs: 800, Requests: 40000, WireBytes: 3_000_000_000, Blocked: 20},
			{Tier: "T2", Kind: "scheduled", Pages: 100, Jobs: 1600, Requests: 8000, WireBytes: 400_000_000},
		}}
	st := r.Steady()
	if st.Pages != 150 || st.Requests != 48000 || st.Blocked != 20 {
		t.Fatalf("steady state sums the scheduled jobs only: %+v", st)
	}
	out := Format(r, Projection{Pages: 5000, IdentityRPS: 0.33, CostPerGB: 4, Headroom: 0.5})
	t.Log("\n" + out)
	// 48000 requests over 150 pages and 2 days = 160 per page per day.
	for _, want := range []string{"over 2.0 days): 160 requests and 11.3 MB per page per day", "block rate 0.04%",
		"93.0% over 200 posts", "97.0% over the 120", "for 5000 pages: 0.80M requests and 56.67 GB per day",
		"about 43 IPs at 0.33 requests/s each (50% headroom)", "$227 a day"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q", want)
		}
	}
	// The distinct pages count, when known.
	r.SteadyPages = 120
	if st := r.Steady(); st.Pages != 120 {
		t.Fatalf("distinct pages: %+v", st)
	}
	// A pilot younger than --since: the steady state is averaged from the
	// first scheduled job, not over hours that had none.
	r.SteadyPages = 0
	r.Since = since.Add(-24 * time.Hour)
	r.SteadySince = since
	if d := r.SteadyDays(); d != 2 {
		t.Fatalf("steady days %v, want 2", d)
	}
	if out := Format(r, Projection{}); !strings.Contains(out, "over 2.0 days): 160 requests") {
		t.Fatalf("clamped span:\n%s", out)
	}
	if !strings.Contains(Format(Report{Since: since, Until: since.Add(time.Hour)}, Projection{}), "no scheduled jobs yet") {
		t.Fatal("an empty pilot says why")
	}
}
