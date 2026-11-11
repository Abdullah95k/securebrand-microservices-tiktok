package report

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Prometheus reads instant queries from a Prometheus HTTP API.
type Prometheus struct {
	URL    string
	HTTP   *http.Client
	Labels string // extra selector, e.g. namespace="tks"
}

func (p Prometheus) query(ctx context.Context, q string, at time.Time) ([]Count, error) {
	u := strings.TrimSuffix(p.URL, "/") + "/api/v1/query?" + url.Values{
		"query": {q}, "time": {strconv.FormatInt(at.Unix(), 10)}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	hc := p.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: 30 * time.Second}
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	var body struct {
		Status string `json:"status"`
		Error  string `json:"error"`
		Data   struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  [2]any            `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("prometheus: %s: %w", resp.Status, err)
	}
	if body.Status != "success" {
		return nil, fmt.Errorf("prometheus: %s", body.Error)
	}
	out := make([]Count, 0, len(body.Data.Result))
	for _, r := range body.Data.Result {
		s, _ := r.Value[1].(string)
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			continue
		}
		out = append(out, Count{Labels: r.Metric, Value: v})
	}
	return out, nil
}

func sum(cs []Count) float64 {
	var t float64
	for _, c := range cs {
		t += c.Value
	}
	return t
}

// ReadTraffic reads the window's traffic. A metric Prometheus does not
// have (kube-state-metrics or cAdvisor not scraped, the signer not run
// yet) is listed in Missing; only an unreachable Prometheus is an error.
func ReadTraffic(ctx context.Context, p Prometheus, w Window) (*Traffic, error) {
	r := fmt.Sprintf("[%ds]", int64(w.Duration().Seconds()))
	sel := func(extra string) string {
		parts := []string{}
		if p.Labels != "" {
			parts = append(parts, p.Labels)
		}
		if extra != "" {
			parts = append(parts, extra)
		}
		return "{" + strings.Join(parts, ",") + "}"
	}
	t := &Traffic{}
	q := func(name, expr string, into *[]Count) error {
		cs, err := p.query(ctx, expr, w.To)
		if err != nil {
			return err
		}
		if len(cs) == 0 {
			t.Missing = append(t.Missing, name)
		}
		*into = cs
		return nil
	}
	var scratch []Count
	steps := []struct {
		name, expr string
		into       *[]Count
	}{
		{"requests", "sum by (query, class) (increase(tks_tt_requests_total" + sel("") + r + "))", &t.Requests},
		{"signatures", "sum by (result) (increase(tks_signer_signs_total" + sel("") + r + "))", &t.Signs},
		{"signed calls", "sum by (result) (increase(tks_signed_calls_total" + sel("") + r + "))", &scratch},
	}
	for _, s := range steps {
		if err := q(s.name, s.expr, s.into); err != nil {
			return nil, err
		}
		if s.name == "signed calls" {
			for _, c := range scratch {
				switch c.Labels["result"] {
				case "answered":
					t.SignedAnswered = c.Value
				case "refused":
					t.SignedRefused = c.Value
				}
			}
		}
	}
	t.RequestsTotal = sum(t.Requests)
	for _, c := range t.Requests {
		switch c.Labels["class"] {
		case "rate_limited", "login_wall", "checkpoint":
			t.Blocks += c.Value
			if c.Labels["class"] == "checkpoint" {
				t.Challenges += c.Value
			}
		}
	}
	scalars := []struct {
		name, expr string
		into       *float64
		div        float64
	}{
		{"proxy data", "sum(increase(tks_proxy_bytes_total" + sel("") + r + "))", &t.ProxyGB, 1e9},
		{"signer browser time", "sum(increase(tks_signer_page_seconds_total" + sel("") + r + "))", &t.BrowserHours, 3600},
	}
	for _, s := range scalars {
		if err := q(s.name, s.expr, &scratch); err != nil {
			return nil, err
		}
		*s.into = sum(scratch) / s.div
	}
	ns := sel(`container!=""`)
	if err := q("restarts (kube-state-metrics)", "sum by (container) (increase(kube_pod_container_status_restarts_total"+ns+r+"))", &t.Restarts); err != nil {
		return nil, err
	}
	// Restarts in the window of containers whose last end was an OOM kill.
	oom := "sum by (container) (increase(kube_pod_container_status_restarts_total" + ns + r + ") " +
		"and on (namespace, pod, container) kube_pod_container_status_last_terminated_reason" + sel(`reason="OOMKilled"`) + " == 1)"
	if err := q("OOM kills", oom, &t.OOMKills); err != nil {
		return nil, err
	}
	// No OOM kill is the usual answer, not a missing metric.
	t.Missing = removeName(t.Missing, "OOM kills")
	for _, s := range []struct {
		name, expr string
		into       *[]Count
	}{
		{"planner slots", "sum by (outcome) (increase(tks_planner_slots_total" + sel("") + r + "))", &t.PlannerSlots},
		{"scheduler loop errors", "sum by (loop) (increase(tks_scheduler_loop_errors_total" + sel("") + r + "))", &t.LoopErrors},
	} {
		if err := q(s.name, s.expr, s.into); err != nil {
			return nil, err
		}
		t.Missing = removeName(t.Missing, s.name) // none is the usual answer
	}
	for _, s := range []struct {
		name string
		phi  float64
		into *float64
	}{{"check duration p50", 0.5, &t.CheckP50Min}, {"check duration p90", 0.9, &t.CheckP90Min}} {
		if err := q(s.name, fmt.Sprintf(`histogram_quantile(%g, sum by (le) (increase(tks_check_duration_seconds_bucket%s%s)))`,
			s.phi, sel(`kind="check"`), r), &scratch); err != nil {
			return nil, err
		}
		t.Missing = removeName(t.Missing, s.name)
		if v := sum(scratch); v == v { // not NaN (no check ended)
			*s.into = v / 60
		}
	}
	if err := q("new comments (24 h)", "sum by (post_age) (increase(tks_comments_new_total"+sel("")+"[24h]))", &t.CommentsNew24h); err != nil {
		return nil, err
	}
	if err := q("memory (cAdvisor)", "max by (container) (max_over_time(container_memory_working_set_bytes"+ns+r+")) / 1e6", &t.MemoryPeakMB); err != nil {
		return nil, err
	}
	return t, nil
}

func removeName(xs []string, x string) []string {
	out := xs[:0]
	for _, v := range xs {
		if v != x {
			out = append(out, v)
		}
	}
	return out
}
