package metrics

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v3"
)

// The alert rules and dashboards shipped with the chart must only use
// metrics that exist: a renamed metric otherwise silently disables an alert.

const chartFiles = "../../../deploy/helm/fbs/files"

var (
	fqName    = regexp.MustCompile(`fqName: "([^"]+)"`)
	igsMetric = regexp.MustCompile(`\bigs_[a-z0-9_]+`)
	igsRecord = regexp.MustCompile(`\bigs:[a-z0-9_:]+`)
)

// known returns every series name the registry can expose: histograms add
// _bucket, _sum and _count.
func known(t *testing.T) map[string]bool {
	t.Helper()
	ch := make(chan *prometheus.Desc, 512)
	go func() {
		Registry.Describe(ch)
		close(ch)
	}()
	out := map[string]bool{}
	for d := range ch {
		m := fqName.FindStringSubmatch(d.String())
		if m == nil {
			continue
		}
		for _, suffix := range []string{"", "_bucket", "_sum", "_count"} {
			out[m[1]+suffix] = true
		}
	}
	if !out["igs_ig_requests_total"] {
		t.Fatal("registry description lacks igs_ig_requests_total")
	}
	return out
}

func balanced(expr string) bool {
	pairs := map[rune]rune{')': '(', ']': '[', '}': '{'}
	var stack []rune
	inStr := false
	for _, r := range expr {
		switch {
		case r == '"':
			inStr = !inStr
		case inStr:
		case r == '(' || r == '[' || r == '{':
			stack = append(stack, r)
		case pairs[r] != 0:
			if len(stack) == 0 || stack[len(stack)-1] != pairs[r] {
				return false
			}
			stack = stack[:len(stack)-1]
		}
	}
	return !inStr && len(stack) == 0
}

type ruleFile struct {
	Groups []struct {
		Name  string `yaml:"name"`
		Rules []struct {
			Alert       string            `yaml:"alert"`
			Record      string            `yaml:"record"`
			Expr        string            `yaml:"expr"`
			For         string            `yaml:"for"`
			Labels      map[string]string `yaml:"labels"`
			Annotations map[string]string `yaml:"annotations"`
		} `yaml:"rules"`
	} `yaml:"groups"`
}

func loadRules(t *testing.T) ruleFile {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(chartFiles, "alerts.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var rf ruleFile
	if err := yaml.Unmarshal(b, &rf); err != nil {
		t.Fatal(err)
	}
	if len(rf.Groups) == 0 {
		t.Fatal("no rule groups")
	}
	return rf
}

func records(rf ruleFile) map[string]bool {
	out := map[string]bool{}
	for _, g := range rf.Groups {
		for _, r := range g.Rules {
			if r.Record != "" {
				out[r.Record] = true
			}
		}
	}
	return out
}

func checkExpr(t *testing.T, where, expr string, metrics, recs map[string]bool) {
	t.Helper()
	if strings.TrimSpace(expr) == "" {
		t.Errorf("%s: empty expression", where)
		return
	}
	if !balanced(expr) {
		t.Errorf("%s: unbalanced brackets or quotes: %s", where, expr)
	}
	for _, m := range igsMetric.FindAllString(expr, -1) {
		if !metrics[m] {
			t.Errorf("%s: unknown metric %s", where, m)
		}
	}
	for _, r := range igsRecord.FindAllString(expr, -1) {
		if !recs[r] {
			t.Errorf("%s: unknown recording rule %s", where, r)
		}
	}
}

func TestAlertRules(t *testing.T) {
	ms := known(t)
	rf := loadRules(t)
	recs := records(rf)
	alerts := map[string]bool{}
	for _, g := range rf.Groups {
		for _, r := range g.Rules {
			switch {
			case r.Record != "":
				if !strings.HasPrefix(r.Record, "igs:") {
					t.Errorf("recording rule %s must be named igs:...", r.Record)
				}
				checkExpr(t, r.Record, r.Expr, ms, recs)
			case r.Alert != "":
				if alerts[r.Alert] {
					t.Errorf("duplicate alert %s", r.Alert)
				}
				alerts[r.Alert] = true
				checkExpr(t, r.Alert, r.Expr, ms, recs)
				switch r.Labels["severity"] {
				case "critical", "warning", "info":
				default:
					t.Errorf("%s: severity %q", r.Alert, r.Labels["severity"])
				}
				if r.For == "" {
					t.Errorf("%s: no for duration", r.Alert)
				}
				for _, a := range []string{"summary", "description", "runbook_url"} {
					if r.Annotations[a] == "" {
						t.Errorf("%s: no %s annotation", r.Alert, a)
					}
				}
				if !strings.Contains(r.Annotations["runbook_url"], "docs/runbooks/") {
					t.Errorf("%s: runbook_url should point at a runbook", r.Alert)
				}
			default:
				t.Errorf("group %s: rule without alert or record", g.Name)
			}
		}
	}
	if len(alerts) < 10 {
		t.Fatalf("only %d alerts", len(alerts))
	}
}

// Runbook anchors the alerts link to must exist (GitHub heading slugs).
func TestAlertRunbookAnchors(t *testing.T) {
	b, err := os.ReadFile("../../../docs/runbooks/operations.md")
	if err != nil {
		t.Fatal(err)
	}
	slugs := map[string]bool{}
	drop := regexp.MustCompile(`[^a-z0-9 _-]`)
	for _, line := range strings.Split(string(b), "\n") {
		if h, ok := strings.CutPrefix(line, "## "); ok {
			slugs[strings.ReplaceAll(drop.ReplaceAllString(strings.ToLower(h), ""), " ", "-")] = true
		}
	}
	for _, g := range loadRules(t).Groups {
		for _, r := range g.Rules {
			u := r.Annotations["runbook_url"]
			if i := strings.IndexByte(u, '#'); i >= 0 && !slugs[u[i+1:]] {
				t.Errorf("%s: no runbook heading for #%s", r.Alert, u[i+1:])
			}
		}
	}
}

type panel struct {
	ID         int    `json:"id"`
	Type       string `json:"type"`
	Title      string `json:"title"`
	Datasource *struct {
		UID string `json:"uid"`
	} `json:"datasource"`
	Targets []struct {
		Expr string `json:"expr"`
	} `json:"targets"`
	Panels []panel `json:"panels"`
}

func TestDashboards(t *testing.T) {
	ms := known(t)
	recs := records(loadRules(t))
	files, _ := filepath.Glob(filepath.Join(chartFiles, "dashboards", "*.json"))
	if len(files) == 0 {
		t.Fatal("no dashboards")
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var d struct {
			UID    string  `json:"uid"`
			Title  string  `json:"title"`
			Panels []panel `json:"panels"`
		}
		if err := json.Unmarshal(b, &d); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		if d.UID == "" || d.Title == "" {
			t.Fatalf("%s: uid and title are required", f)
		}
		ids := map[int]bool{}
		var walk func([]panel)
		walk = func(ps []panel) {
			for _, p := range ps {
				if ids[p.ID] {
					t.Errorf("%s: duplicate panel id %d", f, p.ID)
				}
				ids[p.ID] = true
				walk(p.Panels)
				if p.Type == "row" {
					continue
				}
				if p.Datasource == nil || p.Datasource.UID != "${datasource}" {
					t.Errorf("%s: panel %q must use the ${datasource} variable", f, p.Title)
				}
				if len(p.Targets) == 0 {
					t.Errorf("%s: panel %q has no query", f, p.Title)
				}
				for _, tg := range p.Targets {
					checkExpr(t, d.Title+" / "+p.Title, tg.Expr, ms, recs)
				}
			}
		}
		walk(d.Panels)
	}
}

// TestNoFbsSeries guards ADR 0004 §5: fbs's alert rules select by metric
// name alone and some of its gauges are always exported, so a single fbs_
// series from an igs pod on a shared Prometheus would feed fbs's alerts.
func TestNoFbsSeries(t *testing.T) {
	for name := range known(t) {
		if !strings.HasPrefix(name, "igs_") && !strings.HasPrefix(name, "go_") && !strings.HasPrefix(name, "process_") {
			t.Errorf("series %s is not in the igs_ namespace", name)
		}
	}
}
