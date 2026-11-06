package report

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

func TestCheck(t *testing.T) {
	calm := &Traffic{RequestsTotal: 10000, Blocks: 100, Challenges: 10, SignedAnswered: 900, SignedRefused: 100,
		Restarts: []Count{{Labels: map[string]string{"container": "persister"}, Value: 1}}}
	s := Store{NewPosts: 100, Within3h: 1, CommentReads: map[string]int64{"complete": 99, "partial": 1}}
	if v := Check(DefaultRules, calm, s, 0.001); len(v) != 0 {
		t.Fatalf("calm run stopped: %v", v)
	}
	bad := *calm
	bad.Blocks, bad.Challenges, bad.SignedRefused = 900, 80, 600
	bad.OOMKills = []Count{{Labels: map[string]string{"container": "signer"}, Value: 1}}
	r := DefaultRules
	r.BudgetPerAccount = 0.0019
	v := Check(r, &bad, Store{NewPosts: 10, Within3h: 0.9}, 0.003)
	var stops, warns int
	for _, x := range v {
		if x.Level == "STOP" {
			stops++
		} else {
			warns++
		}
	}
	if stops != 5 || warns != 1 {
		t.Fatalf("verdicts %v", v)
	}
	rep := Report{Window: Window{From: time.Date(2026, 11, 7, 6, 0, 0, 0, time.UTC), To: time.Date(2026, 11, 8, 6, 0, 0, 0, time.UTC)},
		Traffic: &bad, Verdicts: v, Projections: Project(0.00126, 1000, 10000)}
	var b bytes.Buffer
	rep.Write(&b)
	out := b.String()
	for _, want := range []string{"2026-11-07 09:00 to 2026-11-08 09:00 Iraq time", "VERDICT: STOP", "OOM kill", "   1000 accounts: $     1.26 a day"} {
		if !strings.Contains(out, want) {
			t.Errorf("report lacks %q:\n%s", want, out)
		}
	}
}

func TestReadTraffic(t *testing.T) {
	var queries []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		queries = append(queries, q)
		res := ""
		switch {
		case strings.Contains(q, "tks_tt_requests_total"):
			res = `{"metric":{"query":"creator_list","class":"ok"},"value":[1,"950"]},{"metric":{"query":"creator_list","class":"checkpoint"},"value":[1,"50"]}`
		case strings.Contains(q, "tks_signed_calls_total"):
			res = `{"metric":{"result":"answered"},"value":[1,"40"]},{"metric":{"result":"refused"},"value":[1,"10"]}`
		case strings.Contains(q, "tks_proxy_bytes_total"):
			res = `{"metric":{},"value":[1,"2500000000"]}`
		}
		fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[%s]}}`, res)
	}))
	defer srv.Close()
	w := Window{From: time.Now().Add(-24 * time.Hour), To: time.Now()}
	tr, err := ReadTraffic(context.Background(), Prometheus{URL: srv.URL, Labels: `namespace="tks"`}, w)
	if err != nil {
		t.Fatal(err)
	}
	if tr.RequestsTotal != 1000 || tr.Blocks != 50 || tr.Challenges != 50 || tr.SignedRefused != 10 || tr.ProxyGB != 2.5 {
		t.Fatalf("traffic %+v", tr)
	}
	if !strings.Contains(queries[0], `{namespace="tks"}[86400s]`) {
		t.Fatalf("query %s", queries[0])
	}
	if !strings.Contains(strings.Join(tr.Missing, ","), "signatures") {
		t.Fatalf("missing %v", tr.Missing)
	}
}

func TestReadStore(t *testing.T) {
	ctx := context.Background()
	db, err := ttstore.Open(ctx, testenv.EmptyPostgres(t), 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := ttstore.MigrateUp(ctx, db); err != nil {
		t.Fatal(err)
	}
	s, err := ReadStore(ctx, db, Window{From: time.Now().Add(-time.Hour), To: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if s.NewPosts != 0 || s.DatabaseGB <= 0 {
		t.Fatalf("store %+v", s)
	}
}
