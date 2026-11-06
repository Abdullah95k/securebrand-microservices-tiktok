package metrics

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestTmpUsage(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "profile"), 0o700); err != nil {
		t.Fatal(err)
	}
	for name, n := range map[string]int{"a": 1000, "profile/b": 2000} {
		if err := os.WriteFile(filepath.Join(dir, name), make([]byte, n), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("TKS_TMP_LIMIT_MB", "1")
	c := &tmpCollector{used: tmpUsage.used, limit: tmpUsage.limit, dir: dir, every: time.Minute}
	read := func() map[string]float64 {
		r := prometheus.NewRegistry()
		r.MustRegister(c)
		mfs, err := r.Gather()
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]float64{}
		for _, mf := range mfs {
			out[mf.GetName()] = mf.GetMetric()[0].GetGauge().GetValue()
		}
		return out
	}
	want := map[string]float64{"tks_tmp_bytes": 3000, "tks_tmp_limit_bytes": 1 << 20}
	if got := read(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	// Cached for a minute: a new file is not seen until then.
	if err := os.WriteFile(filepath.Join(dir, "c"), make([]byte, 500), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := read(); !reflect.DeepEqual(got, want) {
		t.Fatalf("cached: got %v, want %v", got, want)
	}
}
