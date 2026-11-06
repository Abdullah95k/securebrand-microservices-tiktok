package metrics

import (
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// The pod's scratch volume (/tmp: an emptyDir the kubelet evicts the pod
// over, chart value tmpSizeLimitMB) and how full it is: the signer's
// Chromium profiles and the Postgres CA live there (M16, alert
// TksScratchNearLimit). The walk is cached for a minute.
type tmpCollector struct {
	used, limit *prometheus.Desc
	dir         string
	every       time.Duration
	mu          sync.Mutex
	at          time.Time
	bytes       float64
}

var tmpUsage = &tmpCollector{
	used:  prometheus.NewDesc("tks_tmp_bytes", "Bytes of files under the pod's scratch directory (/tmp).", nil, nil),
	limit: prometheus.NewDesc("tks_tmp_limit_bytes", "The scratch volume's size limit (TKS_TMP_LIMIT_MB; absent when unset).", nil, nil),
	dir:   os.TempDir(), every: time.Minute,
}

func init() { Registry.MustRegister(tmpUsage) }

func (c *tmpCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.used; ch <- c.limit }

func (c *tmpCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	if time.Since(c.at) >= c.every {
		c.bytes, c.at = float64(dirBytes(c.dir)), time.Now()
	}
	used := c.bytes
	c.mu.Unlock()
	ch <- prometheus.MustNewConstMetric(c.used, prometheus.GaugeValue, used)
	if mb, err := strconv.ParseFloat(os.Getenv("TKS_TMP_LIMIT_MB"), 64); err == nil && mb > 0 {
		ch <- prometheus.MustNewConstMetric(c.limit, prometheus.GaugeValue, mb*1024*1024)
	}
}

// dirBytes sums regular files' sizes under dir; unreadable entries count 0.
func dirBytes(dir string) int64 {
	var n int64
	_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // skip what cannot be read
		}
		if d.Type().IsRegular() {
			if info, err := d.Info(); err == nil {
				n += info.Size()
			}
		}
		return nil
	})
	return n
}
