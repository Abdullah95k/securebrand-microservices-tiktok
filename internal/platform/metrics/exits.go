package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// ExitCounts is one proxy tier's exit groups (breaker buckets: a static
// exit's /24, a gateway's country) by state.
type ExitCounts struct {
	Usable  int // breaker closed or half-open: new identities may start there
	Cooling int // breaker open: no new identity until its cooldown ends
}

// proxyExits reports tks_proxy_exits{tier, state} at each scrape from the
// process's proxy pool, so a breaker whose cooldown ended counts as usable
// again without waiting for a request to move it (M16; alert
// TksUsableExitsLow).
type exitsCollector struct {
	desc *prometheus.Desc
	mu   sync.Mutex
	src  func() map[string]ExitCounts
}

var proxyExits = &exitsCollector{desc: prometheus.NewDesc("tks_proxy_exits",
	"Proxy exit groups (breaker buckets) of each tier this pod can start identities on (state usable) or that cool down after blocks (state cooling).",
	[]string{"tier", "state"}, nil)}

func init() { Registry.MustRegister(proxyExits) }

// SetProxyExits makes f the source of tks_proxy_exits (the proxy pool sets
// it; the last pool made wins: a process has one).
func SetProxyExits(f func() map[string]ExitCounts) {
	proxyExits.mu.Lock()
	proxyExits.src = f
	proxyExits.mu.Unlock()
}

func (c *exitsCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *exitsCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	src := c.src
	c.mu.Unlock()
	if src == nil {
		return
	}
	for tier, n := range src() {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(n.Usable), tier, "usable")
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(n.Cooling), tier, "cooling")
	}
}
