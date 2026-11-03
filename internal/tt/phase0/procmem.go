package phase0

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/procfs"
)

// MemSample is one reading of the signer browser's processes (this
// process's children: the browsers it started) and of the machine.
type MemSample struct {
	T      time.Time `json:"t"`
	Engine string    `json:"engine"`
	Phase  string    `json:"phase"`
	// BrowserMB is the browsers' proportional set size (PSS: shared pages
	// split between the processes that share them), in MB.
	BrowserMB float64 `json:"browser_mb"`
	Procs     int     `json:"procs"`
	// CPUPct is the browsers' CPU since the last sample, in % of one core.
	CPUPct float64 `json:"cpu_pct"`
	// HostAvailMB is the machine's MemAvailable.
	HostAvailMB float64 `json:"host_avail_mb"`
	// The memory cgroup this process runs in (the systemd scope or
	// container that caps it): its anonymous memory and its limit.
	CgroupMB    float64 `json:"cgroup_mb,omitempty"`
	CgroupMaxMB float64 `json:"cgroup_max_mb,omitempty"`
}

// MemGuard stops a browser before it takes the machine down: the pilot
// server froze when Chrome grew to 7 GB (2026-11-03).
type MemGuard struct {
	// MaxBrowserMB stops the engine when its browsers pass this.
	MaxBrowserMB float64
	// MinHostMB stops it when the machine has less available.
	MinHostMB float64
	// CgroupShare stops it at this share of the cgroup's limit.
	CgroupShare float64
}

func (g MemGuard) check(s MemSample) string {
	switch {
	case g.MaxBrowserMB > 0 && s.BrowserMB > g.MaxBrowserMB:
		return fmt.Sprintf("browser memory %.0f MB passed %.0f MB", s.BrowserMB, g.MaxBrowserMB)
	case g.MinHostMB > 0 && s.HostAvailMB > 0 && s.HostAvailMB < g.MinHostMB:
		return fmt.Sprintf("the machine had %.0f MB available, under %.0f MB", s.HostAvailMB, g.MinHostMB)
	case g.CgroupShare > 0 && s.CgroupMaxMB > 0 && s.CgroupMB > g.CgroupShare*s.CgroupMaxMB:
		return fmt.Sprintf("the cap's memory %.0f MB passed %.0f%% of its %.0f MB", s.CgroupMB, 100*g.CgroupShare, s.CgroupMaxMB)
	}
	return ""
}

// ErrLowMemory is the cause of an engine's stop by the guard.
type ErrLowMemory struct{ Reason string }

func (e *ErrLowMemory) Error() string { return "stopped for memory: " + e.Reason }

// sampler reads the browsers' memory and CPU every interval, writes the
// samples, and stops the engine (stop) when the guard trips; a browser
// still over the line 10 s later is killed.
type sampler struct {
	engine string
	every  time.Duration
	guard  MemGuard
	stop   context.CancelCauseFunc
	enc    *json.Encoder
	root   int
	now    func() time.Time

	mu      sync.Mutex
	phase   string
	samples []MemSample
	ticks   map[int]int64
	last    time.Time
	tripped time.Time
	reason  string
	err     error

	quit chan struct{}
	done chan struct{}
}

func newSampler(engine string, w io.Writer, every time.Duration, guard MemGuard, stop context.CancelCauseFunc) *sampler {
	return &sampler{engine: engine, every: every, guard: guard, stop: stop, enc: json.NewEncoder(w), root: os.Getpid(),
		now: time.Now, phase: "start", ticks: map[int]int64{}, quit: make(chan struct{}), done: make(chan struct{})}
}

func (s *sampler) setPhase(p string) {
	s.mu.Lock()
	s.phase = p
	s.mu.Unlock()
	s.sample()
}

func (s *sampler) start() {
	go func() {
		defer close(s.done)
		t := time.NewTicker(s.every)
		defer t.Stop()
		for {
			select {
			case <-s.quit:
				return
			case <-t.C:
				s.sample()
			}
		}
	}()
}

// finish takes a last sample and stops sampling.
func (s *sampler) finish() {
	s.sample()
	close(s.quit)
	<-s.done
}

func (s *sampler) sample() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	procs := procfs.Procs()
	kids := procfs.Descendants(procs, s.root)
	smp := MemSample{T: now.UTC(), Engine: s.engine, Phase: s.phase, Procs: len(kids), HostAvailMB: procfs.AvailMB()}
	var used int64
	seen := map[int]int64{}
	for _, pid := range kids {
		smp.BrowserMB += procfs.PSSMB(pid)
		t := procs[pid].Ticks
		seen[pid] = t
		used += t - s.ticks[pid] // a new process: all its time is since the last sample
	}
	s.ticks = seen
	if !s.last.IsZero() {
		if secs := now.Sub(s.last).Seconds(); secs > 0 {
			smp.CPUPct = 100 * float64(used) / procfs.ClockTicks / secs
		}
	}
	s.last = now
	smp.CgroupMB, smp.CgroupMaxMB = procfs.CgroupMB()
	s.samples = append(s.samples, smp)
	if s.err == nil {
		s.err = s.enc.Encode(smp)
	}
	why := s.guard.check(smp)
	switch {
	case why != "" && s.tripped.IsZero():
		s.tripped, s.reason = now, why
		s.stop(&ErrLowMemory{Reason: why})
	case why != "" && now.Sub(s.tripped) > 10*time.Second:
		// Still over the line after the stop: the browser did not close.
		for _, pid := range kids {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	}
}

func (s *sampler) result() ([]MemSample, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.samples, s.reason, s.err
}

// HostAvailMB is the machine's available memory (MemAvailable), in MB.
func HostAvailMB() float64 { return procfs.AvailMB() }

// CgroupMemory is this process's memory cgroup's anonymous memory and
// limit, in MB; the limit is 0 when there is none (no cap).
func CgroupMemory() (cur, limit float64) { return procfs.CgroupMB() }
