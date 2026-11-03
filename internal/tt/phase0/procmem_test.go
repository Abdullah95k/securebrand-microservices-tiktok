package phase0

import (
	"bytes"
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// The sampler finds this process's children (the browsers), measures
// them, and when the guard trips stops the engine, then kills a child that
// stays over the line.
func TestSamplerMeasuresAndGuards(t *testing.T) {
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Skip(err)
	}
	exited := make(chan struct{})
	go func() { _ = cmd.Wait(); close(exited) }()
	defer func() { _ = cmd.Process.Kill() }()

	var buf bytes.Buffer
	var cause error
	stop := func(err error) { cause = err }
	clock := time.Now()
	s := newSampler("test", &buf, time.Hour, MemGuard{MaxBrowserMB: 1e-6}, context.CancelCauseFunc(stop))
	s.now = func() time.Time { return clock }
	s.setPhase("work")
	samples, reason, err := s.result()
	if err != nil || len(samples) != 1 {
		t.Fatalf("%d samples, %v", len(samples), err)
	}
	if samples[0].Procs < 1 || samples[0].BrowserMB <= 0 || samples[0].HostAvailMB <= 0 {
		t.Fatalf("sample %+v", samples[0])
	}
	var low *ErrLowMemory
	if !errors.As(cause, &low) || !strings.Contains(reason, "browser memory") {
		t.Fatalf("guard: %v, %q", cause, reason)
	}
	select {
	case <-exited:
		t.Fatal("killed before its grace")
	default:
	}
	clock = clock.Add(11 * time.Second)
	s.sample()
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("a child over the line was not killed")
	}
	if !strings.Contains(buf.String(), `"engine":"test"`) {
		t.Fatalf("samples written: %s", buf.String())
	}
}

func TestDescendants(t *testing.T) {
	procs := map[int]procStat{2: {ppid: 1}, 3: {ppid: 2}, 4: {ppid: 3}, 5: {ppid: 9}}
	got := descendants(procs, 2)
	if len(got) != 2 || got[0] != 3 || got[1] != 4 {
		t.Fatalf("%v", got)
	}
}
