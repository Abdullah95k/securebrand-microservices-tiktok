// Package procfs reads processes' memory and CPU from Linux's /proc: what
// the signer browsers use, so a page can be reopened and a run stopped
// before the machine runs short (the pilot server froze when Chrome grew to
// 7 GB, 2026-11-03). Elsewhere it reports nothing.
package procfs

import (
	"bufio"
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Proc is one process's parent and CPU time.
type Proc struct {
	PPID int
	// Ticks is user plus system time, in clock ticks (ClockTicks a second).
	Ticks int64
}

// ClockTicks is the kernel's USER_HZ (100 on Linux x86-64 and arm64).
const ClockTicks = 100

// Procs reads every process's parent and CPU time.
func Procs() map[int]Proc {
	out := map[int]Proc{}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return out
	}
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		b, err := os.ReadFile(filepath.Join("/proc", e.Name(), "stat"))
		if err != nil {
			continue
		}
		// The command name may hold spaces and parentheses: the fields
		// start after the last ')'.
		i := bytes.LastIndexByte(b, ')')
		if i < 0 {
			continue
		}
		f := strings.Fields(string(b[i+1:]))
		if len(f) < 13 {
			continue
		}
		ppid, _ := strconv.Atoi(f[1])
		ut, _ := strconv.ParseInt(f[11], 10, 64)
		st, _ := strconv.ParseInt(f[12], 10, 64)
		out[pid] = Proc{PPID: ppid, Ticks: ut + st}
	}
	return out
}

// Descendants are root's children, their children, and so on.
func Descendants(procs map[int]Proc, root int) []int {
	kids := map[int][]int{}
	for pid, p := range procs {
		kids[p.PPID] = append(kids[p.PPID], pid)
	}
	var out []int
	queue := []int{root}
	for len(queue) > 0 {
		p := queue[0]
		queue = queue[1:]
		for _, k := range kids[p] {
			out = append(out, k)
			queue = append(queue, k)
		}
	}
	return out
}

// TreeMB is the proportional set size of root and its descendants, in MB:
// a browser's memory, shared pages split between the processes sharing
// them.
func TreeMB(root int) float64 {
	mb := PSSMB(root)
	for _, pid := range Descendants(Procs(), root) {
		mb += PSSMB(pid)
	}
	return mb
}

// PSSMB is a process's proportional set size, or its resident size where
// the kernel has no smaps_rollup, in MB; 0 when unknown.
func PSSMB(pid int) float64 {
	if kb := Field(filepath.Join("/proc", strconv.Itoa(pid), "smaps_rollup"), "Pss:"); kb >= 0 {
		return kb / 1024
	}
	if kb := Field(filepath.Join("/proc", strconv.Itoa(pid), "status"), "VmRSS:"); kb >= 0 {
		return kb / 1024
	}
	return 0
}

// AvailMB is the machine's available memory (MemAvailable), in MB; 0 when
// unknown.
func AvailMB() float64 {
	if kb := Field("/proc/meminfo", "MemAvailable:"); kb >= 0 {
		return kb / 1024
	}
	return 0
}

// CgroupMB is this process's memory cgroup's anonymous memory and limit
// (cgroup v2), in MB; 0 when unknown or unlimited. Anonymous memory only:
// the page cache (the browser's own binary, written files) is given back
// before the limit bites.
func CgroupMB() (cur, limit float64) {
	b, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return 0, 0
	}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if p, ok := strings.CutPrefix(line, "0::"); ok {
			dir := filepath.Join("/sys/fs/cgroup", p)
			if v := Field(filepath.Join(dir, "memory.stat"), "anon "); v >= 0 {
				cur = v / (1 << 20) // memory.stat counts bytes
			}
			if v, err := os.ReadFile(filepath.Join(dir, "memory.max")); err == nil { //nolint:gosec // /sys
				if n, err := strconv.ParseFloat(strings.TrimSpace(string(v)), 64); err == nil {
					limit = n / (1 << 20)
				}
			}
		}
	}
	return cur, limit
}

// Field reads a "Name:   123 kB" line's number; -1 when there is none.
func Field(path, name string) float64 {
	f, err := os.Open(path) //nolint:gosec // /proc and /sys
	if err != nil {
		return -1
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, name) {
			fs := strings.Fields(line[len(name):])
			if len(fs) > 0 {
				if v, err := strconv.ParseFloat(fs[0], 64); err == nil {
					return v
				}
			}
		}
	}
	return -1
}
