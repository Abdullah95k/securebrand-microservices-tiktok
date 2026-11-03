package procfs

import (
	"os"
	"os/exec"
	"slices"
	"testing"
)

func TestTreeMeasuresChildren(t *testing.T) {
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Skip(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	if !slices.Contains(Descendants(Procs(), os.Getpid()), cmd.Process.Pid) {
		t.Fatal("the child is not among this process's descendants")
	}
	if PSSMB(cmd.Process.Pid) <= 0 || TreeMB(os.Getpid()) <= PSSMB(cmd.Process.Pid) || AvailMB() <= 0 {
		t.Fatalf("child %.1f MB, tree %.1f MB, available %.0f MB", PSSMB(cmd.Process.Pid), TreeMB(os.Getpid()), AvailMB())
	}
}

func TestDescendants(t *testing.T) {
	procs := map[int]Proc{2: {PPID: 1}, 3: {PPID: 2}, 4: {PPID: 3}, 5: {PPID: 9}}
	if got := Descendants(procs, 2); len(got) != 2 || got[0] != 3 || got[1] != 4 {
		t.Fatalf("%v", got)
	}
}
