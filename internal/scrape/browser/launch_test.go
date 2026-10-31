package browser

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A Chromium that cannot start is reported at once, with what it said.
func TestLaunchReportsAnEarlyExit(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "chromium")
	script := "#!/bin/sh\necho 'error while loading shared libraries: libnss3.so' >&2\nexit 127\n"
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil { //nolint:gosec // an executable test script
		t.Fatal(err)
	}
	start := time.Now()
	_, err := launch(context.Background(), bin, true, nil)
	if err == nil || !strings.Contains(err.Error(), "libnss3.so") || !strings.Contains(err.Error(), "exit status 127") {
		t.Fatalf("err %v", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("took %s: the exit must not wait for the deadline", d)
	}
}

// A Chromium that exits 0 without listening, and a cancelled launch, are
// told apart from a crash.
func TestLaunchCleanExitAndCancel(t *testing.T) {
	dir := t.TempDir()
	clean := filepath.Join(dir, "clean")
	if err := os.WriteFile(clean, []byte("#!/bin/sh\necho 'Opening in existing browser session.' >&2\n"), 0o700); err != nil { //nolint:gosec // an executable test script
		t.Fatal(err)
	}
	if _, err := launch(context.Background(), clean, true, nil); err == nil ||
		!strings.Contains(err.Error(), "(exit status 0)") || strings.Contains(err.Error(), "%!") {
		t.Fatalf("clean exit: %v", err)
	}
	hang := filepath.Join(dir, "hang")
	if err := os.WriteFile(hang, []byte("#!/bin/sh\necho 'Fontconfig error: cache rebuild' >&2\nexec sleep 60\n"), 0o700); err != nil { //nolint:gosec // an executable test script
		t.Fatal(err)
	}
	// Long enough for the script to print on a loaded runner.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// The caller's budget running out keeps what Chromium said.
	if _, err := launch(ctx, hang, true, nil); !errors.Is(err, context.DeadlineExceeded) ||
		!strings.Contains(err.Error(), "Fontconfig error") || strings.Contains(err.Error(), "%!") {
		t.Fatalf("cancelled launch: %v", err)
	}
}

// A helper that leaves Chromium's process group with its stderr does not
// hold the launch (or stop) once Chromium itself has exited.
func TestLaunchDoesNotWaitForEscapedHelpers(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "chromium")
	script := "#!/bin/sh\nsetsid sleep 10 &\necho 'crashed' >&2\nexit 3\n"
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil { //nolint:gosec // an executable test script
		t.Fatal(err)
	}
	start := time.Now()
	_, err := launch(context.Background(), bin, true, nil)
	if err == nil || !strings.Contains(err.Error(), "exit status 3") {
		t.Fatalf("err %v", err)
	}
	if d := time.Since(start); d > 6*time.Second {
		t.Fatalf("took %s: waited for the helper holding stderr", d)
	}
}

// A start that hangs (no port, no exit) is killed and Chromium started
// again; when every start hangs, the error says so.
func TestLaunchStartsAgainWhenAStartHangs(t *testing.T) {
	old := launchAttempt
	launchAttempt = 300 * time.Millisecond
	t.Cleanup(func() { launchAttempt = old })
	dir := t.TempDir()
	marker := filepath.Join(dir, "started")
	bin := filepath.Join(dir, "chromium")
	script := "#!/bin/sh\nfor a in \"$@\"; do case \"$a\" in --user-data-dir=*) d=\"${a#--user-data-dir=}\";; esac; done\n" +
		"if [ ! -e '" + marker + "' ]; then : > '" + marker + "'; exec sleep 60; fi\n" +
		"printf '9222\\n/devtools/browser/x\\n' > \"$d/DevToolsActivePort\"\nexec sleep 60\n"
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil { //nolint:gosec // an executable test script
		t.Fatal(err)
	}
	p, err := launch(context.Background(), bin, true, nil)
	if err != nil {
		t.Fatalf("second start: %v", err)
	}
	p.stop()
	if p.wsURL != "ws://127.0.0.1:9222/devtools/browser/x" {
		t.Fatalf("ws url %q", p.wsURL)
	}

	hang := filepath.Join(dir, "hang")
	if err := os.WriteFile(hang, []byte("#!/bin/sh\necho 'stuck' >&2\nexec sleep 60\n"), 0o700); err != nil { //nolint:gosec // an executable test script
		t.Fatal(err)
	}
	start := time.Now()
	_, err = launch(context.Background(), hang, true, nil)
	if !errors.Is(err, errHung) || !strings.Contains(err.Error(), "2 starts") || !strings.Contains(err.Error(), "stuck") {
		t.Fatalf("every start hung: %v", err)
	}
	if d := time.Since(start); d < 2*launchAttempt || d > 10*time.Second {
		t.Fatalf("took %s for two hung starts of %s", d, launchAttempt)
	}
}
