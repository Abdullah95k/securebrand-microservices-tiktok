package browser

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Engine is the browser a page runs in. Chromium is the default; any
// Chromium build starts the same way (Chrome, Chromium, the old headless
// build chrome-headless-shell).
type Engine string

// Engines.
const (
	EngineChromium Engine = ""
	// EngineLightpanda is Lightpanda (lightpanda.io), a headless browser
	// with its own engine and V8, served over the DevTools protocol. It
	// presents itself as Lightpanda (its user agent cannot claim another
	// browser), loads no images, media or fonts, and has no GPU process.
	EngineLightpanda Engine = "lightpanda"
)

// launchLightpanda starts `lightpanda serve` on a free loopback port, with
// the proxy given (which must carry no credentials: Lightpanda takes them
// only on its command line, where any user of the machine could read them;
// a Forwarder adds them instead), and returns the process once it accepts
// DevTools connections.
func launchLightpanda(ctx context.Context, binary string, proxy string) (*process, error) {
	port, err := freePort(ctx)
	if err != nil {
		return nil, err
	}
	args := []string{"serve", "--host", "127.0.0.1", "--port", strconv.Itoa(port), "--log-level", "warn",
		"--http-timeout", "30000",
		// The DevTools client sends this Origin; Lightpanda refuses others.
		"--remote-allow-origins", devtoolsOrigin}
	if proxy != "" {
		args = append(args, "--http-proxy", proxy)
	}
	cmd := exec.CommandContext(ctx, binary, args...) //nolint:gosec // the binary is configuration
	// No usage reports to Lightpanda's makers: they would leave from this
	// machine, outside the identity's exit. And no proxy from the
	// environment (its HTTP library reads it): direct means direct, and an
	// exit is the one given.
	for _, kv := range os.Environ() {
		if k, _, _ := strings.Cut(kv, "="); !strings.HasSuffix(strings.ToLower(k), "_proxy") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, "LIGHTPANDA_DISABLE_TELEMETRY=true")
	stderr := &tail{}
	cmd.Stdout, cmd.Stderr = stderr, stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 2 * time.Second
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("browser: start %s: %w", binary, err)
	}
	p := &process{cmd: cmd, stderr: stderr, done: make(chan struct{}), wsURL: "ws://127.0.0.1:" + strconv.Itoa(port) + "/"}
	go func() {
		p.exitErr = cmd.Wait()
		close(p.done)
	}()
	deadline := time.Now().Add(launchAttempt)
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	d := net.Dialer{Timeout: 200 * time.Millisecond}
	for {
		if c, err := d.DialContext(ctx, "tcp", addr); err == nil {
			_ = c.Close()
			return p, nil
		}
		select {
		case <-p.done:
			p.stop()
			return nil, fmt.Errorf("browser: lightpanda exited before listening: %s", p.stderr)
		case <-ctx.Done():
			p.stop()
			return nil, fmt.Errorf("browser: launch: %w: %s", ctx.Err(), p.stderr)
		default:
		}
		if time.Now().After(deadline) {
			p.stop()
			return nil, fmt.Errorf("browser: lightpanda did not listen within %s: %s", launchAttempt, p.stderr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// freePort asks the kernel for a free loopback port.
func freePort(ctx context.Context) (int, error) {
	var lc net.ListenConfig
	l, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()
	addr, ok := l.Addr().(*net.TCPAddr)
	if !ok {
		return 0, errors.New("browser: no free port")
	}
	return addr.Port, nil
}
