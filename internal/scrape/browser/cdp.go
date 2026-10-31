package browser

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/net/websocket"
)

// A minimal Chrome DevTools Protocol client: enough to drive one headless
// Chromium per render (navigate, read the DOM, cookies and network
// traffic, answer proxy authentication). One process per render keeps
// identities apart: a fresh profile, its own proxy.

const devtoolsOrigin = "http://127.0.0.1"

// message is a CDP command, response or event.
type message struct {
	ID        int64           `json:"id,omitempty"`
	SessionID string          `json:"sessionId,omitempty"`
	Method    string          `json:"method,omitempty"`
	Params    json.RawMessage `json:"params,omitempty"`
	Result    json.RawMessage `json:"result,omitempty"`
	Error     *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// event is a CDP event of the page session.
type event struct {
	Method string
	Params json.RawMessage
}

// conn is a DevTools websocket connection.
type conn struct {
	ws      *websocket.Conn
	next    atomic.Int64
	mu      sync.Mutex
	pending map[int64]chan message
	events  chan event
	done    chan struct{}
	err     error
}

func dial(ctx context.Context, wsURL string) (*conn, error) {
	cfg, err := websocket.NewConfig(wsURL, devtoolsOrigin)
	if err != nil {
		return nil, err
	}
	ws, err := cfg.DialContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("browser: devtools: %w", err)
	}
	ws.MaxPayloadBytes = 64 << 20
	c := &conn{ws: ws, pending: map[int64]chan message{}, events: make(chan event, 4096), done: make(chan struct{})}
	go c.read()
	return c, nil
}

func (c *conn) read() {
	defer close(c.done)
	for {
		var m message
		if err := websocket.JSON.Receive(c.ws, &m); err != nil {
			c.mu.Lock()
			c.err = err
			for id, ch := range c.pending {
				close(ch)
				delete(c.pending, id)
			}
			c.mu.Unlock()
			return
		}
		if m.ID != 0 {
			c.mu.Lock()
			ch := c.pending[m.ID]
			delete(c.pending, m.ID)
			c.mu.Unlock()
			if ch != nil {
				ch <- m
			}
			continue
		}
		select {
		case c.events <- event{Method: m.Method, Params: m.Params}:
		default:
			// A render that stopped reading events: drop rather than block
			// command responses.
		}
	}
}

// call sends a command and waits for its result.
func (c *conn) call(ctx context.Context, session, method string, params any, out any) error {
	id := c.next.Add(1)
	m := message{ID: id, SessionID: session, Method: method}
	if params != nil {
		b, err := json.Marshal(params)
		if err != nil {
			return err
		}
		m.Params = b
	}
	ch := make(chan message, 1)
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return fmt.Errorf("browser: devtools closed: %w", c.err)
	}
	c.pending[id] = ch
	c.mu.Unlock()
	if err := websocket.JSON.Send(c.ws, m); err != nil {
		return fmt.Errorf("browser: %s: %w", method, err)
	}
	select {
	case r, ok := <-ch:
		if !ok {
			return fmt.Errorf("browser: %s: devtools closed", method)
		}
		if r.Error != nil {
			return fmt.Errorf("browser: %s: %s", method, r.Error.Message)
		}
		if out != nil && len(r.Result) > 0 {
			return json.Unmarshal(r.Result, out)
		}
		return nil
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return ctx.Err()
	}
}

func (c *conn) close() { _ = c.ws.Close() }

// process is a running headless Chromium.
type process struct {
	cmd     *exec.Cmd
	profile string
	wsURL   string
	stderr  *tail
	done    chan struct{} // closed once the process has exited
	exitErr error         // its exit, once done is closed
}

// tail keeps the last bytes written to it: Chromium's stderr, for the
// error when it fails to start.
type tail struct {
	mu  sync.Mutex
	buf []byte
}

const tailSize = 4 << 10

func (t *tail) Write(b []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(t.buf, b...)
	if len(t.buf) > tailSize {
		t.buf = append(t.buf[:0], t.buf[len(t.buf)-tailSize:]...)
	}
	return len(b), nil
}

func (t *tail) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return strings.TrimSpace(string(t.buf))
}

// launchAttempt bounds one start of Chromium, up to its DevTools port. A
// start takes about a second; on a loaded CI runner some hang outright (no
// port and no exit, 45s on), and a fresh one then works. The caller's
// context (the bootstrapper's fb.browser.timeout) still bounds the whole
// render, launch included.
var launchAttempt = 20 * time.Second

// launchAttempts is how many times launch starts Chromium when starts hang.
const launchAttempts = 2

// errHung reports a start that neither opened its DevTools port nor exited
// within launchAttempt.
var errHung = errors.New("browser: chromium did not open its DevTools port")

// launch starts Chromium with a fresh profile and the DevTools endpoint on
// a free port, once more when a start hangs.
func launch(ctx context.Context, binary string, noSandbox bool, args []string) (*process, error) {
	var err error
	for range launchAttempts {
		var p *process
		if p, err = launchOnce(ctx, binary, noSandbox, args); !errors.Is(err, errHung) {
			return p, err
		}
	}
	return nil, fmt.Errorf("%w (%d starts)", err, launchAttempts)
}

func launchOnce(ctx context.Context, binary string, noSandbox bool, args []string) (*process, error) {
	profile, err := os.MkdirTemp("", "igs-browser-")
	if err != nil {
		return nil, err
	}
	flags := []string{
		"--headless=new", "--remote-debugging-port=0", "--user-data-dir=" + profile,
		// The DevTools client sends this Origin; Chromium refuses others.
		"--remote-allow-origins=" + devtoolsOrigin,
		"--no-first-run", "--no-default-browser-check", "--disable-gpu", "--disable-extensions",
		"--disable-background-networking", "--disable-sync", "--disable-dev-shm-usage",
		"--mute-audio", "--hide-scrollbars", "--window-size=1366,900",
		// No traffic of Chromium's own (updates, safe browsing, metrics,
		// field trials): it would go through the identity's paid exit.
		"--disable-component-update", "--disable-domain-reliability", "--disable-client-side-phishing-detection",
		"--disable-default-apps", "--disable-breakpad", "--no-pings", "--metrics-recording-only",
		"--disable-field-trial-config", "--disable-features=OptimizationHints,MediaRouter,Translate,AutofillServerCommunication",
		"--safebrowsing-disable-auto-update",
		// No keyring: on Linux Chromium otherwise asks the desktop's secret
		// service over D-Bus as it starts, which can block where there is
		// none (servers, CI runners). The rest, as Playwright starts it:
		// nothing that waits on a session or a user.
		"--password-store=basic", "--use-mock-keychain", "--no-service-autorun", "--disable-hang-monitor",
		"--disable-component-extensions-with-background-pages", "--disable-search-engine-choice-screen",
	}
	if noSandbox || os.Geteuid() == 0 || os.Getenv("IGS_BROWSER_NO_SANDBOX") != "" {
		flags = append(flags, "--no-sandbox") // Chromium refuses to run as root with its sandbox
	}
	flags = append(flags, args...)
	flags = append(flags, "about:blank")
	cmd := exec.CommandContext(ctx, binary, flags...) //nolint:gosec // the binary is configuration
	stderr := &tail{}
	cmd.Stdout, cmd.Stderr = nil, stderr
	// Its own process group, so stop kills the renderer and zygote
	// children too instead of orphaning them.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	// A helper that leaves the group can keep the stderr pipe open: Wait
	// returns this long after Chromium itself exits, whatever holds it.
	cmd.WaitDelay = 2 * time.Second
	if err := cmd.Start(); err != nil {
		_ = os.RemoveAll(profile)
		return nil, fmt.Errorf("browser: start %s: %w", binary, err)
	}
	p := &process{cmd: cmd, profile: profile, stderr: stderr, done: make(chan struct{})}
	go func() {
		p.exitErr = cmd.Wait()
		close(p.done)
	}()
	// Chromium writes the port and the browser target path once listening.
	deadline := time.Now().Add(launchAttempt)
	for {
		if f, err := os.Open(filepath.Join(profile, "DevToolsActivePort")); err == nil { //nolint:gosec // our own temp profile
			sc := bufio.NewScanner(f)
			var lines []string
			for sc.Scan() {
				lines = append(lines, strings.TrimSpace(sc.Text()))
			}
			_ = f.Close()
			if len(lines) >= 2 && lines[0] != "" && lines[1] != "" {
				p.wsURL = "ws://127.0.0.1:" + lines[0] + lines[1]
				return p, nil
			}
		}
		if err := ctx.Err(); err != nil {
			// The render's budget ran out first: what Chromium said is
			// the only clue to why it was slow.
			p.stop()
			return nil, fmt.Errorf("browser: launch: %w: %s", err, p.stderr)
		}
		select {
		case <-p.done:
			p.stop()
			exit := "exit status 0"
			if p.exitErr != nil {
				exit = p.exitErr.Error()
			}
			return nil, fmt.Errorf("browser: chromium exited before opening its DevTools port (%s): %s", exit, p.stderr)
		default:
		}
		if time.Now().After(deadline) {
			p.stop()
			return nil, fmt.Errorf("%w within %s: %s", errHung, launchAttempt, p.stderr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (p *process) stop() {
	_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
	<-p.done
	_ = os.RemoveAll(p.profile)
}
