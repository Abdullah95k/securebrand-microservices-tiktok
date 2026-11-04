package phase0_test

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/browser"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/phase0"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/sign"
)

// enginePage is a signer browser that says what it is and can send a call
// itself; bad ones sign nothing.
type enginePage struct {
	bad    bool
	closed *int
}

func (p enginePage) Sign(_ context.Context, raw string) (string, error) {
	if p.bad {
		return raw, nil
	}
	return fakett.Sign(raw)
}

func (p enginePage) Close() { *p.closed++ }
func (enginePage) Agent() sign.Agent {
	return sign.Agent{UserAgent: "Fake/1.0", AppVersion: "1.0", Platform: "Linux x86_64"}
}
func (enginePage) ProxyBytes() int64 { return 1234 }

func (p enginePage) PageFetch(ctx context.Context, raw string) (*browser.Fetched, error) {
	signed, err := p.Sign(ctx, raw)
	if err != nil {
		return nil, err
	}
	resp, err := http.Get(signed) //nolint:gosec,noctx // fakett
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return &browser.Fetched{Status: resp.StatusCode, Body: b}, nil
}

func engineOpener(bad bool, closed *int) sign.Opener {
	return func(context.Context, *session.Identity, string) (sign.Page, error) {
		return enginePage{bad: bad, closed: closed}, nil
	}
}

// The comparison end to end against fakett: a browser whose page signs is
// answered, its page also sends some calls itself and signs again after
// idling; one that signs nothing is refused; one that would take the
// machine's memory is stopped by the guard. The report and the bundle are
// written, without a username.
func TestSignersComparesBrowsers(t *testing.T) {
	k, env, out := kit(t, fakett.Faults{RequireSignature: true, SignedPaths: []string{"/api/comment/list/", "/api/comment/list/reply/"}})
	var goodClosed, badClosed int
	sc := phase0.SignersConfig{
		Engines:  []phase0.SignerEngine{{Name: "good", Opener: engineOpener(false, &goodClosed)}, {Name: "bad", Opener: engineOpener(true, &badClosed)}},
		Accounts: 2, Idle: 300 * time.Millisecond, FetchEvery: 2, SampleEvery: 50 * time.Millisecond,
		Limits: phase0.Limits{BackfillDays: 365, CommentPosts: 1, MaxCommentPages: 2, ReplyThreads: 1, MaxReplyPages: 1},
		Signer: config.Signer{Pages: 1, SignTimeout: 5 * time.Second, PageTimeout: 5 * time.Second},
	}
	rep, err := k.Signers(context.Background(), sc)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Engines) != 2 {
		t.Fatalf("%d engines", len(rep.Engines))
	}
	good, bad := rep.Engines[0], rep.Engines[1]
	if good.Signed == 0 || good.SignedOK != good.Signed || good.PageFetches == 0 || good.PageFetchOK != good.PageFetches {
		t.Fatalf("good: %+v", good)
	}
	if good.AfterIdle != "ok" || good.Agent == nil || good.Agent.UserAgent != "Fake/1.0" || good.PagesOpened != 1 || goodClosed != 1 {
		t.Fatalf("good: after idle %q, agent %+v, %d pages opened, %d closed", good.AfterIdle, good.Agent, good.PagesOpened, goodClosed)
	}
	if good.BrowserProxyBytes != 1234 || good.Accounts != 2 || good.Stopped != "" {
		t.Fatalf("good: %+v", good)
	}
	if bad.Signed == 0 || bad.SignedOK != 0 || bad.AfterIdle == "ok" || bad.PagesOpened < 2 || badClosed != bad.PagesOpened {
		t.Fatalf("bad: %+v", bad)
	}
	txt, err := os.ReadFile(filepath.Join(out, phase0.FileSignersTxt))
	if err != nil || !strings.Contains(string(txt), "Fake/1.0") || !strings.Contains(string(txt), "signed calls answered") {
		t.Fatalf("report: %s %v", txt, err)
	}
	files := untar(t, filepath.Join(out, phase0.FileSignersBundle))
	for _, want := range []string{"signers.json", "signers.txt", "signers-good.jsonl", "signers-good-mem.jsonl", "signers-bad.jsonl"} {
		if _, ok := files["tks-phase0/"+want]; !ok {
			t.Fatalf("bundle lacks %s: %v", want, keys(files))
		}
	}
	for name, b := range files {
		for _, a := range env.World.Accounts() {
			if strings.Contains(string(b), a.Username) || strings.Contains(string(b), a.SecUID) {
				t.Fatalf("%s holds an account", name)
			}
		}
	}

	// The guard: the machine never has this much memory available.
	k2, _, _ := kit(t, fakett.Faults{RequireSignature: true, SignedPaths: []string{"/api/comment/list/"}})
	var closed int
	sc.Engines = []phase0.SignerEngine{{Name: "hungry", Opener: engineOpener(false, &closed)}}
	sc.Guard = phase0.MemGuard{MinHostMB: 1e12}
	sc.Idle = 10 * time.Second
	start := time.Now()
	rep, err = k2.Signers(context.Background(), sc)
	if err != nil {
		t.Fatal(err)
	}
	if h := rep.Engines[0]; !strings.Contains(h.Stopped, "memory guard") || time.Since(start) > 8*time.Second {
		t.Fatalf("hungry: stopped %q after %s", h.Stopped, time.Since(start))
	}
}
