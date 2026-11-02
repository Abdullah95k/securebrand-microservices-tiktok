package phase0

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
)

// LadderConfig is the per-IP rate ladder (T20): creator-list checks through
// one exit IP at rising rates, each held for Step, until TikTok pushes
// back.
type LadderConfig struct {
	// Rates are requests per second through one IP, all its identities
	// together. The crawler's identity pace is 0.33; C3 held ~0.3 per IP for
	// an hour with no captcha.
	Rates []float64     `json:"rates"`
	Step  time.Duration `json:"step"`
	// IPs is how many of a tier's exits climb the ladder (at once). Each
	// may be burnt for a while by the top steps, so keep it small.
	IPs int `json:"ips"`
	// Accounts are the accounts the checks rotate over, from the first.
	Accounts int `json:"accounts"`
	// An IP stops climbing after a step whose blocked share reaches
	// StopBlock, or with StopCaptchas captchas. A step whose blocked share
	// stays at or under OKBlock counts as tolerated.
	StopBlock    float64 `json:"stop_block"`
	StopCaptchas int     `json:"stop_captchas"`
	OKBlock      float64 `json:"ok_block"`
	// Identities caps the identities one IP uses at once.
	Identities int `json:"identities"`
	// MaxRequests is the requests an identity serves before it is
	// replaced, as the crawler's (tt.identity_max_requests_*).
	MaxRequests int `json:"max_requests"`
}

func (c *LadderConfig) defaults() {
	if len(c.Rates) == 0 {
		c.Rates = []float64{0.33, 0.66, 1, 2, 4}
	}
	if c.Step <= 0 {
		c.Step = 10 * time.Minute
	}
	if c.IPs <= 0 {
		c.IPs = 2
	}
	if c.Accounts <= 0 {
		c.Accounts = 10
	}
	if c.StopBlock <= 0 {
		c.StopBlock = 0.05
	}
	if c.StopCaptchas <= 0 {
		c.StopCaptchas = 3
	}
	if c.OKBlock <= 0 {
		c.OKBlock = 0.01
	}
	if c.Identities <= 0 {
		c.Identities = 16
	}
	if c.MaxRequests <= 0 {
		c.MaxRequests = 600
	}
}

func (c LadderConfig) maxDuration() time.Duration { return time.Duration(len(c.Rates)) * c.Step }

// LadderIP is one exit IP's identity pool: every identity it makes goes
// out through that IP.
type LadderIP struct {
	Pool *session.Pool
	Tier string // the pool's tier name
}

func (k *Kit) ladder(ctx context.Context) error {
	if k.Stack.LadderPools == nil {
		k.logf("no ladder: no per-IP pools")
		return nil
	}
	lc := k.Cfg.Ladder
	for _, tier := range k.Cfg.Tiers {
		ips, err := k.Stack.LadderPools(tier, lc.IPs, lc.Rates[len(lc.Rates)-1])
		if err != nil {
			return &StepError{Step: StepLadder, Err: err}
		}
		if len(ips) == 0 {
			k.logf("%s: no ladder (a gateway tier has no fixed IP to climb)", tier)
			continue
		}
		f, err := os.OpenFile(k.path(LadderFile(tier)), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		rec := NewRecorder(f, StepLadder, tier)
		k.logf("%s: %d IPs climb %v requests/s, %s a step (at most %s)", tier, len(ips), lc.Rates, lc.Step, lc.maxDuration())
		var wg sync.WaitGroup
		for _, ip := range ips {
			wg.Go(func() { k.climb(ctx, tier, ip, rec) })
		}
		wg.Wait()
		_ = f.Close()
		if err := ctx.Err(); err != nil {
			k.logf("interrupted: rerun with --from ladder to start the ladder again")
			return err
		}
		if err := rec.Err(); err != nil {
			return err
		}
	}
	return nil
}

// target is an account the ladder's checks read.
type target struct {
	acct int
	name string
	sec  string
}

// climb runs the ladder on one IP: each step sends creator-list checks at
// its rate through a few identities on the IP, and records the step.
func (k *Kit) climb(ctx context.Context, tier string, ip LadderIP, rec *Recorder) {
	lc := k.Cfg.Ladder
	w := k.newWork(ip.Tier, rec, Limits{})
	targets, label := k.targets(ctx, w, ip)
	if len(targets) == 0 {
		k.logf("%s: an IP has no account to check (no profile page loaded); skipped", tier)
		return
	}
	var next atomic.Int64
	for _, rate := range lc.Rates {
		if ctx.Err() != nil {
			return
		}
		var calls, blocks, captchas atomic.Int64
		n := min(lc.Identities, int(math.Ceil(rate))+1)
		tokens := make(chan struct{})
		stepCtx, cancel := context.WithTimeout(ctx, lc.Step)
		var wg sync.WaitGroup
		for range n {
			wg.Go(func() {
				var id *session.Identity
				served := 0
				defer func() {
					if id != nil {
						ip.Pool.Release(id)
					}
				}()
				for range tokens {
					if id == nil || served >= lc.MaxRequests || id.RetireReason() != "" {
						if id != nil {
							ip.Pool.Release(id)
							id = nil
						}
						var err error
						if id, err = ip.Pool.Acquire(stepCtx, ip.Tier); err != nil {
							return
						}
						served = 0
						t := targets[int(next.Add(1))%len(targets)]
						if sec, _ := w.page(stepCtx, id, label, t.acct, t.name); sec == "" {
							continue
						}
					}
					t := targets[int(next.Add(1))%len(targets)]
					resp, r := w.call(stepCtx, id, label, t.acct, OpLadder, queryList,
						map[string]any{"sec_uid": t.sec, "count": listPageSize, "cursor": (k.now().Unix() + 1) * 1000}, profileURL(id, t.name))
					served++
					if stepCtx.Err() != nil && resp == nil {
						return
					}
					r.Rate = rate
					if resp != nil && len(resp.Parts) > 0 {
						r.Items = len(resp.Parts[0].Get("itemList").Array())
					}
					rec.Add(r)
					calls.Add(1)
					if blocking(r.Class) {
						blocks.Add(1)
					}
					if r.Class == detect.Checkpoint.String() {
						captchas.Add(1)
					}
				}
			})
		}
		tick := time.NewTicker(time.Duration(float64(time.Second) / rate))
	feed:
		for {
			select {
			case <-stepCtx.Done():
				break feed
			case <-tick.C:
				if captchas.Load() >= int64(lc.StopCaptchas) {
					break feed
				}
				select {
				case tokens <- struct{}{}:
				default: // every identity busy: the step runs below its rate, and the record shows it
				}
			}
		}
		tick.Stop()
		close(tokens)
		wg.Wait()
		cancel()
		st := Record{Op: OpLadderStep, Exit: label, Rate: rate, Calls: int(calls.Load()), Blocks: int(blocks.Load())}
		share := 0.0
		if st.Calls > 0 {
			share = float64(st.Blocks) / float64(st.Calls)
		}
		stop := share >= lc.StopBlock || captchas.Load() >= int64(lc.StopCaptchas)
		if stop {
			st.Stop = "blocked"
		}
		if ctx.Err() == nil {
			rec.Add(st)
			k.logf("%s %s: %.2f requests/s: %d calls, %d blocked (%.1f%%)%s", tier, label, rate, st.Calls, st.Blocks, 100*share, map[bool]string{true: "; stops climbing"}[stop])
		}
		if stop {
			return
		}
	}
}

// targets loads the ladder's accounts' pages through the IP, for their
// secUids (the run's are kept when it ran in this process).
func (k *Kit) targets(ctx context.Context, w *work, ip LadderIP) ([]target, string) {
	n := min(k.Cfg.Ladder.Accounts, len(k.Cfg.Accounts))
	id, err := ip.Pool.Acquire(ctx, ip.Tier)
	if err != nil {
		return nil, ""
	}
	defer ip.Pool.Release(id)
	label := k.exits.Label(id)
	var out []target
	for i := range n {
		acct := k.line(i)
		k.resolvedMu.Lock()
		sec := k.resolved[acct]
		k.resolvedMu.Unlock()
		if sec == "" {
			sec, _ = w.page(ctx, id, label, acct, k.Cfg.Accounts[i])
		}
		if sec != "" {
			out = append(out, target{acct: acct, name: k.Cfg.Accounts[i], sec: sec})
		}
	}
	return out, label
}

// String describes a ladder for logs.
func (c LadderConfig) String() string {
	return fmt.Sprintf("%v requests/s per IP, %s a step, %d IPs", c.Rates, c.Step, c.IPs)
}
