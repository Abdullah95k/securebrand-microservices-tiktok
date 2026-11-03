# tks signers: a lighter browser to sign comment calls

TikTok's comment and reply calls need a signature that only TikTok's own script makes, so tks runs that script in a browser page (ADR 0006 §4). During the Phase 0 run of 2026-11-02/03 that browser was Chrome, and it grew to about 7 GB while its page sat idle on a video page; the pilot server froze. This test tries two lighter browsers against Chrome, on a few of your accounts, under a memory cap, and says which one TikTok answers and what each costs in memory, CPU and proxy data.

| Browser | What it is | On the fake TikTok (this repository's test) |
|---|---|---|
| **Lightpanda** | A headless browser with its own engine and Google's V8 for JavaScript; no images, no graphics, no GPU process; one process | 23 MB, 1 process |
| **chrome-headless-shell** | Google's old headless Chrome: the same engine as Chrome without the parts a screen needs; no GPU process | 175 MB, 7 processes |
| **Chrome** (the control) | The Chrome or Chromium the probe and Phase 0 used | 340 MB, 10 processes |

The real TikTok page is much heavier than the fake one, so expect more for all three; the test measures it.

**Every browser presents itself as itself.** Lightpanda says `Lightpanda/1.0`; both Chromes say `HeadlessChrome` and report `navigator.webdriver = true`. tks sends each signed call with the user agent of the browser that signed it. Nothing is hidden. (Unlike the Phase 0 signer, which ran Chrome without its automation flag and with the identity's Chrome user agent; [`methods.md` §5.5](../tiktok/methods.md).) Lightpanda's usage reports to its makers are turned off, and no browser is given your proxy password on its command line: a small relay inside tks adds it and counts the proxy data.

## What it does

For each browser, one after the other:

1. 5 accounts spread over your accounts file: each one's profile page and newest posts, by plain HTTP as the crawler does.
2. On one post per account, up to 3 pages of comments and 1 page of replies. Each page is signed by TikTok's script in the browser under test; tks sends the signed call.
3. On the first signature and every third after, the page also sends the call itself. This tells "TikTok refused the signature" apart from "TikTok refused tks's request".
4. The page then stays open, unused, for 3 minutes (Chrome grew while idle), and signs once more.
5. Every 2 seconds it records the browser's memory (all its processes), its CPU, and the machine's free memory.

About **20 minutes** in all, about **15 to 50 MB** of proxy data, through the `isp` tier at the crawler's pace.

## What keeps the server safe

1. **A cap from the kernel.** `make tks-signers` runs inside a systemd scope named `tks-signers` limited to **2 GB** of memory, no swap, and **1.5 CPU cores**. Past 2 GB the kernel stops the run, not the machine. (Change them with `TKS_SIGNERS_MEM=1500M` or `TKS_SIGNERS_CPU=100%`.)
2. **A guard in tks.** It stops a browser when its processes pass **1,200 MB**, or when the machine has under **1,500 MB** available, and kills it if it has not closed 10 seconds later. The report then says "stopped early" and why.
3. **One browser and one page at a time.** It refuses to start when the machine has under 1,500 MB available.
4. **Stop it at any time:** Ctrl-C in its window, or from any terminal on the droplet `systemctl stop tks-signers.scope` (it ends the tool and every browser in it, and nothing else).

The two browsers are downloaded into `~/tks-browsers` (about 250 MB). Nothing is installed system-wide. Remove them with `rm -rf ~/tks-browsers`.

## The results

In `phase0/out-tt-signers/`:

- `signers.txt`: a table, a column per browser. Safe to paste in chat (no personal data).
- `signers.json`, `signers-<browser>.jsonl` (each request: its class, time, bytes, signature time) and `signers-<browser>-mem.jsonl` (the memory samples).
- `tks-signers-bundle.tar.gz`: all of the above. It holds no username, id, link, comment text or proxy credential; it refuses to be written if it finds one.

The rows that decide:

| Row | What a good browser shows |
|---|---|
| signed calls answered | 95 % or more, like Chrome |
| page sends answered | the same as the row above; if this is high and the row above is low, TikTok accepts the browser but not tks's request with its signature |
| signed after the idle wait | `ok` |
| memory, peak; memory growth while idle | far under Chrome; flat while idle |
| stopped early | `no` |

If no browser that presents itself as automated is answered, TikTok refuses such browsers; what to do then is a decision for you and counsel ([`methods.md` §5.7](../tiktok/methods.md)).

## Rehearse (no traffic)

```bash
make tks-signers-dryrun
```

It runs all three browsers, under the same cap, against a built-in fake TikTok in under a minute, and prints the same table.

## Prerequisites

- The droplet has run Phase 0: `phase0/out-tt-phase0/registry.yaml` (which call variants TikTok answers today), `phase0/proxies.yaml` and `phase0/tt-accounts-all.txt` are there.
- Chrome is installed (`make tks-browser-check`).
- systemd runs the machine (any Ubuntu droplet).

## The crawler's signer and its memory (S2)

The owner can use no provider and needs comments (2026-11-03), so the crawler keeps the signer that got them answered: Chrome as the identity, its automation hidden (`sign.BrowserOpener`; for counsel, [`methods.md` §5.5](../tiktok/methods.md)). Its memory runaway is TikTok's video page in Chrome's engine. tks now has:

**Safety nets, on by default** (`tt.signer`):
- `idle_close: 2m`: a page unused for 2 minutes is closed; the next call opens a fresh one.
- `max_page_mb: 800`: a page whose Chrome uses more than 800 MB is closed after its call, or by the sweep every 10 seconds while idle.

**Three cures, on by default since S2's result below:**
- `freeze: true`: the page is frozen between its signatures (no script, no timer, no growth) and thawed for each call.
- `lite: true`: Chrome starts without autoplay and without images, so the video player never starts.
- `open_on: profile`: a page opens on the account's profile page instead of the post's video page.

`make tks-signers` now compares, by default, five versions of the crawler's signer, one after the other, under the same cap and guard:

| Engine | What it is |
|---|---|
| `crawler` | as C4 ran it (the control: it should be answered, and grow) |
| `crawler-freeze` | frozen between signatures |
| `crawler-lite` | no autoplay, no images |
| `crawler-profile` | pages open on the profile page |
| `crawler-fixed` | all three cures, and both safety nets |

About **35 minutes** (less when the guard stops a version early). The table's rows are the same; in "says it is", these show "the identity's Chrome". The browsers that present themselves as themselves can still be compared: `TKS_SIGNERS_ARGS="--engines lightpanda,headless-shell,chrome"`.

### S2 result (2026-11-03 15:20 UTC)

Every signed comment call was answered by every version (39 of 39), sent by tks with the page's signature:

| | crawler | crawler-freeze | crawler-lite | crawler-profile | crawler-fixed |
|---|---|---|---|---|---|
| signed calls answered | 3/3 | 11/11 | 3/3 | 11/11 | 11/11 |
| memory, peak | 1,255 MB (stopped) | 961 MB | 1,251 MB (stopped) | 812 MB | 681 MB |
| memory, mean while working | 593 MB | 756 MB | 572 MB | 626 MB | 554 MB |
| CPU, mean (one core) | 93 % | 9 % | 88 % | 12 % | 6 % |
| while idle (3 min) | stopped | 878 to 842 MB | stopped | 667 to 653 MB | closed after 2 min |

So the runaway is TikTok's video page, a frozen page does no work, and autoplay and images are not the cause (lite only saves a third of the page's proxy data). `freeze`, `lite` and `open_on: profile` are now on by default, with the safety nets.

## Result of 2026-11-03

On the pilot droplet (`isp`, 5 accounts per browser, the C4 registry's variants), TikTok refused every comment call from all three browsers: a 200 with an empty answer, whether tks sent the call with the browser's signature or the browser sent it itself (Lightpanda 0 of 7, chrome-headless-shell 0 of 3, Chrome 0 of 3). The Chrome that hid its automation had them answered the night before (C4), as in the probe (C2 runs 1 and 2). Lightpanda used 105 MB at most; both Chromes passed 1.2 GB within about 30 seconds of opening a post page and the guard stopped them, with the machine keeping 4.7 GB or more available. Details in [`build-status.md`](../tiktok/build-status.md) (S1).
