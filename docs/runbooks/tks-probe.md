# tks probe: what logged-out TikTok serves a crawler (checkpoints C2 and C3)

The probe reads a list of public TikTok accounts the way tks will, and answers the questions the build cannot answer without real TikTok traffic ([open-questions.md](../tiktok/open-questions.md)). The main ones:

| # | Question | What the probe does |
|---|---|---|
| T1, T3, T28 | Does a plain page load get the account, a shell or a challenge? | Loads each profile page and records its form |
| T2 | What does a call need: a device id, cookies, msToken? | Repeats a call without each |
| T4 | Which calls work without TikTok's signatures? | Makes every call unsigned first |
| T7, T27 | Does TikTok's own script sign calls made from its page, and can a signed URL be sent from elsewhere? | With the browser signer, repeats the calls inside a headless Chrome page, then replays the signed URLs |
| T8, T9, T10, T11 | Do post ids carry their time? Are list items complete? Pinned posts? How far back does the list go? | Walks each account's post list back in time |
| T12–T15 | Comments and replies served to a crawler? In what order? How complete? | Reads the newest posts' comments and some replies |
| T18, T22, T23, T26, T30 | Media links, cursor portability, country effects, embeds, filtered posts | One check each, per account |

It also collects everything it reads, for you to look at:
- each account's details and avatar;
- every post it pages through, with its counts, music and cover;
- comments and replies;
- one sample video per account in the test pass.

It runs on the Phase 0 droplet, through your ISP proxies, one run per proxy tier (your existing tier and the European one), and paces like a person: one request every 3 seconds or so.

**The output holds personal data** (names, bios, comments, videos). It stays in `phase0/`, which git ignores. Share it privately, and never commit it.

## What you need

| | |
|---|---|
| The droplet | The same machine as the Instagram probe |
| Your proxies file | The one Facebook's and Instagram's probes use, plus the new European ISP tier (step 2) |
| The two accounts files | `tt-accounts.txt` (5 accounts for the test pass) and `tt-accounts-all.txt` (the whole list). The build session sends them to you |

## 1. Get your proxies file onto your Mac

On your Mac, in Terminal:

```bash
scp root@YOUR_DROPLET_IP:securebrand-microservices/phase0/proxies.yaml ~/Downloads/proxies.yaml
```

## 2. Add the European tier, on your Mac

1. Open `~/Downloads/proxies.yaml` with TextEdit (right-click it, **Open With → TextEdit**).
2. Find the line `tiers:`. Under your existing `isp` tier, add this block, keeping the same indentation as the `- name: isp` line. Replace the two example lines with your European proxies, one per line, in the same `http://USER:PASS@IP:PORT` form as your existing ones:

```yaml
      - name: isp_eu
        kind: static_list
        country: de
        cost_per_gb_usd: 0
        endpoints:
          - "http://USER:PASS@IP1:PORT"
          - "http://USER:PASS@IP2:PORT"
```

3. Save (**Cmd-S**). If TextEdit asks, keep it as plain text.

## 3. Put the accounts files in Downloads

Save `tt-accounts.txt` and `tt-accounts-all.txt` (from the build session) in `~/Downloads`.

## 4. Get the code on the droplet

Log in to the droplet:

```bash
ssh root@YOUR_DROPLET_IP
```

Clone tks next to the other two. If git asks for a password, use a GitHub personal access token, as for the others:

```bash
cd ~ && git clone https://github.com/Abdullah95k/securebrand-microservices-tiktok
```

```bash
cd ~/securebrand-microservices-tiktok
```

Later, to get updates: `git pull --ff-only origin main`.

## 5. Copy your three files to the droplet

On your **Mac** (a second Terminal window, not the droplet):

```bash
scp ~/Downloads/proxies.yaml ~/Downloads/tt-accounts.txt ~/Downloads/tt-accounts-all.txt root@YOUR_DROPLET_IP:securebrand-microservices-tiktok/phase0/
```

Back on the **droplet**:

```bash
chmod 600 phase0/proxies.yaml phase0/tt-accounts.txt phase0/tt-accounts-all.txt
```

## 6. Install Chrome for the browser signer (once)

```bash
wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
```

```bash
apt-get install -y ./google-chrome-stable_current_amd64.deb
```

```bash
make tks-browser-check
```

It must say `Chromium found`.

## 7. Rehearse (about a minute, no traffic to TikTok)

```bash
make tks-probe-dryrun
```

Its summary must say `bootstrapped 3/3`.

## 8. The test pass (checkpoint C2)

Start a `tmux` session, so the run continues if your connection drops:

```bash
tmux new -s tks
```

```bash
make tks-probe TKS_PROBE_TIERS=isp,isp_eu
```

It runs the 5 test accounts through each tier in turn, a few minutes per account. It prints a line per account, and every 10 list pages on a long account. To leave it running, press **Ctrl-B, then D**; to come back, `tmux attach -t tks`.

**Ctrl-C stops the run** and keeps what was read; the summary says it stopped early. After 10 captchas the probe stops by itself, so a bad batch of IPs is not burned.

At the end of each tier it prints a summary, for example:

```
bootstrapped 5/5; page forms map[ssr:5]; captchas 0, challenges 0
calls map[creator_list:map[ok:5] post_list:map[registry_stale_empty:5] ...]
posts 2100 (with counts 2100, pinned 4, ads 0, photos 12); id time matches 2100, mismatches 0
list stops map[cap:3 end:2], oldest 2025-11-02; comments served 900 in 15 reads, order map[not_time_ordered:15], replies 60
```

## 9. Send the results back

On your **Mac**:

```bash
scp "root@YOUR_DROPLET_IP:securebrand-microservices-tiktok/phase0/out-tt/*.tar.gz" ~/Downloads/
```

Then put the `.tar.gz` files in your Drive folder and paste the link into the build session. The session records the answers in `docs/tiktok/build-status.md` and the open questions, and keeps the files out of git.

## The full pass (checkpoint C3), when the session asks for it

The whole list, back one year, through the tier the test pass showed works best (the session tells you which). It takes about a day: at one request every 3 seconds, a busy news account reads about 100 list pages plus comments.

```bash
tmux new -s tks-all
```

```bash
make tks-probe-all TKS_PROBE_TIERS=isp_eu
```

Download afterwards on your Mac:

```bash
scp "root@YOUR_DROPLET_IP:securebrand-microservices-tiktok/phase0/out-tt-all/*.tar.gz" ~/Downloads/
```

## What it writes

For each tier, `phase0/out-tt/<tier>/` holds:

| File | Holds |
|---|---|
| `accounts.csv` | One row per account: link, ids, name, bio, bio link, counts, avatar file, how far the list went |
| `posts.csv` | One row per post read: link, kind, time, caption, hashtags, mentions, counts, music, pinned, ad, cover file |
| `comments.csv` | Comments and replies read: post, author, time, likes, reply count, text |
| `images/` | Avatars and covers |
| `videos/` | The sampled videos (test pass only) |
| `raw/` | TikTok's raw answers, to check their shape: the first answer of each call kind, every failed one, and the first pages' HTML |
| `report.json` | Everything above, plus every request's outcome, timing and size, and the answers under `summary` |

and `phase0/out-tt/<tier>.tar.gz` packs the folder for downloading.

## Reference

| Setting | Default | Change with |
|---|---|---|
| Accounts file | `phase0/tt-accounts.txt` (`-all.txt` for the full pass) | `TKS_PROBE_ACCOUNTS=...`, `TKS_PROBE_ACCOUNTS_ALL=...` |
| Tiers | `isp` | `TKS_PROBE_TIERS=isp,isp_eu` (names as in your proxies file; `direct` = the droplet's own IP) |
| Output | `phase0/out-tt/` | `TKS_PROBE_OUT=...` |
| Proxies | `phase0/proxies.yaml` | `PHASE0_CONFIG=...` |
| Anything else | | `TKS_PROBE_ARGS="--max-list-pages 20 --videos 2"` |

All options: `go run ./cmd/tks-spike probe --help`.
