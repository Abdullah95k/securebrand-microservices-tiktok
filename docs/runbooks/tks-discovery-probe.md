# tks discovery probe: what TikTok's discovery pages give a logged-out visitor (checkpoint C6)

The discovery probe answers the questions [`discovery-plan.md`](../tiktok/discovery-plan.md) §8 and §9 leave for C6, before M12 (search, hashtags, sounds, the feed, related posts) and M13 (following and followers lists) are built. It runs on the Phase 0 droplet, through your `isp` tier, with the crawler's own identities, client and browser signer, at the crawler's pace, **for about an hour, under caps**.

| Source | Calls | What it answers |
|---|---|---|
| Keyword search (posts) | `/api/search/general/full/` for 16 keywords of the Iraq list, 5 pages each; the first keyword up to 30 pages | Is a signature from tks's signer pool accepted from tks's exits? How deep does it go before TikTok stops or asks to log in? How Arabic or Kurdish are the results from a US exit? |
| Account search | `/api/search/user/full/`, 6 keywords, 5 pages | Everything (not recorded yet) |
| The For You feed | `/api/recommend/item_list/`, 6 reads | Does each read bring new posts? |
| Hashtags | the tag page, `/api/challenge/detail/`, `/api/challenge/item_list/`: 8 hashtags (half made of keywords, half found in results), 5 pages | Everything |
| Sounds | the music page, `/api/music/detail/`, `/api/music/item_list/`: the 5 sounds found posts use most, 3 pages | Everything |
| Related posts | `/api/related/item_list/` for 6 found posts | Answered unsigned? |
| Following, followers, reposts | `/api/user/list/` (scene 21 and 67), `/api/repost/item_list/`, for 5 of your accounts, 3 pages | **Does a logged-out visitor get these lists at all?** (part D depends on it) |
| Where found authors post from | one creator-list page for 30 found authors not on your list | Search results carry no post region: how many found authors post from Iraq (the exit-country question, §8.3) |
| Posts no longer public (T34) | your accounts' lists compared with the full pass's `posts.csv` (C3); the pages of posts no longer listed, at most 30 | Which `statusCode` a deleted, private or removed post's page gives |
| Short links | the links in `phase0/tt-short-links.txt`, if you add the file | How `vm.tiktok.com` redirects |
| TikTok's own page | in a browser: the search, account search, For You, tag, sound, post and profile pages, 4 scrolls each | The calls TikTok's own page makes there: paths, parameter names, answer sizes (for every call above that is a guess today, T36) |

Each call is tried as seeded and as its signed or unsigned twin on its source's first items, and a signed one is signed both on a profile page (as the crawler signs) and on the call's own page; the first variant that answers is used for the rest.

**At most about 900 requests**, in two lanes of one identity each at the crawler's pace. The probe stops by itself after 5 captchas, so your IPs are not burnt. Nothing logs in (D3), and no signature is made by tks: TikTok's own script signs in the browser page (D11).

## What it writes, and what holds personal data

Everything is under `phase0/out-tt-discover/` (git-ignored, files 0600):

| File | Holds |
|---|---|
| `calls.jsonl` | One line per request: source, call, variant, signed or not, TikTok's answer class and code, how many posts and people, the answer's top-level keys, sizes and times. **No username, name, id, link or text**: keywords, hashtags, sounds and accounts are numbered |
| `pages.json` | The browser pages' own calls: paths, parameter names, the values of parameters that name nothing (counts, cursors, scenes, flags) |
| `report.txt`, `report.json` | The summary below |
| `raw/` | **TikTok's answers as they came**, the first 3 of each call: names, links, captions. The build session turns them into scrubbed test fixtures for M12 (`tksctl fixtures add`), as it did with the probe's |
| **`tks-discover-bundle.tar.gz`** | All of the above: **the file to send back, through your Drive only** (it holds `raw/`) |

The bundle refuses to be written if a proxy password, or one of your usernames outside `raw/`, is in it.

## What the report says

`report.txt` has, per call variant, how many calls were answered and how many held results, then per source: the pages read per item and why each stopped (`end`: TikTok said there was no more; `cap`: the probe's limit; `empty`; `stuck`; a class with TikTok's code, such as `soft_error:2483`, when TikTok stopped answering), the posts and people found, how many are new (not on your list), the posts' languages, the regions found authors post from, the codes post pages gave, the short links' shapes, and the browser pages' own calls. The build session reads the bundle and records the answers.

## Run 2 on a droplet of its own (recommended)

The probe opens Chrome on TikTok's pages, so it does not run on the shared pilot server without the owner's approval (standing instruction 3). A small droplet of its own costs a few cents for the run and is deleted afterwards. The exits are still your `isp` tier, so TikTok sees the same US IPs as before.

**In DigitalOcean (the website):**
1. Create a droplet:
   - Ubuntu 24.04 LTS;
   - Basic, Regular, **4 GB / 2 CPUs**;
   - any region;
   - your usual SSH key.
2. Note its IP: `NEW_DROPLET_IP` below.
3. **If your proxy provider only accepts listed IPs** (an IP allowlist rather than a username and password), add `NEW_DROPLET_IP` there first.

**On your Mac, take the two files from the old droplet** (only a copy: nothing runs there):

```bash
scp root@OLD_DROPLET_IP:securebrand-microservices-tiktok/phase0/proxies.yaml ~/Downloads/tks-proxies.yaml
```

```bash
scp root@OLD_DROPLET_IP:securebrand-microservices-tiktok/phase0/tt-accounts-all.txt ~/Downloads/tt-accounts-all.txt
```

**Log in to the new droplet:**

```bash
ssh root@NEW_DROPLET_IP
```

**Install the tools.** Each command once:

```bash
apt-get update
```

```bash
apt-get install -y git make tmux wget
```

```bash
wget -q https://go.dev/dl/go1.27.4.linux-amd64.tar.gz
```

```bash
tar -C /usr/local -xzf go1.27.4.linux-amd64.tar.gz
```

```bash
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.profile
```

```bash
source ~/.profile
```

```bash
go version
```

It must say `go1.27.4`.

**Get the code** (if git asks for a password, use your GitHub token, as on the old droplet):

```bash
cd ~ && git clone https://github.com/Abdullah95k/securebrand-microservices-tiktok
```

```bash
cd ~/securebrand-microservices-tiktok
```

```bash
mkdir -p phase0
```

**On your Mac, copy the two files to the new droplet:**

```bash
scp ~/Downloads/tks-proxies.yaml root@NEW_DROPLET_IP:securebrand-microservices-tiktok/phase0/proxies.yaml
```

```bash
scp ~/Downloads/tt-accounts-all.txt root@NEW_DROPLET_IP:securebrand-microservices-tiktok/phase0/tt-accounts-all.txt
```

Then remove the proxies copy from your Mac:

```bash
rm ~/Downloads/tks-proxies.yaml
```

**Back on the droplet: protect the files.**

```bash
chmod 600 phase0/proxies.yaml phase0/tt-accounts-all.txt
```

**Install Chrome.**

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

**Rehearse** (about a minute, no traffic to TikTok):

```bash
make tks-discover-dryrun
```

It must end with `discover: done.`

**Run it in tmux,** so a lost connection doesn't stop it:

```bash
tmux new -s tks
```

```bash
make tks-discover TKS_DISCOVER_OUT=phase0/out-tt-discover-2 TKS_DISCOVER_ARGS="--sources feed,hashtag,sound,related,entry,pages"
```

- **How long:** about 30 minutes. It ends by printing where the bundle is.
- **If your connection drops:** log in again with `ssh root@NEW_DROPLET_IP`, then `tmux attach -t tks`.
- **If it stops printing for 10 minutes:** see "If it stops printing" below; the folder there is `phase0/out-tt-discover-2`.

**On your Mac, take the bundle:**

```bash
scp root@NEW_DROPLET_IP:securebrand-microservices-tiktok/phase0/out-tt-discover-2/tks-discover-bundle.tar.gz ~/Downloads/tks-discover-bundle-2.tar.gz
```

**Send it through your Drive only,** and give the link to a tks build session. It holds TikTok's raw answers, with names and captions, so never send it by email or chat attachment.

**Delete the droplet in DigitalOcean** once the bundle is on your Mac. That deletes the proxies file with it.

## Before you start

| | |
|---|---|
| The droplet | The same machine as the probe and Phase 0; tks is in `~/securebrand-microservices-tiktok` |
| Your proxies file | Already at `phase0/proxies.yaml` |
| The accounts file | Already at `phase0/tt-accounts-all.txt` (C3) |
| The full pass's posts | `phase0/out-tt-all/isp/posts.csv` from C3, if it is still there (the T34 comparison uses it; without it that part is skipped) |
| Chrome | Installed for the probe (`make tks-browser-check` says `Chromium found`) |
| Optional | `phase0/tt-gone-posts.txt`: links of posts you know were deleted or made private, one per line. `phase0/tt-short-links.txt`: short links (`https://vm.tiktok.com/...`), one per line. Both made on your Mac and copied with `scp` |

The run is capped like Phase 0: a systemd scope named `tks-discover` (3 GB of memory, no swap, 2 CPU cores). `systemctl stop tks-discover.scope` stops it and every browser in it. Ctrl-C stops it too, and the report and bundle are still written from what was read.

## Rehearse first (about a minute, no traffic)

```bash
make tks-discover-dryrun
```

It runs every source against a built-in fake TikTok and ends with `discover: done.`

## Commands

| Command | What it does |
|---|---|
| `make tks-discover` | The whole probe |
| `make tks-discover TKS_DISCOVER_ARGS="--sources search,feed"` | Some sources only |
| `make tks-discover TKS_DISCOVER_ARGS="--report-only"` | The report and the bundle again from the files |
| `make tks-discover TKS_DISCOVER_OUT=phase0/out-tt-discover-2 TKS_DISCOVER_ARGS="--sources feed,hashtag,sound,related,entry,pages"` | Some sources into a new folder, keeping an earlier run's (C6 run 2 does this) |
| `make tks-discover-dryrun` | The rehearsal |

`go run ./cmd/tks-spike discover --help` lists every flag (each source's cap, the keywords, the signer).

## If it stops printing

Each source prints `start` and `done`; a long silence is normal only while one source walks many pages. To tell a slow run from a stuck one, from a second shell (or after leaving tmux with Ctrl-b, then d):

1. `wc -l phase0/out-tt-discover/calls.jsonl`, then again a minute later: the count grows while it works.
2. `systemctl status tks-discover.scope --no-pager`: memory against the 3 GB cap, and CPU time.

If the count has not moved for 10 minutes, it is stuck. Before stopping it, save where it waits, for the build session: `tmux pipe-pane -o -t tks 'cat >> /root/tks-discover-stuck.txt'`, then `pkill -QUIT -f 'bin/tks-spike discover'` (it prints every goroutine and stops), then `systemctl stop tks-discover.scope` and `make tks-discover TKS_DISCOVER_ARGS="--report-only"` for the report and bundle. Send the bundle through your Drive, with the dump (it holds no names or text).

C6 run 1 (2026-11-05) hung this way on the For You page: a browser call Chromium never answered held the page's close ([`c6-findings-2026-11-05.md`](../tiktok/c6-findings-2026-11-05.md) §3). Fixed: browser calls and page closes are bounded, and so is every probe call (3 minutes).
