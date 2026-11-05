# tks discovery: the entry check, the dormant list and the Iraq score

How tks decides whether an account a discovery source found is added to monitoring ([`discovery-plan.md`](../tiktok/discovery-plan.md) §3 and §4; [ADR 0007 §19](../tiktok/adr/0007-tks-data-model-and-contracts.md#19-settled-in-m10c-the-entry-check-the-dormant-list-and-the-iraq-score)). The sources themselves come with M11 (mentions, replies, commenters), M12 (search, hashtags, sounds, the For You feed, related posts) and M13 (following and followers lists). Until then nothing finds accounts, and this machinery is idle.

## What happens to a found account

1. **Found.** A source hands tks the account (its user id, secUid and username, the source and what led to it). Its source is recorded in `tt.account_sources` with the evidence.
   - Already monitored: nothing more; the source is evidence.
   - Removed by any client (`DELETE /v1/accounts/{account}`): `blocked`; never read, never added.
   - The source already shows it private, or with no post: straight onto the **dormant list**, with no read.
   - Otherwise it waits for its **entry check** (`pending`).
2. **The entry check.** The scheduler's leader sends waiting accounts to their check, the most promising first, at most `tt.discovery.entry.batch` a tick and `max_running` at once, in the backfill lane (when the budget pauses backfills, entry checks wait too; monitoring never does). The crawler reads the first page of the account's post list (15 newest posts, by plain HTTP, unsigned) and, only when needed, its profile page. Everything read is stored. Nothing else is read.
3. **The decision**, once what was read is stored:

   | The check found | The account |
   |---|---|
   | Private, banned or gone | **Dormant list** (`private`, `banned`, `gone`) |
   | No post at all | **Dormant list** (`never_posted`) |
   | Newest post older than 90 days (`active_for`) | **Dormant list** (`inactive`) |
   | Nothing (errors), 3 times (`max_attempts`) | **Dormant list** (`unreadable`) |
   | Public, active, and the Iraq score passes | `qualified`, then **added** |
   | Public, active, the score does not pass | `not_iraqi` (yet): kept, re-scored with no read when a new signal arrives |

4. **Added.** The leader registers it to the discovery client (`tt.discovery.client`) with origin `discovery`. Its first read, a 365-day backfill, waits in the admission queue behind the owner's accounts, by score ([`tks-import.md`](tks-import.md#the-admission-queue)).
5. **Goes quiet.** A discovered account that turns private, banned or gone, or has no post for 90 days, moves to the dormant list: its checks stop, its data stays. Accounts the owner registered are never moved (they get the lighter checks of M10a).

**The dormant list.** An account on it is read again only when a source finds it again **after** its next check date (its check plus 180 days, `recheck_after`). Found earlier, the finding is counted and nothing is read. Not found again, it is never read again.

## The Iraq score

Each kind of signal counts once. One **strong** signal passes; so do two **different medium** ones; **weak** signals never pass alone or with one medium one.

| Signal | Strength |
|---|---|
| A post made in Iraq (`locationCreated: IQ`) among the posts read | strong |
| Iraq, the Kurdistan Region or an Iraqi place in the bio or nickname (the enabled keywords) | strong |
| An Iraqi phone number in the bio (`+964`, `00964`, `07[3-9]…`) | strong |
| A `.iq` link | strong |
| Iraqi places in captions or hashtags | medium; strong in 3 of the posts read |
| Followed by monitored accounts (their following lists) | medium; strong from 10 |
| Mentioned or replied to by monitored accounts | medium; strong from 10 |
| Shown to viewers in Iraq (search, hashtag, sound, feed or related finds served with region IQ) | medium; strong from 10 |
| Posts in Arabic or Kurdish | weak |
| Comments on, or follows, monitored accounts | weak |

The numbers are `tt.discovery.score` (weights, `pass`, `caption_posts_strong`, `audience_strong`, `regions`, `languages`). During testing, check a sample of each kind with the owner ("is this Iraqi?") and tune them. A keyword turned off (`PATCH /v1/discovery/keywords/{id}`) stops counting within 5 minutes.

## Switches

All in `tt.discovery` (off by default):

```yaml
tt:
  discovery:
    enabled: true
    sources: {mention: true}   # each source as it is built (M11 to M13)
    max_new_per_day: 0         # a cap for testing; 0 = no cap
```

Discovery cannot be turned on with `privacy.pii_mode: pseudonymized`: it needs people's ids.

## Looking at it

The found accounts, by state, with each one's reason, score, evidence and dates (key with `discovery:read`; usernames need `pii:read`):

```bash
curl -s -H "Authorization: Bearer $KEY" "$TKS/v1/discovery/accounts?state=dormant&limit=100"
```

`state=not_iraqi` lists the accounts close to the threshold; `counts` gives every state's total; `next` is the cursor for `&after=`. In SQL: `SELECT * FROM tt.dormant_accounts`.

**Metrics:** `tks_discovery_findings_total{source,state}`, `tks_entry_checks_total{state}`, `tks_discovered_accounts_total`, `tks_dormant_moves_total{reason}`, `tks_discovery_candidates{state}` (leader). Many `entry_checks_total{state="pending"}` (retries) or a growing `discovery_candidates{state="checking"}` mean the checks fail: look at the account stage's errors (entry checks run on its topics). A `pending` count that never falls while backfills are paused is the budget at work (`GET /v1/admission`).
