# tks in production: first run, report, stop rules, ramp

For the owner and the master (M18; decision D-026: there is no pilot server, tks's first real run is in production). The platform's side (images, Secrets, roles, topics) is in [`../tiktok/production-handover.md`](../tiktok/production-handover.md). Times are **Iraq time** (UTC+3).

The commands below run `tksctl` inside a tks pod with `kubectl`, by whoever holds access to the cluster (the master, or the owner with access). Each command is one block.

## 1. Before the first sync

1. The platform's Secrets exist in namespace `tks`: `platform-credentials` (with the five roles), `proxies`, `tks-env` (handover §4, §5).
2. The pinned commit's images are built (handover §2).
3. Argo CD syncs `gitops/services/tks.yaml`. The migrate hook runs first: it migrates schema `tt` as the owner and grants the five roles their rights. Its log ends with a JSON line listing `"grants"`, one per role.

If the hook fails with `role "…" does not exist`, a role is missing from the platform: create it, then sync again.

## 2. Check it runs

Every pod ready:

```sh
kubectl -n tks get pods
```

The schema and its migrations:

```sh
kubectl -n tks exec deploy/tks-api -- /usr/local/bin/tksctl migrate status
```

What it should show: every line `Applied`, the last one `00011`. (`migrate status` reads only; any role can run it.)

The canary's first run (within its interval) in its log:

```sh
kubectl -n tks logs deploy/tks-canary --tail=50
```

What it should show: a run with status `ok`. `degraded` or `error`: see [`tks-canary.md`](tks-canary.md) before going on.

## 3. The canary of accounts (C5)

Start with **50 to 100 of the owner's accounts**, paced so their year-long first reads do not all start at once.

1. Make an API key for the owner (once):

   ```sh
   kubectl -n tks exec deploy/tks-api -- /usr/local/bin/tksctl apikey create --name owner --scopes data:read,accounts:write,jobs:write,pii:read
   ```

2. Send the first 50 to 100 accounts through the API (`POST /v1/accounts/import`, [`tks-import.md`](tks-import.md)), or copy the file into the API pod and import it:

   ```sh
   kubectl -n tks cp canary-accounts.txt tks-api-POD:/tmp/canary-accounts.txt
   ```

   ```sh
   kubectl -n tks exec tks-api-POD -- /usr/local/bin/tksctl accounts import /tmp/canary-accounts.txt --client owner --label list:canary
   ```

   (`tks-api-POD`: a pod name from `kubectl -n tks get pods`.) The accounts file never goes into git.

## 4. The report

Every few hours on the first day, then once a day:

```sh
kubectl -n tks exec deploy/tks-api -- /usr/local/bin/tksctl report --hours 24 --prometheus http://PROMETHEUS:9090
```

(`http://PROMETHEUS:9090`: the platform's Prometheus that scrapes namespace `tks`. Without `--prometheus` the report still shows what Postgres holds and the cost.)

A window of your own, in Iraq time:

```sh
kubectl -n tks exec deploy/tks-api -- /usr/local/bin/tksctl report --from "2026-11-08 09:00" --to "2026-11-09 09:00" --prometheus http://PROMETHEUS:9090
```

What it shows:
- **VERDICT: GO or STOP**, and every rule that fired;
- accounts by status;
- requests to TikTok by operation and outcome, blocks, captchas and challenges per 1,000 requests;
- signatures by result, and how many signed calls TikTok answered or refused;
- proxy data (GB) and the signer's browser hours;
- each container's peak memory and restarts;
- new posts stored within 3 hours and 6 hours, and the median delay;
- comment reads by state, collected comments against TikTok's counts, posts stored against the profiles' counts;
- the cost per account and per 1,000 accounts a day at the owner's prices (one IP $3 a month, $0.017 a signer page-hour, $0.0002 compute per account a day, $0.10 per GB-month), and projected to 1,000, 10,000, 100,000 and 300,000 accounts.

`--json` prints the same as JSON; `--check` exits with code 3 when a stop rule fires (for an automated check).

## 5. Stop rules

The report says **STOP** when, over its window:

| Rule | Threshold (flag to change it) |
|---|---|
| Blocked requests (rate limited, login wall, challenge) | over 5 % (`--max-block-rate 0.05`) |
| Captchas and challenges | over 5 per 1,000 requests (`--max-challenges-per-1000 5`) |
| Signed calls TikTok refused | over 20 % (`--max-sign-refused 0.2`) |
| A container killed for memory (OOM) | any |
| A container's restarts | over 3 (`--max-restarts 3`) |
| Cost per account a day | over the budget, $0.0019 (`tt.cost.budget_per_account_usd`) |

It warns (no stop) when fewer than 99 % of new posts were stored within 3 hours, or fewer than 98 % of comment reads completed: those are judged over C5's 7 days.

**On STOP:**

1. Stop tks at once (no task starts and no request goes to TikTok; nothing is lost):

   ```sh
   kubectl -n tks exec deploy/tks-api -- /usr/local/bin/tksctl stop --reason "stop rule: block rate"
   ```

2. Save the report as JSON and send it to the build session:

   ```sh
   kubectl -n tks exec deploy/tks-api -- /usr/local/bin/tksctl report --hours 24 --prometheus http://PROMETHEUS:9090 --json > tks-report.json
   ```

3. When the cause is fixed, start again:

   ```sh
   kubectl -n tks exec deploy/tks-api -- /usr/local/bin/tksctl start
   ```

While stopped, the crawlers hold their tasks in Kafka (consumer lag grows, and KEDA may scale the crawlers to their maximum: they stay idle), the canary skips its runs, and the scheduler keeps planning. A report taken while stopped says so.

## 6. The ramp

After **24 hours with GO**, double the accounts: 100, then 250, 500, 1,000, then the whole list, each step after 24 hours with GO. Keep the pace of new accounts under the budget ([`tks-import.md`](tks-import.md): `TKS_TT__ADMISSION__PER_HOUR`). C5 is met when the build brief's §8 (A1 to A5) holds on the canary for **7 days**.

When a report's cost per 1,000 accounts differs from the estimate (≈ $1.26 a day), the report's figures replace it.

## 7. What tks does not do yet in production

- Media downloads stay off (`tt.media.include: false`) until the service that deletes the files exists ([`tks-media.md`](tks-media.md)).
- Discovery stays off until the owner switches it on ([`tks-discovery.md`](tks-discovery.md)).
- The backup job, egress policy and the rest of the hardening come in M16.
