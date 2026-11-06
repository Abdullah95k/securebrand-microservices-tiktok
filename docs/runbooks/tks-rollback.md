# tks: rolling back a release

For the master and whoever operates tks (M16, H1). Times are **Iraq time** (UTC+3). A release is a commit of this repository pinned in the platform's `gitops/services/tks.yaml`; Argo CD deploys it into namespace `tks`.

## When

Roll back when a new release makes things worse and the cause is not a quick fix: a stop rule of `tksctl report` fires that did not before ([`tks-production.md`](tks-production.md#5-stop-rules)), crash loops, `TksPersisterQuarantine` or `TksSchemaDrift` right after the sync, or the API failing. If TikTok changed (blocks, drift on every release alike), a rollback does not help: see [`tks-canary.md`](tks-canary.md).

## Why the database needs nothing

**Migrations only add** (columns nullable or with defaults, new tables, indexes, functions): they never rename, drop or tighten what released code uses. CI checks it on every change (`schema-compat`: the previous release's end-to-end suite runs on the new schema, `scripts/check-schema-compat.sh`). So the previous commit runs on the newer schema as it is, and **a rollback never runs a down migration**. The migrate hook of the older commit finds the newer migrations applied and changes nothing; the newer columns stay, unused, until the release comes back.

Never run `tksctl migrate down` in production: down migrations drop what newer code wrote, and nothing is deleted at tks (D8).

## Steps

1. **Stop tks** while you roll back (no task starts and no request goes to TikTok; nothing is lost):

   ```sh
   kubectl -n tks exec deploy/tks-api -- /usr/local/bin/tksctl stop --reason "rollback"
   ```

2. Save what the bad release did, for the build session:

   ```sh
   kubectl -n tks exec deploy/tks-api -- /usr/local/bin/tksctl report --hours 6 --prometheus http://PROMETHEUS:9090 --json > tks-report-before-rollback.json
   ```

3. In the platform's GitOps repository, set `gitops/services/tks.yaml` back to the **previous commit's full SHA** for all three images (`image.tag`, `image.browserTag`, `image.backupTag`) and the chart's revision, and merge. The previous commit is in `build-status.md` (Log) or the GitOps history. The platform's images of that commit are still in its registry.
4. Argo CD syncs. The migrate hook runs (it changes nothing), then the Deployments roll back. Check every pod is ready:

   ```sh
   kubectl -n tks get pods
   ```

   ```sh
   kubectl -n tks exec deploy/tks-api -- /usr/local/bin/tksctl migrate status
   ```

   What it should show: every line `Applied`, including the newer release's migrations (they stay).
5. **Start tks** again:

   ```sh
   kubectl -n tks exec deploy/tks-api -- /usr/local/bin/tksctl start
   ```

6. Watch the next report (an hour later) say GO, and send the saved report to the build session with what you saw.

## What is kept and what is redone

- Tasks waiting in Kafka are kept and read by the older code (task messages only gain fields, which older code ignores).
- Records the persister set aside are in `tks.persist.dlq`; replay them once fixed ([`tks-operations.md`](tks-operations.md#replaying-dead-letters)).
- The operation registry (`tt.operations`) is kept, but the older commit's seed writes its own variants again (as at every sync): the canary promotes a working one again within its interval if TikTok changed since.

## When a rollback is not enough

If the newer release wrote rows the older code cannot read (it should not happen: CI checks it), or data was damaged, restore the last good dump into a new database ([`tks-backup.md`](tks-backup.md#restoring)).
