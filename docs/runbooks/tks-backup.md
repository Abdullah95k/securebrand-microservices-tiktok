# tks database backups

For the master and whoever operates tks (M16, H2). Times are **Iraq time** (UTC+3). Synthetic examples only.

## What runs

The CronJob `tks-backup` runs **every day at 02:17 Iraq time** (`backup.schedule`, `backup.timeZone: Asia/Baghdad`). It runs `tksctl backup run` in the backup image (`deploy/docker/Dockerfile --target backup`: `tksctl` with PostgreSQL 18's `pg_dump`), logged in as the **read-only role** (`tks_reader`).

- It dumps **schema `tt`** in `pg_dump`'s custom format (compressed), without owners or rights.
- The dump is **streamed straight into the backup bucket** (`S3_BUCKET_BACKUPS` in `platform-credentials`), never written to the pod's disk: the database only grows, since nothing is deleted (D8).
- Keys: `tks/postgres/YYYY/MM/DD/tks-<UTC time>.dump`, and next to each a `.sha256` file with its checksum, which a restore checks.
- **A dump that fails leaves no object**: the upload is cancelled when `pg_dump` fails.
- **tks never deletes a dump.** How long they are kept is the bucket's rule (the platform's). A month of dailies is a sensible start.
- One job at a time (`concurrencyPolicy: Forbid`); a job is stopped after 6 hours (`backup.activeDeadlineSeconds`). It holds one upload part in memory (`backup.partMB`, 64 MB); a dump can be at most 10,000 parts (640 GB at 64 MB): raise `partMB` (and the job's memory limit) before the database nears that.

The restore is tested in CI on every change (`internal/tt/backup`): a dump taken by a read-only role is restored into a new database, every table's rows compared, and a damaged dump and a failed `pg_dump` are caught.

## Commands

Each command is one block. They run in a tks pod; the backup image is the only one with `pg_dump` and `pg_restore`, so `run` and `restore` run in a backup job (below), while `list` runs anywhere.

The dumps in the bucket, oldest first (with each time in Iraq time):

```sh
kubectl -n tks exec deploy/tks-api -- /usr/local/bin/tksctl backup list
```

A backup now, outside the schedule (a job from the CronJob):

```sh
kubectl -n tks create job --from=cronjob/tks-backup tks-backup-now
```

Its result (the last line is the dump: key, bytes, checksum):

```sh
kubectl -n tks logs job/tks-backup-now
```

## A backup failed

`TksBackupFailed` (a backup job failed in the last day) or `TksBackupMissing` (no backup succeeded for 26 hours). Nothing is lost: the last good dump is still in the bucket.

1. Read the failed job's log:

   ```sh
   kubectl -n tks get jobs -l app.kubernetes.io/component=backup
   ```

   ```sh
   kubectl -n tks logs job/JOB-NAME
   ```

2. By what it says:
   - `no bucket`: `S3_BUCKET_BACKUPS` is missing from `platform-credentials` (the platform's).
   - `pg_dump is version …, the server …`: the backup image is older than the server; rebuild it from the pinned commit (its base is `postgres:18-alpine`).
   - `permission denied for table …`: the read-only role lacks a right; run `tksctl migrate up` (the migrate hook does, on every sync), which grants `tks_reader` every table.
   - `DeadlineExceeded` (the job ran 6 hours): the database outgrew the window; raise `backup.activeDeadlineSeconds`.
   - The object store refused the upload: its credentials or the bucket (the platform's).
3. When fixed, take one now (above) and check `backup list` shows it.

## Restoring

A restore goes into a **new, empty database** on the same server, never over the live one (`tksctl` refuses a database that already has schema `tt`). It runs in one transaction: a restore that fails leaves nothing.

1. **Stop tks** so nothing is written meanwhile:

   ```sh
   kubectl -n tks exec deploy/tks-api -- /usr/local/bin/tksctl stop --reason "restore"
   ```

2. The platform creates the new database (for example `tks_restored`), owned by tks's owner role (`POSTGRES_USER`).
3. Restore the newest dump into it, as the owner, in a pod of the backup image (a job made from the CronJob, its command changed):

   ```sh
   kubectl -n tks create job --from=cronjob/tks-backup tks-restore --dry-run=client -o yaml > tks-restore.yaml
   ```

   In `tks-restore.yaml`, set the container's `args` to `["backup", "restore", "--target-database", "tks_restored", "--key", "latest"]` (or a key from `backup list`), and its Postgres user and password to the owner's (`POSTGRES_USER`, `POSTGRES_PASSWORD`), then:

   ```sh
   kubectl -n tks apply -f tks-restore.yaml
   ```

   ```sh
   kubectl -n tks logs -f job/tks-restore
   ```

   What it should show: a last line with the dump's key and `sha256`. `checksum …, want …`: the dump is damaged; restore the one before it.
4. Point tks at the new database (`POSTGRES_DATABASE` in `platform-credentials`) and sync: the migrate hook grants the five roles their rights on it.
5. Check it, then start tks:

   ```sh
   kubectl -n tks exec deploy/tks-api -- /usr/local/bin/tksctl migrate status
   ```

   ```sh
   kubectl -n tks exec deploy/tks-api -- /usr/local/bin/tksctl start
   ```

What a restore does not bring back: what was written after the dump. tks reads it again: the next checks read every account's newest posts, and comments are read again where counts grew. Posts that left TikTok in between are lost to tks.
