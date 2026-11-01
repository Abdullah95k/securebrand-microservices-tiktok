# tks API: keys, registrations and erasure

`tks-api` serves `/v1` (contract: [`api/openapi/tks.v1.yaml`](../../api/openapi/tks.v1.yaml); design: [ADR 0007 §13](../tiktok/adr/0007-tks-data-model-and-contracts.md#13-settled-in-m6b-the-api-and-erasure)). It reads and writes schema `tt` of the `tks` database; `tks-scheduler` carries out what it queues (scheduled checks, erasures). Deployment (Helm, Compose) comes with M9; until then these steps are for a local or staging stack.

## API keys

Make a key (shown once; store it in the client's secret store):

```
tksctl apikey create --name analysis --scopes data:read,pii:read
```

Scopes: `jobs:write` (on-demand reads), `accounts:write` (registrations), `data:read`, `pii:read` (names, bios, comment text and authors, links that name a username), `admin` (everything, erasure). Give `pii:read` only to clients that need to see who people are.

Revoke a key by its prefix (the part after `tks_`):

```
tksctl apikey revoke --prefix abcd1234
```

## Registering accounts

`POST /v1/accounts` with `{"account": "@username"}` (or a link, `sec:<secUid>`). The account is due at once: the scheduler's planner backfills it (365 days or to its first post), then checks it every 2 h 45 min. Registering again is safe. `GET /v1/accounts` lists the caller's registrations; `DELETE /v1/accounts/{account}` removes the caller's (the schedule is disabled once nobody is registered).

## Erasure (data-subject requests)

Only an admin key can erase. Name the subject one way:

- a person who commented or was mentioned: `{"subject_type": "author", "username": "..."}` (or `user_id`, `sec_uid`, `author_key`);
- a monitored account: `{"subject_type": "account", "username": "..."}` (or `user_id`, `sec_uid`), or `DELETE /v1/accounts/{account}?purge=true`.

```
curl -sS -X POST -H "Authorization: Bearer $TKS_ADMIN_KEY" -H 'Content-Type: application/json' \
  -d '{"subject_type": "author", "username": "someone"}' http://localhost:8080/v1/erasure-requests
```

The answer is `202` with the request's id; `GET /v1/erasure-requests/{id}` shows `pending`, then `done` with what was deleted or redacted per table (within seconds while `tks-scheduler` runs). A `400 unknown author` means tks stores no one by that name: ask for the user id, which works even for someone tks never stored (they are suppressed from then on). A request still `pending` after its first attempt shows its `error` and `attempts`; it is retried for about 16 hours, then `failed`. One that meets stored media while `tt.media.include` is off waits for a scheduler with S3.

What it does is in ADR 0007 §13. In short: an author's comments keep only their ids, their identity and media go, their mentions keep only a pseudonymous key; an account's data is deleted and it is erased as an author too. Both are suppressed: nothing about them is collected again. The request keeps no trace of the name it was given.
