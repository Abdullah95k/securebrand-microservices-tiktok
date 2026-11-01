# tks API: keys and registrations

`tks-api` serves `/v1` (contract: [`api/openapi/tks.v1.yaml`](../../api/openapi/tks.v1.yaml); design: [ADR 0007 §13](../tiktok/adr/0007-tks-data-model-and-contracts.md#13-settled-in-m6b-the-api-and-erasure)). It reads and writes schema `tt` of the `tks` database; `tks-scheduler` runs the scheduled checks it registers. tks deletes nothing it collects and has no erasure (owner, 2026-11-01; [ADR 0007 §14](../tiktok/adr/0007-tks-data-model-and-contracts.md#14-the-owners-decision-nothing-deleted-everything-collected-2026-11-01)). Deployment (Helm, Compose) comes with M9; until then these steps are for a local or staging stack.

## API keys

Make a key (shown once; store it in the client's secret store):

```
tksctl apikey create --name analysis --scopes data:read,pii:read
```

Scopes: `jobs:write` (on-demand reads), `accounts:write` (registrations), `data:read`, `pii:read` (names, bios, comment text and authors, links that name a username), `admin` (everything: all clients' registrations and jobs, the operation registry). Give `pii:read` only to clients that need to see who people are.

Revoke a key by its prefix (the part after `tks_`):

```
tksctl apikey revoke --prefix abcd1234
```

## Registering accounts

`POST /v1/accounts` with `{"account": "@username"}` (or a link, `sec:<secUid>`). The account is due at once: the scheduler's planner backfills it (365 days or to its first post), then checks it every 2 h 45 min. Registering again is safe. `GET /v1/accounts` lists the caller's registrations; `DELETE /v1/accounts/{account}` removes the caller's (the schedule is disabled once nobody is registered; the account's data stays).
