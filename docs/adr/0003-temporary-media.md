# ADR 0003: Media is stored temporarily and deleted on acknowledgement

Status: accepted

## Context
fbcdn URLs are signed and expire (the `oe` parameter, typically days). The
analysis service needs the files, but SecureBrand does not want a long-term
media archive.

## Decision
Download promptly over direct datacenter egress (never through residential
proxies) into S3-compatible storage, announce on `fb.media.ready.v1`, delete
when every required consumer acks on `fb.media.processed.v1`, with a TTL sweep,
bucket lifecycle rule and orphan reconciliation as safety nets. Media already
handed off is not downloaded again on later scrapes (stable media ids).
See `docs/contracts/media-handoff.md`.

## Consequences
* Storage is bounded by throughput × TTL (≈5-12 TB at the target volume with
  a 72h TTL).
* If the analysis service is down longer than the TTL, media is lost for it;
  the deleted events say why (`TTL_EXPIRED`).
