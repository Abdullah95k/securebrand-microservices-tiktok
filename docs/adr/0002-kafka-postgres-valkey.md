# ADR 0002: Kafka for work and events, Postgres for state, Valkey for hot coordination

Status: accepted

## Decision
* **Kafka** carries both the internal work queues (`fbs.task.*`, one topic per
  stage and priority lane) and the public contract (`fb.*`). A topic sits at
  every 1-to-N fan-out; cursor pagination stays inside one task, which yields
  a continuation after `crawler.yield_pages` pages so tasks stay short.
* Crawlers publish entity events directly (no outbox). Every task is
  idempotent: deterministic task ids, `done:` markers, stream leases, content
  hashes, deterministic media ids.
* The consumer processes records of a partition concurrently and commits only
  the highest contiguous completed offset, so a crash redelivers at most the
  in-flight window.
* **Postgres 18** stores the queryable view, built by the persister from the
  public topics with batched COPY + upsert that is a no-op for unchanged
  content. Posts and comments are partitioned monthly by the post's creation
  time (retention = DROP PARTITION; every PK starts with the post so Citus is
  possible later).
* **Valkey** holds what dozens of pods need at sub-millisecond latency: GCRA
  rate limits (per page, global), stream leases, checkpoints and job
  accounting (`pending = |spawned| - |done|` in Lua).

## Consequences
* A job is `completed` only after the persister's committed offsets pass the
  end offsets snapshotted when the job's last task finished, so "completed"
  means "queryable".
* Redis state is short-lived; durable progress (timeline watermarks) lives in
  Postgres, so losing Valkey costs at most re-crawling recent pages.
* Protobuf records carry their type in a header. The schemas are registered
  with a Schema Registry (`fbctl schemas apply`, BACKWARD_TRANSITIVE) for
  documentation and as a deploy-time compatibility gate; the wire format
  stays plain protobuf ([contract](../contracts/kafka-schemas.md)).
