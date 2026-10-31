# Kafka record schemas (fb.v1)

fbs publishes its results on the public topics below. Topic names, and the
Schema Registry subjects, get `kafka.topic_prefix` prepended.

| Topic | Key | Value message | Proto file |
|---|---|---|---|
| `fb.page.v1` | page id | `securebrand.fb.v1.Page` | `page.proto` |
| `fb.post.v1` | post id | `securebrand.fb.v1.Post` | `post.proto` |
| `fb.post_metrics.v1` | post id | `securebrand.fb.v1.PostMetrics` | `post.proto` |
| `fb.comment.v1` | post id (a post's comments stay in order) | `securebrand.fb.v1.Comment` | `comment.proto` |
| `fb.media.v1` | media id | `securebrand.fb.v1.Media` | `media.proto` |
| `fb.media.ready.v1` | media id | `securebrand.fb.v1.MediaReady` | `media.proto` |
| `fb.media.processed.v1` | media id | `securebrand.fb.v1.MediaProcessed` (the analysis service produces it) | `media.proto` |
| `fb.media.deleted.v1` | media id | `securebrand.fb.v1.MediaDeleted` | `media.proto` |
| `fb.job.v1` | job id | `securebrand.fb.v1.JobEvent` | `job.proto` |
| `fb.erasure.v1` | erasure request id | `securebrand.fb.v1.ErasureEvent` | `erasure.proto` |

The files are in `api/proto/securebrand/fb/v1/`. Generated Go is in `gen/go/securebrand/fb/v1`.

## Comment versions and deletions

`fb.comment.v1` carries every observation of a comment, so a consumer sees each version:

- **Edits:** a later record of the same `comment_id` with other `text` is an edit. The store counts them (`edit_count`, `edited_at` in the API).
- **Deletions:** when a full read of a post (a recrawl, or a full job on a post crawled before) no longer returns a known top-level comment, the job finalizer publishes a record with `status: COMMENT_STATUS_DELETED`. Only `comment_id`, `post_id`, `page_id`, `post_created_at`, `depth` and `obs` are set; keep the content you already have. "Deleted" means no longer publicly visible: deleted by its author, removed or hidden by the page, or filtered by Facebook. Replies are not checked.
- **Re-sightings:** a later ordinary record of a deleted comment (status unspecified) means it is visible again, and so are the replies deleted with it.
- **Event ids:** `obs.event_id` is derived from the content, so unchanged re-scrapes repeat it. A deletion record's is unique to its job. Deduplicate by `event_id` only among records of the same status: a re-sighting can repeat the `event_id` of the record before the deletion.

Crawled records leave `status` unspecified, which means visible.

## Wire format

- A value is the plain protobuf encoding of the message. There is no Schema Registry framing: no magic byte, schema id or message indexes.
- The `igs-type` header holds the full message name, for example `securebrand.fb.v1.Comment`.
- `content-type` is `application/x-protobuf`.

A consumer decodes by `igs-type`: with the generated code, or with a descriptor built from the registered schema.

## Schema Registry

`fbctl schemas apply` registers the contract with a Confluent-compatible registry (`kafka.schema_registry.url`). Redpanda's registry, on port 8081, works.

- **Subjects:**
  - each topic's subject `<topic>-value` holds the whole proto file that declares its value message. For example, `fb.post.v1-value` and `fb.post_metrics.v1-value` both hold `post.proto`. The table above, and each record's `igs-type` header, say which message a topic carries. Confluent-framed tooling (Connect converters, ksqlDB) cannot decode these records directly: it expects the magic byte and schema id, and by default takes the file's first message;
  - `securebrand/fb/v1/common.proto`, which the other files import, has a subject of its own, and the topic subjects reference it;
  - `google/protobuf/*` imports are the registry's built-in well-known types.
- **Check first, then register:** apply first checks every changed schema against its subject's latest version, and registers nothing unless all pass, so a failed upgrade leaves the registry as it was.
  - A changed import is staged under a temporary subject (`igs-check-…`) during the check, so the files importing it are checked against its new content. The temporary subjects are deleted afterwards.
  - On registration the registry enforces the subject's level against every earlier version.
- **Compatibility:** a subject without a level of its own is set to `BACKWARD_TRANSITIVE`: a consumer built against any earlier version can still read new data, so it can upgrade at its own pace. A level an operator set on a subject (for a planned migration, say) is kept.
- **Idempotent:** an unchanged contract registers nothing new.
- **`fbctl schemas check`** runs only the check. It writes nothing except the temporary subjects.

The registry does not change what is on the wire. It documents the schemas for consumers and refuses breaking changes at deploy time. The Helm hook (`hooks.schemas: true`) runs `fbctl schemas apply` before each upgrade, so an incompatible contract change fails the upgrade before any pod produces it. Docker Compose runs it as the `schemas` service.

## Changing the contract

1. Only add fields, with new field numbers. Never change a field's type or number, and never reuse a removed number: mark it `reserved`.
2. CI runs `buf breaking` against the base branch (`FILE` rules), so a breaking change fails the pull request.
3. On deploy, `fbctl schemas apply` registers the new version. The registry refuses it if it is not backward compatible with every earlier version.
4. A real break needs a new topic and package (`fb.comment.v2`, `securebrand.fb.v2`), produced alongside v1 until consumers move.
