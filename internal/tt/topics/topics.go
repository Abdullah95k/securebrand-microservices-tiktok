// Package topics names every Kafka topic tks produces or consumes (docs/tiktok
// ADR 0007 §3), and the proto file of each public topic's value. The topics
// themselves, with their partitions and retention, are in
// configs/tt/topics.yaml; a test keeps the two in step.
package topics

import (
	"fmt"

	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
)

// Public topics (securebrand.tt.v1 and securebrand.common.v1).
const (
	Account        = "tt.account.v1"
	Post           = "tt.post.v1"
	PostMetrics    = "tt.post_metrics.v1"
	Comment        = "tt.comment.v1"
	Music          = "tt.music.v1"
	Media          = "tt.media.v1"
	MediaReady     = "tt.media.ready.v1"
	MediaProcessed = "tt.media.processed.v1"
	MediaDeleted   = "tt.media.deleted.v1"
	Job            = "tt.job.v1"
	Erasure        = "tt.erasure.v1"
)

// Internal topics (securebrand.tks.task.v1.Task, and the persister's
// quarantine).
const (
	TaskMedia = "tks.task.media"
	Retry30s  = "tks.task.retry.30s"
	Retry5m   = "tks.task.retry.5m"
	Retry1h   = "tks.task.retry.1h"
	DLQ       = "tks.task.dlq"
	// PersistQuarantine holds the records the persister set aside: those
	// that do not decode or that Postgres refuses.
	PersistQuarantine = "tks.persist.dlq"
)

// TypeHeader is the record header naming the value's full message name
// (ADR 0006 §5).
const TypeHeader = "tks-type"

// ValueSchemas maps each public topic to the proto file (import path in
// api/proto) declaring its value message. Records carry plain protobuf and
// the message name in the tks-type header; the Schema Registry subjects
// (<topic>-value) document and gate the contract without changing the wire
// format (`tksctl schemas apply`).
var ValueSchemas = map[string]string{
	Account:        "securebrand/tt/v1/account.proto",
	Post:           "securebrand/tt/v1/post.proto",
	PostMetrics:    "securebrand/tt/v1/post.proto",
	Comment:        "securebrand/tt/v1/comment.proto",
	Music:          "securebrand/tt/v1/music.proto",
	Media:          "securebrand/tt/v1/media.proto",
	MediaReady:     "securebrand/common/v1/media.proto",
	MediaProcessed: "securebrand/common/v1/media.proto",
	MediaDeleted:   "securebrand/common/v1/media.proto",
	Job:            "securebrand/common/v1/job.proto",
	Erasure:        "securebrand/common/v1/erasure.proto",
}

// ValueTypes maps each public topic to the full name of its value message,
// the tks-type header's value.
var ValueTypes = map[string]string{
	Account:        "securebrand.tt.v1.Account",
	Post:           "securebrand.tt.v1.Post",
	PostMetrics:    "securebrand.tt.v1.PostMetrics",
	Comment:        "securebrand.tt.v1.Comment",
	Music:          "securebrand.tt.v1.Music",
	Media:          "securebrand.tt.v1.Media",
	MediaReady:     "securebrand.common.v1.MediaReady",
	MediaProcessed: "securebrand.common.v1.MediaProcessed",
	MediaDeleted:   "securebrand.common.v1.MediaDeleted",
	Job:            "securebrand.common.v1.JobEvent",
	Erasure:        "securebrand.common.v1.ErasureEvent",
}

// Entity lists the public entity topics the persister consumes; the job
// finalizer waits for the persister to catch up on exactly these.
var Entity = []string{Account, Post, PostMetrics, Comment, Music, Media}

// RetryTopics are consumed by the retry relay.
var RetryTopics = []string{Retry30s, Retry5m, Retry1h}

// Task returns the work topic for a task kind and lane.
func Task(kind tkstaskv1.Kind, lane tkstaskv1.Lane) string {
	var stage string
	switch kind {
	case tkstaskv1.Kind_KIND_MEDIA:
		return TaskMedia
	case tkstaskv1.Kind_KIND_ACCOUNT:
		stage = "account"
	case tkstaskv1.Kind_KIND_POST:
		stage = "post"
	case tkstaskv1.Kind_KIND_THREAD:
		stage = "thread"
	default:
		panic(fmt.Sprintf("topics: unknown kind %v", kind))
	}
	return fmt.Sprintf("tks.task.%s.%s", stage, LaneName(lane))
}

// LaneName returns p0, p1 or p2.
func LaneName(l tkstaskv1.Lane) string {
	switch l {
	case tkstaskv1.Lane_LANE_P0:
		return "p0"
	case tkstaskv1.Lane_LANE_P2:
		return "p2"
	default:
		return "p1"
	}
}

// ParseLane is the inverse of LaneName.
func ParseLane(s string) tkstaskv1.Lane {
	switch s {
	case "p0":
		return tkstaskv1.Lane_LANE_P0
	case "p2":
		return tkstaskv1.Lane_LANE_P2
	default:
		return tkstaskv1.Lane_LANE_P1
	}
}
