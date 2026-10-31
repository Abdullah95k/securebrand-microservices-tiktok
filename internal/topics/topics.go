// Package topics names every Kafka topic fbs produces or consumes.
package topics

import (
	"fmt"

	taskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fbs/task/v1"
)

const (
	Page          = "fb.page.v1"
	Post          = "fb.post.v1"
	PostMetrics   = "fb.post_metrics.v1"
	Comment       = "fb.comment.v1"
	Media         = "fb.media.v1"
	MediaReady    = "fb.media.ready.v1"
	MediaProcessd = "fb.media.processed.v1"
	MediaDeleted  = "fb.media.deleted.v1"
	Job           = "fb.job.v1"
	Erasure       = "fb.erasure.v1"

	TaskMedia = "fbs.task.media"
	Retry30s  = "fbs.task.retry.30s"
	Retry5m   = "fbs.task.retry.5m"
	Retry1h   = "fbs.task.retry.1h"
	DLQ       = "fbs.task.dlq"
)

// Entity lists the public entity topics the persister consumes; the job
// finalizer waits for the persister to catch up on exactly these.
var Entity = []string{Page, Post, Comment, Media}

// RetryTopics are consumed by the retry relay.
var RetryTopics = []string{Retry30s, Retry5m, Retry1h}

// Task returns the work topic for a task kind and lane.
func Task(kind taskv1.Kind, lane taskv1.Lane) string {
	if kind == taskv1.Kind_KIND_MEDIA {
		return TaskMedia
	}
	var stage string
	switch kind {
	case taskv1.Kind_KIND_PAGE:
		stage = "page"
	case taskv1.Kind_KIND_POST:
		stage = "post"
	case taskv1.Kind_KIND_THREAD:
		stage = "thread"
	default:
		panic(fmt.Sprintf("topics: unknown kind %v", kind))
	}
	return fmt.Sprintf("fbs.task.%s.%s", stage, LaneName(lane))
}

// LaneName returns p0/p1/p2.
func LaneName(l taskv1.Lane) string {
	switch l {
	case taskv1.Lane_LANE_P0:
		return "p0"
	case taskv1.Lane_LANE_P2:
		return "p2"
	default:
		return "p1"
	}
}

// ParseLane is the inverse of LaneName.
func ParseLane(s string) taskv1.Lane {
	switch s {
	case "p0":
		return taskv1.Lane_LANE_P0
	case "p2":
		return taskv1.Lane_LANE_P2
	default:
		return taskv1.Lane_LANE_P1
	}
}
