package scheduler

import (
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/jobs"
)

// A job whose lane has tasks waiting (a burst of backfills) is not stalled;
// one whose lane is empty, or past the age cap, is.
func TestStalled(t *testing.T) {
	now := time.Now()
	after := 30 * time.Minute
	quiet := jobs.Status{Spawned: 40, Pending: 25, LastActivity: now.Add(-2 * time.Hour)}
	recent := jobs.Status{Spawned: 40, Pending: 25, LastActivity: now.Add(-5 * time.Minute)}
	for name, c := range map[string]struct {
		st      jobs.Status
		queued  bool
		overdue bool
		want    string
	}{
		"queued in a busy lane":      {quiet, true, false, ""},
		"lane idle: tasks lost":      {quiet, false, false, "partial"},
		"busy lane but past the cap": {quiet, true, true, "partial"},
		"progressing":                {recent, false, false, ""},
		"nothing pending":            {jobs.Status{Spawned: 40, Done: 40, LastActivity: now.Add(-2 * time.Hour)}, false, false, ""},
		"accounting state lost":      {jobs.Status{}, true, false, "failed"},
	} {
		if got, _ := stalled(c.st, c.queued, c.overdue, after, now); got != c.want {
			t.Errorf("%s: %q, want %q", name, got, c.want)
		}
	}
}

func TestTopicLane(t *testing.T) {
	for topic, want := range map[string]int16{
		"fbs.task.page.p0": 1, "fbs.task.post.p1": 2, "fbs.task.thread.p2": 3,
		"fbs.task.media": 0, "fbs.task.retry.30s": 0, "fbs.task.dlq": 0, "fbs.task.page.p9": 0, "fb.post.v1": 0,
	} {
		l, ok := topicLane(topic)
		if ok != (want != 0) || (ok && int16(l) != want) {
			t.Errorf("%s: %v %v, want %d", topic, l, ok, want)
		}
	}
}
