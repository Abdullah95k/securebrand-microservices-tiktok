package scheduler

import (
	"context"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"

	taskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/topics"
)

// LagGauges publishes the consumer lag of every fbs consumer group per
// topic (tks_kafka_consumer_lag), so dashboards and alerts do not depend on
// a separate Kafka exporter. Group and topic labels drop the topic prefix.
type LagGauges struct {
	Admin  *kadm.Client
	Prefix string // kafka.topic_prefix

	seen map[[2]string]bool
}

// groupLags reads the lag of the consumer groups whose names start with
// prefix+group.
func groupLags(ctx context.Context, admin *kadm.Client, prefix, group string) (kadm.DescribedGroupLags, error) {
	listed, err := admin.ListGroups(ctx)
	if err != nil {
		return nil, err
	}
	var groups []string
	for _, g := range listed.Groups() {
		if strings.HasPrefix(g, prefix+group) {
			groups = append(groups, g)
		}
	}
	if len(groups) == 0 {
		return nil, nil
	}
	return admin.Lag(ctx, groups...)
}

// TaskBacklog returns how many crawl tasks (page, post and thread) wait in
// each lane's topics, unconsumed or unfinished, keyed by lane as stored in
// jobs.lane. Lanes nothing waits in are absent.
func TaskBacklog(ctx context.Context, admin *kadm.Client, prefix string) (map[int16]int64, error) {
	lags, err := groupLags(ctx, admin, prefix, "tks-crawler-")
	if err != nil {
		return nil, err
	}
	out := map[int16]int64{}
	for _, gl := range lags {
		if err := gl.Error(); err != nil {
			return nil, err
		}
		for _, tl := range gl.Lag.TotalByTopic() {
			if lane, ok := topicLane(strings.TrimPrefix(tl.Topic, prefix)); ok && tl.Lag > 0 {
				out[int16(lane)] += tl.Lag
			}
		}
	}
	return out, nil
}

// topicLane returns the lane of a page, post or thread task topic.
func topicLane(topic string) (taskv1.Lane, bool) {
	rest, ok := strings.CutPrefix(topic, "fbs.task.")
	if !ok {
		return 0, false
	}
	stage, lane, ok := strings.Cut(rest, ".")
	if !ok || (stage != "page" && stage != "post" && stage != "thread") {
		return 0, false
	}
	switch lane {
	case "p0", "p1", "p2":
		return topics.ParseLane(lane), true
	}
	return 0, false
}

// Tick reads the groups' committed offsets and the partitions' end offsets.
func (l *LagGauges) Tick(ctx context.Context) error {
	lags, err := groupLags(ctx, l.Admin, l.Prefix, "tks-")
	if err != nil || len(lags) == 0 {
		return err
	}
	now := map[[2]string]bool{}
	failed := map[string]bool{}
	for _, gl := range lags {
		group := strings.TrimPrefix(gl.Group, l.Prefix)
		if gl.Error() != nil {
			// Keep the group's last values: a transient lookup error must
			// not blank the series and restart alert timers.
			failed[group] = true
			continue
		}
		for _, tl := range gl.Lag.TotalByTopic() {
			k := [2]string{group, strings.TrimPrefix(tl.Topic, l.Prefix)}
			now[k] = true
			metrics.ConsumerLag.WithLabelValues(k[0], k[1]).Set(float64(max(tl.Lag, 0)))
		}
	}
	// Groups and topics that went away stop being reported.
	for k := range l.seen {
		switch {
		case now[k]:
		case failed[k[0]]:
			now[k] = true
		default:
			metrics.ConsumerLag.DeleteLabelValues(k[0], k[1])
		}
	}
	l.seen = now
	return nil
}
