package scheduler

import (
	"context"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
)

// LagGauges publishes the consumer lag of every fbs consumer group per
// topic (igs_kafka_consumer_lag), so dashboards and alerts do not depend on
// a separate Kafka exporter. Group and topic labels drop the topic prefix.
type LagGauges struct {
	Admin  *kadm.Client
	Prefix string // kafka.topic_prefix

	seen map[[2]string]bool
}

// Tick reads the groups' committed offsets and the partitions' end offsets.
func (l *LagGauges) Tick(ctx context.Context) error {
	listed, err := l.Admin.ListGroups(ctx)
	if err != nil {
		return err
	}
	var groups []string
	for _, g := range listed.Groups() {
		if strings.HasPrefix(g, l.Prefix+"igs-") {
			groups = append(groups, g)
		}
	}
	if len(groups) == 0 {
		return nil
	}
	lags, err := l.Admin.Lag(ctx, groups...)
	if err != nil {
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
