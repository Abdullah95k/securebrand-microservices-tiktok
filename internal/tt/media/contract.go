package media

import (
	"context"
	"log/slog"
	"strings"

	"github.com/abdullah95k/securebrand-contracts/conformance"

	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/content"
)

// Contract is the shared contract's media hand-off (M15b; ADR 0007 §25):
// every announcement on tt.media.ready.v1 also goes on
// content.media.ready.v1, in the same batch, with the contract's deadline
// and required consumers. tks still deletes nothing (D5): delete_after is
// the deleting service's deadline.
type Contract struct {
	Producer *kafka.Producer
	Handoff  content.Handoff
	// Options are the conformance checks each event passes before it
	// leaves; one that fails is counted and logged, never sent.
	Options conformance.Options
}

// add puts f's announcement number attempt on content.media.ready.v1 in b.
// A file the contract cannot name (no owner) or an announcement it
// refuses (a fifth) is counted and left out: tks's own topic still has it.
func (c *Contract) add(ctx context.Context, b *kafka.Batch, f content.StoredFile, attempt int32) error {
	m := content.MediaReady(f, attempt, c.Handoff)
	if m == nil {
		metrics.ContentEvents.WithLabelValues(content.TopicMediaReady, "invalid").Inc()
		slog.Warn("media: file has no owner the contract can name; not on content.media.ready.v1", "media", f.MediaID)
		return nil
	}
	if err := content.Check(m, c.Options); err != nil {
		metrics.ContentEvents.WithLabelValues(content.TopicMediaReady, "invalid").Inc()
		slog.Warn("media: ready event breaks the contract, not sent", "media", f.MediaID, "err", err)
		return nil
	}
	rec, err := content.Record(c.Producer, content.TopicMediaReady, m.MediaId, m)
	if err != nil {
		return err
	}
	b.Add(ctx, rec)
	metrics.ContentEvents.WithLabelValues(content.TopicMediaReady, "sent").Inc()
	return nil
}

// ttKind is the media kind of schema tt's kind name (Kind's inverse).
func ttKind(kind string) ttv1.MediaKind {
	return ttv1.MediaKind(ttv1.MediaKind_value["MEDIA_KIND_"+strings.ToUpper(kind)])
}
