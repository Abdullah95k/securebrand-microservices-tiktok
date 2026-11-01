package persist

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"

	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	sharedpersist "github.com/abdullah95k/securebrand-microservices-tiktok/internal/persist"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
)

// Group is tks's persister's consumer group on tt.*.v1 (the copied
// Facebook pipeline's is internal/persist.Group); the job finalizer waits
// for its committed offsets.
const Group = "tks-tt-persister"

// Sink is schema tt's side of internal/persist's consumer.
type Sink struct{ W *Writer }

var _ sharedpersist.Sink = Sink{}

// NewConsumer reads tt.*.v1 into schema tt, setting aside what cannot be
// written to tks.persist.dlq.
func NewConsumer(cfg kafka.ClientConfig, w *Writer, maxBatch int, linger time.Duration) (*sharedpersist.Consumer, error) {
	return sharedpersist.NewSinkConsumer(cfg, Sink{W: w},
		sharedpersist.SinkConfig{Group: Group, Quarantine: topics.PersistQuarantine}, maxBatch, linger)
}

func (Sink) Topics() []string { return topics.Entity }

func (Sink) New(topic string) proto.Message {
	switch topic {
	case topics.Account:
		return &ttv1.Account{}
	case topics.Post:
		return &ttv1.Post{}
	case topics.PostMetrics:
		return &ttv1.PostMetrics{}
	case topics.Comment:
		return &ttv1.Comment{}
	case topics.Music:
		return &ttv1.Music{}
	case topics.Media:
		return &ttv1.Media{}
	}
	return nil
}

func (Sink) Entity(topic string) string {
	switch topic {
	case topics.Account:
		return "account"
	case topics.Post:
		return "post"
	case topics.PostMetrics:
		return "post_metrics"
	case topics.Comment:
		return "comment"
	case topics.Music:
		return "music"
	case topics.Media:
		return "media"
	}
	return "unknown"
}

// Write writes a batch: accounts first, then music and posts (the rows
// comments and metrics refer to), then the rest.
func (s Sink) Write(ctx context.Context, msgs []proto.Message) error {
	var (
		accounts []*ttv1.Account
		posts    []*ttv1.Post
		pms      []*ttv1.PostMetrics
		comments []*ttv1.Comment
		music    []*ttv1.Music
		media    []*ttv1.Media
	)
	for _, m := range msgs {
		switch m := m.(type) {
		case *ttv1.Account:
			accounts = append(accounts, m)
		case *ttv1.Post:
			posts = append(posts, m)
		case *ttv1.PostMetrics:
			pms = append(pms, m)
		case *ttv1.Comment:
			comments = append(comments, m)
		case *ttv1.Music:
			music = append(music, m)
		case *ttv1.Media:
			media = append(media, m)
		}
	}
	if err := s.W.Accounts(ctx, accounts); err != nil {
		return err
	}
	if err := s.W.Music(ctx, music); err != nil {
		return err
	}
	if err := s.W.Posts(ctx, posts); err != nil {
		return err
	}
	if err := s.W.PostMetrics(ctx, pms); err != nil {
		return err
	}
	if err := s.W.Comments(ctx, comments); err != nil {
		return err
	}
	return s.W.Media(ctx, media)
}
