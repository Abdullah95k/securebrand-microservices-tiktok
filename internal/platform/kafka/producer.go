// Package kafka wraps franz-go with the producer and consumer semantics the
// fbs services rely on: idempotent zstd producing of protobuf records, and a
// consumer that processes records of one partition concurrently while only
// committing the highest contiguous completed offset.
package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"google.golang.org/protobuf/proto"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
)

// Header names used on every record.
const (
	HeaderType        = "igs-type"
	HeaderContentType = "content-type"
	HeaderOrigTopic   = "x-orig-topic"
	HeaderNotBefore   = "x-not-before"
	HeaderReason      = "x-reason"
	HeaderError       = "x-error"
	HeaderTraceParent = "traceparent"
	ContentTypeProto  = "application/x-protobuf"
)

// ClientConfig is shared by producers and consumers.
type ClientConfig struct {
	Brokers     []string
	ClientID    string
	TopicPrefix string
	Linger      time.Duration
	// TLS, when set, encrypts the broker connections; SASL authenticates.
	TLS  *tls.Config
	SASL sasl.Mechanism
}

// Opts are the client options every client of this config needs: brokers,
// client id, TLS and SASL.
func (c ClientConfig) Opts() []kgo.Opt {
	opts := []kgo.Opt{kgo.SeedBrokers(c.Brokers...), kgo.ClientID(c.ClientID)}
	if c.TLS != nil {
		opts = append(opts, kgo.DialTLSConfig(c.TLS.Clone()))
	}
	if c.SASL != nil {
		opts = append(opts, kgo.SASL(c.SASL))
	}
	return opts
}

// Producer produces protobuf records.
type Producer struct {
	cl     *kgo.Client
	prefix string
}

func NewProducer(cfg ClientConfig, extra ...kgo.Opt) (*Producer, error) {
	opts := append(cfg.Opts(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.ZstdCompression(), kgo.Lz4Compression(), kgo.NoCompression()),
		kgo.ProducerLinger(cfg.Linger),
		kgo.ProducerBatchMaxBytes(1<<20),
		kgo.AllowAutoTopicCreation(),
	)
	opts = append(opts, extra...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: producer: %w", err)
	}
	return &Producer{cl: cl, prefix: cfg.TopicPrefix}, nil
}

// Client exposes the underlying client (admin calls, tests).
func (p *Producer) Client() *kgo.Client { return p.cl }

// Topic applies the configured prefix.
func (p *Producer) Topic(base string) string { return p.prefix + base }

// Ping verifies broker connectivity.
func (p *Producer) Ping(ctx context.Context) error { return p.cl.Ping(ctx) }

func (p *Producer) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = p.cl.Flush(ctx)
	p.cl.Close()
}

// Record builds a record for base topic (prefix applied) with a protobuf value.
func (p *Producer) Record(base, key string, msg proto.Message, headers ...kgo.RecordHeader) (*kgo.Record, error) {
	b, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("kafka: marshal %T: %w", msg, err)
	}
	h := make([]kgo.RecordHeader, 0, len(headers)+2)
	h = append(h,
		kgo.RecordHeader{Key: HeaderType, Value: []byte(proto.MessageName(msg))},
		kgo.RecordHeader{Key: HeaderContentType, Value: []byte(ContentTypeProto)},
	)
	h = append(h, headers...)
	return &kgo.Record{Topic: p.Topic(base), Key: []byte(key), Value: b, Headers: h}, nil
}

// ProduceSync produces records and waits for all acks.
func (p *Producer) ProduceSync(ctx context.Context, recs ...*kgo.Record) error {
	if len(recs) == 0 {
		return nil
	}
	res := p.cl.ProduceSync(ctx, recs...)
	for _, r := range res {
		outcome := "ok"
		if r.Err != nil {
			outcome = "error"
		}
		metrics.KafkaProduce.WithLabelValues(r.Record.Topic, outcome).Inc()
	}
	return res.FirstErr()
}

// Batch collects asynchronous produces so a caller can wait for all of them
// at a checkpoint boundary (for example after one page of results).
type Batch struct {
	p    *Producer
	wg   sync.WaitGroup
	mu   sync.Mutex
	errs []error
}

func (p *Producer) NewBatch() *Batch { return &Batch{p: p} }

// Add produces rec asynchronously.
func (b *Batch) Add(ctx context.Context, rec *kgo.Record) {
	b.wg.Add(1)
	b.p.cl.Produce(ctx, rec, func(r *kgo.Record, err error) {
		outcome := "ok"
		if err != nil {
			outcome = "error"
			b.mu.Lock()
			b.errs = append(b.errs, fmt.Errorf("produce %s: %w", r.Topic, err))
			b.mu.Unlock()
		}
		metrics.KafkaProduce.WithLabelValues(r.Topic, outcome).Inc()
		b.wg.Done()
	})
}

// AddProto builds and produces a protobuf record.
func (b *Batch) AddProto(ctx context.Context, base, key string, msg proto.Message, headers ...kgo.RecordHeader) error {
	rec, err := b.p.Record(base, key, msg, headers...)
	if err != nil {
		return err
	}
	b.Add(ctx, rec)
	return nil
}

// Wait blocks until every added record is acknowledged and returns the
// combined error. A Batch can be reused after Wait.
func (b *Batch) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() { b.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	err := errors.Join(b.errs...)
	b.errs = nil
	return err
}

// HeaderValue returns the value of header key, or "".
func HeaderValue(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
