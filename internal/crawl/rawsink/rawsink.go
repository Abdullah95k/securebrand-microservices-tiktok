// Package rawsink batches raw TikTok responses into zstd-compressed JSONL
// objects in S3 so payloads can be re-parsed after a parser fix
// or a question about one. Failures and a sample of the other answers go
// under KeepPrefix (kept for good), the other answers, when kept at all,
// under RestPrefix (the production platform expires tks/30d/ after 30
// days; M18).
package rawsink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/minio/minio-go/v7"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
)

// Sink implements graphql.RawSink.
type Sink struct {
	s3      *minio.Client
	bucket  string
	pod     string
	maxSize int

	// KeepPrefix and RestPrefix start the objects' keys ("raw-failures/"
	// and "raw/" by default). Sample is the share of good answers kept
	// under KeepPrefix; the others go under RestPrefix when KeepRest is set
	// and are dropped otherwise.
	KeepPrefix, RestPrefix string
	Sample                 float64
	KeepRest               bool

	mu   sync.Mutex
	bufs map[string]*bytes.Buffer // prefix/query -> JSONL
	seq  int
}

func New(s3 *minio.Client, bucket string) *Sink {
	pod, _ := os.Hostname()
	return &Sink{s3: s3, bucket: bucket, pod: pod, maxSize: 8 << 20, bufs: map[string]*bytes.Buffer{},
		KeepPrefix: "raw-failures/", RestPrefix: "raw/", KeepRest: true}
}

// Line is one captured response (one JSONL line of a raw object).
type Line struct {
	At    time.Time `json:"at"`
	Query string    `json:"query"`
	Class string    `json:"class"`
	// Sample marks a good answer kept as part of the sample.
	Sample bool            `json:"sample,omitempty"`
	Vars   map[string]any  `json:"vars,omitempty"`
	Body   json.RawMessage `json:"body,omitempty"`
	Text   string          `json:"text,omitempty"`
}

// Capture buffers one response.
func (s *Sink) Capture(ctx context.Context, query string, cls detect.Class, body []byte, vars map[string]any) {
	prefix, sample := s.KeepPrefix, false
	if cls == detect.OK {
		switch {
		case s.Sample > 0 && rand.Float64() < s.Sample:
			sample = true
		case s.KeepRest:
			prefix = s.RestPrefix
		default:
			return
		}
	}
	l := Line{At: time.Now().UTC(), Query: query, Class: cls.String(), Sample: sample, Vars: vars}
	if json.Valid(body) {
		l.Body = body
	} else {
		l.Text = string(body)
	}
	b, _ := json.Marshal(l)
	key := prefix + query
	s.mu.Lock()
	buf := s.bufs[key]
	if buf == nil {
		buf = &bytes.Buffer{}
		s.bufs[key] = buf
	}
	buf.Write(b)
	buf.WriteByte('\n')
	full := buf.Len() >= s.maxSize
	s.mu.Unlock()
	if full {
		go s.flushKey(context.WithoutCancel(ctx), key)
	}
}

// Run flushes buffers every interval until ctx is done.
func (s *Sink) Run(ctx context.Context, every time.Duration) error {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			s.FlushAll(context.Background())
			return nil
		case <-t.C:
			s.FlushAll(ctx)
		}
	}
}

func (s *Sink) FlushAll(ctx context.Context) {
	s.mu.Lock()
	keys := make([]string, 0, len(s.bufs))
	for k := range s.bufs {
		keys = append(keys, k)
	}
	s.mu.Unlock()
	for _, k := range keys {
		s.flushKey(ctx, k)
	}
}

func (s *Sink) flushKey(ctx context.Context, key string) {
	s.mu.Lock()
	buf := s.bufs[key]
	if buf == nil || buf.Len() == 0 {
		s.mu.Unlock()
		return
	}
	delete(s.bufs, key)
	s.seq++
	seq := s.seq
	s.mu.Unlock()

	var out bytes.Buffer
	enc, _ := zstd.NewWriter(&out, zstd.WithEncoderLevel(zstd.SpeedDefault))
	_, _ = enc.Write(buf.Bytes())
	_ = enc.Close()
	now := time.Now().UTC()
	obj := fmt.Sprintf("%s/%s/%s-%06d.jsonl.zst", key, now.Format("2006/01/02/15"), s.pod, seq)
	_, err := s.s3.PutObject(ctx, s.bucket, obj, &out, int64(out.Len()), minio.PutObjectOptions{ContentType: "application/zstd"})
	if err != nil {
		slog.Warn("rawsink: upload failed", "object", obj, "err", err)
	}
}
