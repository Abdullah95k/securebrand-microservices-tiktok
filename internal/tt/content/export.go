package content

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	sharedpersist "github.com/abdullah95k/securebrand-microservices-tiktok/internal/persist"
)

// ExportOptions are `tksctl content export`'s.
type ExportOptions struct {
	// Topics are the tt.*.v1 topics to read, without the prefix.
	Topics []string
	// Group is the persister's consumer group, without the prefix: the
	// export stops at its committed offsets, so every record it sends was
	// already written to Postgres (the events read stored owners and
	// subtitles) and the live persister sends everything after.
	Group string
	// New returns an empty message for a topic (without the prefix), or
	// nil for one that has no content events: the persister's decoder.
	New func(topic string) proto.Message
	// Since starts each partition at its first record at or after this
	// time; zero starts at the beginning (what retention still holds).
	Since time.Time
	// Batch is how many records go through the emitter at a time.
	Batch int
	// Idle stops the export with an error after this long without a
	// record while partitions are still short of their end (30s).
	Idle time.Duration
}

// ExportResult is what an export read and sent.
type ExportResult struct {
	// Read is the records read by topic (without the prefix).
	Read map[string]int64 `json:"read"`
	// Missing are topics that do not exist (nothing to export).
	Missing []string `json:"missing_topics,omitempty"`
	// Undecodable records were skipped (the persister set them aside too).
	Undecodable int64 `json:"undecodable"`
	// Sent, Unchanged and Invalid are the emitter's outcomes: events
	// produced, held back by the change gate (already sent, or older than
	// what was sent), and refused by the contract's checks.
	Sent      int64 `json:"sent"`
	Unchanged int64 `json:"unchanged"`
	Invalid   int64 `json:"invalid"`
}

// Export sends the content.* events of the tt.*.v1 records Kafka still
// holds, through e (the persister's own path): records are decoded and
// cleaned as the persister does, then passed to Emit in batches. The
// change gate and the event ids (SHA-256 of id, observed time and content
// hash) make it safe to run beside the live persister and to run again: an
// event already sent is held back, and one sent twice has the same id. It
// commits no offsets.
func Export(ctx context.Context, opts []kgo.Opt, prefix string, e *Emitter, o ExportOptions) (ExportResult, error) {
	res := ExportResult{Read: map[string]int64{}}
	if o.Batch <= 0 {
		o.Batch = 500
	}
	names := make([]string, len(o.Topics))
	for i, t := range o.Topics {
		names[i] = prefix + t
	}
	admin, err := kgo.NewClient(opts...)
	if err != nil {
		return res, err
	}
	adm := kadm.NewClient(admin)
	committed, err := adm.FetchOffsets(ctx, prefix+o.Group)
	if err == nil {
		err = committed.Error()
	}
	if err != nil {
		admin.Close()
		return res, fmt.Errorf("content export: offsets of %s: %w", prefix+o.Group, err)
	}
	var starts kadm.ListedOffsets
	if o.Since.IsZero() {
		starts, err = adm.ListStartOffsets(ctx, names...)
	} else {
		starts, err = adm.ListOffsetsAfterMilli(ctx, o.Since.UnixMilli(), names...)
	}
	admin.Close()
	if err != nil {
		return res, fmt.Errorf("content export: start offsets: %w", err)
	}
	// A topic that does not exist (tt.subtitle.v1 before subtitles are on)
	// has nothing to export; any other error stops the export.
	for _, ps := range starts {
		for _, lo := range ps {
			if errors.Is(lo.Err, kerr.UnknownTopicOrPartition) {
				res.Missing = append(res.Missing, strings.TrimPrefix(lo.Topic, prefix))
				delete(starts[lo.Topic], lo.Partition)
			} else if lo.Err != nil {
				return res, fmt.Errorf("content export: start offset of %s/%d: %w", lo.Topic, lo.Partition, lo.Err)
			}
		}
	}
	slices.Sort(res.Missing)
	res.Missing = slices.Compact(res.Missing)

	// Each partition is read from its start to the persister's committed
	// offset; a partition the persister has not committed is skipped.
	ends := map[string]map[int32]int64{}
	from := map[string]map[int32]kgo.Offset{}
	starts.Each(func(lo kadm.ListedOffset) {
		end, ok := committed.Lookup(lo.Topic, lo.Partition)
		if !ok || end.At <= lo.Offset {
			return
		}
		if ends[lo.Topic] == nil {
			ends[lo.Topic], from[lo.Topic] = map[int32]int64{}, map[int32]kgo.Offset{}
		}
		ends[lo.Topic][lo.Partition] = end.At
		from[lo.Topic][lo.Partition] = kgo.NewOffset().At(lo.Offset)
	})
	open := 0
	for _, ps := range ends {
		open += len(ps)
	}
	if open == 0 {
		return res, nil
	}
	cl, err := kgo.NewClient(append(opts, kgo.ConsumePartitions(from))...)
	if err != nil {
		return res, err
	}
	defer cl.Close()

	counts := &SendCounts{}
	e.Counts = counts
	defer func() { e.Counts = nil }()
	idle := o.Idle
	if idle <= 0 {
		idle = 30 * time.Second
	}
	var batch []proto.Message
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := e.Emit(ctx, batch)
		batch = batch[:0]
		return err
	}
	for open > 0 {
		pctx, cancel := context.WithTimeout(ctx, idle)
		fs := cl.PollFetches(pctx)
		cancel()
		if err := ctx.Err(); err != nil {
			return res, err
		}
		if fs.Empty() && pctx.Err() != nil {
			return res, fmt.Errorf("content export: %d partitions not read to the persister's offset after %s without records", open, idle)
		}
		var ferr error
		fs.EachError(func(t string, p int32, err error) { ferr = fmt.Errorf("content export: %s/%d: %w", t, p, err) })
		if ferr != nil {
			return res, ferr
		}
		fs.EachPartition(func(fp kgo.FetchTopicPartition) {
			end, ok := ends[fp.Topic][fp.Partition]
			if !ok {
				return
			}
			for _, r := range fp.Records {
				if r.Offset >= end {
					break
				}
				base := strings.TrimPrefix(r.Topic, prefix)
				res.Read[base]++
				m := o.New(base)
				if m == nil {
					continue
				}
				if proto.Unmarshal(r.Value, m) != nil {
					res.Undecodable++
					continue
				}
				// The persister's writer strips NULs before it stores and
				// emits; the content hash must be the same.
				sharedpersist.StripNUL(m.ProtoReflect())
				batch = append(batch, m)
			}
			if n := len(fp.Records); n > 0 && fp.Records[n-1].Offset >= end-1 {
				delete(ends[fp.Topic], fp.Partition)
				open--
				cl.PauseFetchPartitions(map[string][]int32{fp.Topic: {fp.Partition}})
			}
		})
		if len(batch) >= o.Batch || open == 0 {
			if err := flush(); err != nil {
				return res, err
			}
		}
	}
	if err := flush(); err != nil {
		return res, err
	}
	res.Sent, res.Unchanged, res.Invalid = counts.Sent, counts.Unchanged, counts.Invalid
	return res, nil
}

// SendCounts tallies an emitter's outcomes for its caller (the export);
// the metrics count them for everyone.
type SendCounts struct{ Sent, Unchanged, Invalid int64 }
