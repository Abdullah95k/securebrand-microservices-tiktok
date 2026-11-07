package content

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	// Idle ends the export after this long without a record while
	// partitions are still short of their end (30s); those partitions are
	// listed in the result as stalled. An offset can hold no readable
	// record (a transaction marker, an aborted or compacted batch), so a
	// partition can end short of the persister's offset with nothing left
	// to read.
	Idle time.Duration
}

// Stalled is a partition the export stopped reading short of the
// persister's offset: nothing more came within ExportOptions.Idle.
type Stalled struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	// LastRead is the last offset read (-1: none); End the persister's
	// committed offset the export read towards.
	LastRead int64 `json:"last_read"`
	End      int64 `json:"end"`
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
	// Stalled are the partitions that ended short of their offset.
	Stalled []Stalled `json:"stalled,omitempty"`
}

// Export sends the content.* events of the tt.*.v1 records Kafka still
// holds, through e (the persister's own path): records are decoded and
// cleaned as the persister does, then passed to Emit in batches. The
// change gate and the event ids (SHA-256 of id, observed time and content
// hash) make it safe to run beside the live persister and to run again: an
// event already sent is held back, and one sent twice has the same id. It
// commits no offsets.
func Export(ctx context.Context, opts []kgo.Opt, prefix string, e *Emitter, o ExportOptions) (res ExportResult, err error) {
	res = ExportResult{Read: map[string]int64{}}
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
	// Bounded fetches: the export runs beside a live service (in its pod),
	// so it holds at most one fetch (a few MiB) and one batch at a time.
	cl, err := kgo.NewClient(append(opts, kgo.ConsumePartitions(from),
		kgo.FetchMaxBytes(exportFetchBytes), kgo.FetchMaxPartitionBytes(exportPartitionBytes),
		kgo.MaxConcurrentFetches(1))...)
	if err != nil {
		return res, err
	}
	defer cl.Close()

	// The emitter's outcomes go into the result however the export ends:
	// batches are sent as they fill, so a failed export has sent some.
	counts := &SendCounts{}
	e.Counts = counts
	defer func() {
		e.Counts = nil
		res.Sent, res.Unchanged, res.Invalid = counts.Sent, counts.Unchanged, counts.Invalid
	}()
	last := map[string]map[int32]int64{}
	idle := o.Idle
	if idle <= 0 {
		idle = 30 * time.Second
	}
	batch := make([]proto.Message, 0, o.Batch)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := e.Emit(ctx, batch)
		clear(batch)
		batch = batch[:0]
		return err
	}
	for open > 0 {
		pctx, cancel := context.WithTimeout(ctx, idle)
		fs := cl.PollRecords(pctx, o.Batch-len(batch))
		cancel()
		if err := ctx.Err(); err != nil {
			return res, err
		}
		if fs.Empty() && pctx.Err() != nil {
			// Nothing more came: the partitions still open end here. Send
			// what was read and name them.
			for t, ps := range ends {
				for p, end := range ps {
					lr, ok := last[t][p]
					if !ok {
						lr = -1
					}
					res.Stalled = append(res.Stalled, Stalled{Topic: strings.TrimPrefix(t, prefix), Partition: p, LastRead: lr, End: end})
				}
			}
			slices.SortFunc(res.Stalled, func(a, b Stalled) int {
				if c := strings.Compare(a.Topic, b.Topic); c != 0 {
					return c
				}
				return int(a.Partition - b.Partition)
			})
			slog.Warn("content export: partitions ended short of the persister's offset", "idle", idle, "stalled", res.Stalled)
			break
		}
		var ferr error
		fs.EachError(func(t string, p int32, err error) {
			if !errors.Is(err, context.DeadlineExceeded) {
				ferr = fmt.Errorf("content export: %s/%d: %w", t, p, err)
			}
		})
		if ferr != nil {
			return res, ferr
		}
		fs.EachRecord(func(r *kgo.Record) {
			end, ok := ends[r.Topic][r.Partition]
			if !ok || r.Offset >= end {
				return // a partition already read to its end
			}
			if last[r.Topic] == nil {
				last[r.Topic] = map[int32]int64{}
			}
			last[r.Topic][r.Partition] = r.Offset
			if r.Offset >= end-1 {
				delete(ends[r.Topic], r.Partition)
				open--
				cl.PauseFetchPartitions(map[string][]int32{r.Topic: {r.Partition}})
			}
			base := strings.TrimPrefix(r.Topic, prefix)
			res.Read[base]++
			m := o.New(base)
			if m == nil {
				return
			}
			if proto.Unmarshal(r.Value, m) != nil {
				res.Undecodable++
				return
			}
			// The persister's writer strips NULs before it stores and
			// emits; the content hash must be the same.
			sharedpersist.StripNUL(m.ProtoReflect())
			batch = append(batch, m)
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
	return res, nil
}

// The export's fetch bounds: one fetch of at most 4 MiB, 1 MiB per
// partition.
const (
	exportFetchBytes     = 4 << 20
	exportPartitionBytes = 1 << 20
)

// SendCounts tallies an emitter's outcomes for its caller (the export);
// the metrics count them for everyone.
type SendCounts struct{ Sent, Unchanged, Invalid int64 }
