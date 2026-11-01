package kafka

import (
	"context"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

type tp struct {
	topic string
	part  int32
}

type entry struct {
	offset  int64
	epoch   int32
	started bool // its handler runs (or ran)
	done    bool
}

// partition tracks fetched-but-uncommitted offsets of one partition, in
// fetch order. The commit point is the first offset that is not done yet.
type partition struct {
	queue     []*entry
	next      kgo.EpochOffset // offset after the last fetched record
	committed int64
	sem       chan struct{}
	// ctx is the handlers' context for this partition's records, cancelled
	// when the partition is revoked or lost: its records belong to the next
	// owner then, so a handler still running must stop, not finish them a
	// second time.
	ctx    context.Context
	cancel context.CancelFunc
}

// tracker is safe for concurrent use.
type tracker struct {
	mu         sync.Mutex
	parts      map[tp]*partition
	perPartMax int
	// owned holds the partitions assigned to this member. A revoke can
	// come while records of a fetch are still being handed out (the poll
	// loop waits for capacity meanwhile); those of a partition no longer
	// owned are the next owner's, and must neither run here nor have their
	// offsets committed by this member.
	owned map[tp]bool
}

func newTracker(perPartMax int) *tracker {
	if perPartMax <= 0 {
		perPartMax = 1
	}
	return &tracker{parts: map[tp]*partition{}, perPartMax: perPartMax, owned: map[tp]bool{}}
}

// assign records partitions assigned to this member (before their fetches
// begin).
func (t *tracker) assign(assigned map[string][]int32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for topic, parts := range assigned {
		for _, part := range parts {
			t.owned[tp{topic, part}] = true
		}
	}
}

// add registers a fetched record and returns its entry, the partition
// semaphore the caller must acquire before processing, and the context to
// process it in (derived from base, cancelled when the partition goes). It
// returns a nil entry for a record of a partition this member doesn't own
// (any more): the caller skips it.
func (t *tracker) add(r *kgo.Record, base context.Context) (*entry, chan struct{}, context.Context) {
	t.mu.Lock()
	defer t.mu.Unlock()
	k := tp{r.Topic, r.Partition}
	if !t.owned[k] {
		return nil, nil, nil
	}
	p := t.parts[k]
	if p == nil {
		p = &partition{committed: -1, sem: make(chan struct{}, t.perPartMax)}
		p.ctx, p.cancel = context.WithCancel(base)
		t.parts[k] = p
	}
	e := &entry{offset: r.Offset, epoch: r.LeaderEpoch}
	p.queue = append(p.queue, e)
	p.next = kgo.EpochOffset{Epoch: r.LeaderEpoch, Offset: r.Offset + 1}
	return e, p.sem, p.ctx
}

// start marks e's handler as running, unless r's partition was revoked
// while e waited for capacity: then it must not run here (false).
func (t *tracker) start(e *entry, r *kgo.Record) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.owned[tp{r.Topic, r.Partition}] {
		return false
	}
	e.started = true
	return true
}

func (t *tracker) markDone(e *entry) {
	t.mu.Lock()
	e.done = true
	t.mu.Unlock()
}

// commitable returns, for every partition (or only those in only, when
// non-nil), the contiguous commit point if it advanced since the last call.
func (t *tracker) commitable(only map[string][]int32) map[string]map[int32]kgo.EpochOffset {
	t.mu.Lock()
	defer t.mu.Unlock()
	want := func(k tp) bool {
		if only == nil {
			return true
		}
		for _, p := range only[k.topic] {
			if p == k.part {
				return true
			}
		}
		return false
	}
	out := map[string]map[int32]kgo.EpochOffset{}
	for k, p := range t.parts {
		if !want(k) {
			continue
		}
		i := 0
		for i < len(p.queue) && p.queue[i].done {
			i++
		}
		var at kgo.EpochOffset
		if i < len(p.queue) {
			at = kgo.EpochOffset{Epoch: p.queue[i].epoch, Offset: p.queue[i].offset}
		} else {
			at = p.next
		}
		p.queue = p.queue[i:]
		if at.Offset <= p.committed {
			continue
		}
		if out[k.topic] == nil {
			out[k.topic] = map[int32]kgo.EpochOffset{}
		}
		out[k.topic][k.part] = at
	}
	return out
}

// markCommitted records a successful commit.
func (t *tracker) markCommitted(offsets map[string]map[int32]kgo.EpochOffset) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for topic, parts := range offsets {
		for part, eo := range parts {
			if p := t.parts[tp{topic, part}]; p != nil && eo.Offset > p.committed {
				p.committed = eo.Offset
			}
		}
	}
}

// pending reports how many records of the given partitions are running
// (started, not done). Those not started never will be once their
// partition goes, so a revoke doesn't wait for them.
func (t *tracker) pending(only map[string][]int32) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for topic, parts := range only {
		for _, part := range parts {
			if p := t.parts[tp{topic, part}]; p != nil {
				for _, e := range p.queue {
					if e.started && !e.done {
						n++
					}
				}
			}
		}
	}
	return n
}

// disown stops accepting records of partitions being revoked, while those
// already running get a moment to finish (drop comes after).
func (t *tracker) disown(only map[string][]int32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for topic, parts := range only {
		for _, part := range parts {
			delete(t.owned, tp{topic, part})
		}
	}
}

// drop forgets partitions revoked or lost: their handlers are cancelled and
// their records are no longer accepted.
func (t *tracker) drop(only map[string][]int32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for topic, parts := range only {
		for _, part := range parts {
			k := tp{topic, part}
			if p := t.parts[k]; p != nil && p.cancel != nil {
				p.cancel()
			}
			delete(t.parts, k)
			delete(t.owned, k)
		}
	}
}
