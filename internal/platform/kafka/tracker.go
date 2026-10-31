package kafka

import (
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

type tp struct {
	topic string
	part  int32
}

type entry struct {
	offset int64
	epoch  int32
	done   bool
}

// partition tracks fetched-but-uncommitted offsets of one partition, in
// fetch order. The commit point is the first offset that is not done yet.
type partition struct {
	queue     []*entry
	next      kgo.EpochOffset // offset after the last fetched record
	committed int64
	sem       chan struct{}
}

// tracker is safe for concurrent use.
type tracker struct {
	mu         sync.Mutex
	parts      map[tp]*partition
	perPartMax int
}

func newTracker(perPartMax int) *tracker {
	if perPartMax <= 0 {
		perPartMax = 1
	}
	return &tracker{parts: map[tp]*partition{}, perPartMax: perPartMax}
}

// add registers a fetched record and returns its entry and the partition
// semaphore the caller must acquire before processing.
func (t *tracker) add(r *kgo.Record) (*entry, chan struct{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	k := tp{r.Topic, r.Partition}
	p := t.parts[k]
	if p == nil {
		p = &partition{committed: -1, sem: make(chan struct{}, t.perPartMax)}
		t.parts[k] = p
	}
	e := &entry{offset: r.Offset, epoch: r.LeaderEpoch}
	p.queue = append(p.queue, e)
	p.next = kgo.EpochOffset{Epoch: r.LeaderEpoch, Offset: r.Offset + 1}
	return e, p.sem
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

// pending reports how many records of the given partitions are not done.
func (t *tracker) pending(only map[string][]int32) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for topic, parts := range only {
		for _, part := range parts {
			if p := t.parts[tp{topic, part}]; p != nil {
				for _, e := range p.queue {
					if !e.done {
						n++
					}
				}
			}
		}
	}
	return n
}

func (t *tracker) drop(only map[string][]int32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for topic, parts := range only {
		for _, part := range parts {
			delete(t.parts, tp{topic, part})
		}
	}
}
