// Package lanes shares a stage's in-flight capacity between the priority
// lanes (p0 on-demand, p1 scheduled, p2 backfill). Slots go to the highest
// priority lane that is waiting, so idle on-demand capacity is lent to
// scheduled and backfill work and taken back as tasks finish. Each lane has
// a cap (backfill never takes more than its share) and a guaranteed minimum
// (no lane starves however busy the others are).
package lanes

import (
	"container/list"
	"context"
	"sync"
)

// Lane limits, in slots.
type Lane struct {
	Name string
	Max  int // most slots the lane may hold
	Min  int // slots a waiting lane gets before higher lanes are served
}

// Scheduler hands out slots. It is safe for concurrent use.
type Scheduler struct {
	total int
	// OnChange, when set, is called (under the lock, keep it cheap) with a
	// lane's slots in use and waiters after each change.
	OnChange func(lane string, used, waiting int)

	mu    sync.Mutex
	used  int
	lanes []*laneState // in priority order
}

type laneState struct {
	Lane
	used    int
	waiters list.List // of chan struct{}
	min0    *int      // configured Min, kept across SetMax
}

// New returns a scheduler with total slots and lanes in priority order
// (highest first). Max is clamped to [1, total]; the sum of Min is kept
// within total by trimming the lowest lanes' minimums.
func New(total int, lanes ...Lane) *Scheduler {
	s := &Scheduler{total: max(1, total)}
	budget := s.total
	for _, l := range lanes {
		l.Max = min(max(1, l.Max), s.total)
		l.Min = min(max(0, l.Min), l.Max, budget)
		budget -= l.Min
		s.lanes = append(s.lanes, &laneState{Lane: l})
	}
	return s
}

// Acquire blocks until lane (an index into the lanes given to New) may
// start one more task, or ctx is done (false).
func (s *Scheduler) Acquire(ctx context.Context, lane int) bool {
	ch := make(chan struct{})
	s.mu.Lock()
	l := s.lanes[lane]
	elem := l.waiters.PushBack(ch)
	s.dispatch()
	s.changed(l)
	s.mu.Unlock()
	select {
	case <-ch:
		return true
	case <-ctx.Done():
		s.mu.Lock()
		select {
		case <-ch: // granted while we were giving up: hand it back
			s.release(l)
		default:
			l.waiters.Remove(elem)
			s.changed(l)
		}
		s.mu.Unlock()
		return false
	}
}

// Release returns a slot taken by Acquire.
func (s *Scheduler) Release(lane int) {
	s.mu.Lock()
	s.release(s.lanes[lane])
	s.mu.Unlock()
}

func (s *Scheduler) release(l *laneState) {
	l.used--
	s.used--
	s.changed(l)
	s.dispatch()
}

// dispatch grants free slots: first to waiting lanes below their minimum,
// then by priority to waiting lanes below their cap.
func (s *Scheduler) dispatch() {
	for s.used < s.total {
		next := s.pick()
		if next == nil {
			return
		}
		ch := next.waiters.Remove(next.waiters.Front()).(chan struct{})
		next.used++
		s.used++
		close(ch)
		s.changed(next)
	}
}

func (s *Scheduler) pick() *laneState {
	for _, l := range s.lanes {
		if l.waiters.Len() > 0 && l.used < l.Min {
			return l
		}
	}
	for _, l := range s.lanes {
		if l.waiters.Len() > 0 && l.used < l.Max {
			return l
		}
	}
	return nil
}

func (s *Scheduler) changed(l *laneState) {
	if s.OnChange != nil {
		s.OnChange(l.Name, l.used, l.waiters.Len())
	}
}

// SetMax changes a lane's cap; 0 pauses the lane (its minimum is ignored
// while paused). Tasks already running are not interrupted.
func (s *Scheduler) SetMax(lane, n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l := s.lanes[lane]
	if l.min0 == nil {
		m := l.Min
		l.min0 = &m
	}
	l.Max = min(max(0, n), s.total)
	l.Min = min(*l.min0, l.Max)
	s.dispatch()
}

// Stats is a lane's current use.
type Stats struct {
	Name          string
	Used, Waiting int
}

// Stats returns every lane's use, in priority order.
func (s *Scheduler) Stats() []Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Stats, 0, len(s.lanes))
	for _, l := range s.lanes {
		out = append(out, Stats{Name: l.Name, Used: l.used, Waiting: l.waiters.Len()})
	}
	return out
}

// Slots adapts one lane to kafka.Slots.
type Slots struct {
	S    *Scheduler
	Lane int
}

func (l Slots) Acquire(ctx context.Context) bool { return l.S.Acquire(ctx, l.Lane) }
func (l Slots) Release()                         { l.S.Release(l.Lane) }
