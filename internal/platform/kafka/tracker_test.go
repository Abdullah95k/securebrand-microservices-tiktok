package kafka

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestTrackerCommitsOnlyContiguousOffsets(t *testing.T) {
	tr := newTracker(10)
	var es []*entry
	for off := int64(10); off < 15; off++ {
		e, _ := tr.add(&kgo.Record{Topic: "t", Partition: 0, Offset: off, LeaderEpoch: 1})
		es = append(es, e)
	}
	if got := tr.commitable(nil); got["t"][0].Offset != 10 {
		t.Fatalf("nothing done: commit point should stay at 10, got %v", got)
	}
	tr.markCommitted(map[string]map[int32]kgo.EpochOffset{"t": {0: {Offset: 10}}})
	// Complete out of order: 11, 12 done; 10 still running.
	tr.markDone(es[1])
	tr.markDone(es[2])
	if got := tr.commitable(nil); len(got) != 0 {
		t.Fatalf("head not done: no new commit expected, got %v", got)
	}
	tr.markDone(es[0])
	got := tr.commitable(nil)
	if got["t"][0].Offset != 13 {
		t.Fatalf("want commit at 13 (next unfinished), got %v", got)
	}
	tr.markCommitted(got)
	tr.markDone(es[3])
	tr.markDone(es[4])
	if got := tr.commitable(nil); got["t"][0].Offset != 15 {
		t.Fatalf("all done: want 15, got %v", got)
	}
	if n := tr.pending(map[string][]int32{"t": {0}}); n != 0 {
		t.Fatalf("pending = %d", n)
	}
}
