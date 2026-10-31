// Package checkpoint stores short-lived crawl progress in Redis: resume
// points within a pagination stream, completed-task markers, stream leases
// and per-post/thread caches used to decide whether a re-scrape is needed.
package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/redisx"
)

// State is the resume point of one task segment.
type State struct {
	Cursor         string `json:"cursor"`
	Pages          int    `json:"pages"`
	PostsSeen      int64  `json:"posts_seen,omitempty"`
	ConsecutiveOld int32  `json:"consecutive_old,omitempty"`
	NewestSeenUnix int64  `json:"newest_seen,omitempty"`
	ItemsSeen      int64  `json:"items_seen,omitempty"`
	// Walls counts login walls hit at WallCursor on distinct identities.
	Walls      int    `json:"walls,omitempty"`
	WallCursor string `json:"wall_cursor,omitempty"`
	// Counters of the pages this segment already read, so a retried or
	// redelivered segment that resumes at Cursor reports them too.
	Counters map[string]int64 `json:"counters,omitempty"`
}

// Resume adds the saved counters to c.
func (s State) Resume(c map[string]int64) {
	for k, v := range s.Counters {
		c[k] += v
	}
}

// Store is the Redis-backed store.
type Store struct {
	r *redis.Client
}

func New(r *redis.Client) *Store { return &Store{r: r} }

func ckptKey(lineage string, seq uint32) string {
	return redisx.KeyPrefix + "ckpt:" + lineage + ":" + strconv.FormatUint(uint64(seq), 10)
}

// Load returns the checkpoint of a segment, or ok=false.
func (s *Store) Load(ctx context.Context, lineage string, seq uint32) (State, bool, error) {
	b, err := s.r.Get(ctx, ckptKey(lineage, seq)).Bytes()
	if errors.Is(err, redis.Nil) {
		return State{}, false, nil
	}
	if err != nil {
		return State{}, false, err
	}
	var st State
	if err := json.Unmarshal(b, &st); err != nil {
		return State{}, false, nil
	}
	return st, true, nil
}

// Save writes a checkpoint (24h TTL).
func (s *Store) Save(ctx context.Context, lineage string, seq uint32, st State) error {
	b, _ := json.Marshal(st)
	return s.r.Set(ctx, ckptKey(lineage, seq), b, 24*time.Hour).Err()
}

// IsDone reports whether a task already completed.
func (s *Store) IsDone(ctx context.Context, taskID string) (bool, error) {
	n, err := s.r.Exists(ctx, redisx.KeyPrefix+"done:"+taskID).Result()
	return n > 0, err
}

// MarkDone records task completion (7d TTL).
func (s *Store) MarkDone(ctx context.Context, taskID string) error {
	return s.r.Set(ctx, redisx.KeyPrefix+"done:"+taskID, "1", 7*24*time.Hour).Err()
}

var extendScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0`)

var releaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0`)

// Lease is an exclusive claim on a pagination stream.
type Lease struct {
	s      *Store
	key    string
	owner  string
	cancel context.CancelFunc
	done   chan struct{}
}

// Acquire claims scope for owner (the task lineage). The same lineage may
// re-acquire its own lease (redelivery after a crash). The lease is kept
// alive until Release.
func (s *Store) Acquire(ctx context.Context, scope, owner string, ttl time.Duration) (*Lease, bool, error) {
	k := redisx.KeyPrefix + "lease:" + scope
	ok, err := s.r.SetNX(ctx, k, owner, ttl).Result()
	if err != nil {
		return nil, false, err
	}
	if !ok {
		cur, err := s.r.Get(ctx, k).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return nil, false, err
		}
		if cur != owner {
			return nil, false, nil
		}
		s.r.PExpire(ctx, k, ttl)
	}
	hctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	l := &Lease{s: s, key: k, owner: owner, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(l.done)
		t := time.NewTicker(ttl / 3)
		defer t.Stop()
		for {
			select {
			case <-hctx.Done():
				return
			case <-t.C:
				extendScript.Run(hctx, s.r, []string{k}, owner, ttl.Milliseconds())
			}
		}
	}()
	return l, true, nil
}

// Release drops the lease if still owned.
func (l *Lease) Release(ctx context.Context) {
	l.cancel()
	<-l.done
	releaseScript.Run(context.WithoutCancel(ctx), l.s.r, []string{l.key}, l.owner)
}

// PostCache remembers what was collected for a post across jobs.
type PostCache struct {
	Reported        int64 `json:"reported"`
	NewestComment   int64 `json:"newest_comment"`
	ThreadsComplete bool  `json:"threads_complete"`
}

const cacheTTL = 45 * 24 * time.Hour

func (s *Store) GetPost(ctx context.Context, postID int64) (PostCache, bool) {
	b, err := s.r.Get(ctx, redisx.KeyPrefix+"pc:"+strconv.FormatInt(postID, 10)).Bytes()
	if err != nil {
		return PostCache{}, false
	}
	var pc PostCache
	if json.Unmarshal(b, &pc) != nil {
		return PostCache{}, false
	}
	return pc, true
}

func (s *Store) SetPost(ctx context.Context, postID int64, pc PostCache) error {
	b, _ := json.Marshal(pc)
	return s.r.Set(ctx, redisx.KeyPrefix+"pc:"+strconv.FormatInt(postID, 10), b, cacheTTL).Err()
}

// GetThread returns the reply count collected last time for a comment.
func (s *Store) GetThread(ctx context.Context, commentID int64) (int64, bool) {
	v, err := s.r.Get(ctx, redisx.KeyPrefix+"tc:"+strconv.FormatInt(commentID, 10)).Int64()
	if err != nil {
		return 0, false
	}
	return v, true
}

func (s *Store) SetThread(ctx context.Context, commentID, replies int64) error {
	return s.r.Set(ctx, redisx.KeyPrefix+"tc:"+strconv.FormatInt(commentID, 10), replies, cacheTTL).Err()
}
