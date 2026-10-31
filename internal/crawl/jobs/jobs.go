// Package jobs keeps per-job task accounting in Redis so a job knows when
// every task it spawned has finished, regardless of retries, duplicates and
// redeliveries. Invariant: pending = |spawned| - |done|.
package jobs

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/redisx"
)

// FinalizeSet holds job ids whose pending count reached zero.
const FinalizeSet = redisx.KeyPrefix + "jobs:finalize"

const ttl = 7 * 24 * time.Hour

// Post comment-collection states recorded per job.
const (
	PostPending   = "pending"
	PostComplete  = "complete"
	PostTruncated = "truncated"
	PostSkipped   = "skipped" // unchanged since the last scrape, or no comments
	PostFailed    = "failed"
)

func key(job, name string) string { return redisx.KeyPrefix + "{job:" + job + "}:" + name }

var spawnScript = redis.NewScript(`
local added = 0
for i = 3, #ARGV do
  if redis.call('SADD', KEYS[1], ARGV[i]) == 1 then
    added = added + 1
  end
end
if added > 0 then redis.call('INCRBY', KEYS[2], added) end
redis.call('PEXPIRE', KEYS[1], ARGV[1])
redis.call('PEXPIRE', KEYS[2], ARGV[1])
redis.call('SET', KEYS[3], ARGV[2], 'PX', ARGV[1])
return added
`)

var doneScript = redis.NewScript(`
if redis.call('SISMEMBER', KEYS[1], ARGV[3]) == 0 then
  return -1
end
local pending = tonumber(redis.call('GET', KEYS[3]) or '0')
if redis.call('SADD', KEYS[2], ARGV[3]) == 1 then
  pending = redis.call('DECR', KEYS[3])
  for i = 4, #ARGV, 2 do
    redis.call('HINCRBY', KEYS[4], ARGV[i], ARGV[i+1])
  end
end
redis.call('PEXPIRE', KEYS[2], ARGV[1])
redis.call('PEXPIRE', KEYS[3], ARGV[1])
redis.call('PEXPIRE', KEYS[4], ARGV[1])
redis.call('SET', KEYS[5], ARGV[2], 'PX', ARGV[1])
return pending
`)

// Tracker records spawns and completions.
type Tracker struct {
	r *redis.Client
}

func New(r *redis.Client) *Tracker { return &Tracker{r: r} }

// Spawn registers child task ids before they are produced.
func (t *Tracker) Spawn(ctx context.Context, job string, taskIDs ...string) error {
	if len(taskIDs) == 0 || job == "" {
		return nil
	}
	args := []any{ttl.Milliseconds(), time.Now().Unix()}
	for _, id := range taskIDs {
		args = append(args, id)
	}
	return spawnScript.Run(ctx, t.r, []string{key(job, "spawned"), key(job, "pending"), key(job, "last")}, args...).Err()
}

// Done marks a task finished and adds counters. When the job's pending count
// reaches zero the job is queued for finalisation.
func (t *Tracker) Done(ctx context.Context, job, taskID string, counters map[string]int64) (int64, error) {
	if job == "" {
		return 0, nil
	}
	args := []any{ttl.Milliseconds(), time.Now().Unix(), taskID}
	for k, v := range counters {
		args = append(args, k, v)
	}
	pending, err := doneScript.Run(ctx, t.r, []string{key(job, "spawned"), key(job, "done"), key(job, "pending"),
		key(job, "counters"), key(job, "last")}, args...).Int64()
	if err != nil {
		return 0, err
	}
	if pending == -1 {
		return 0, fmt.Errorf("jobs: task %s was never spawned for job %s", taskID, job)
	}
	if pending == 0 {
		if err := t.r.SAdd(ctx, FinalizeSet, job).Err(); err != nil {
			return 0, err
		}
	}
	return pending, nil
}

// AddCounters adds to the job's counters outside of a task (the finalizer).
func (t *Tracker) AddCounters(ctx context.Context, job string, counters map[string]int64) error {
	k := key(job, "counters")
	pipe := t.r.TxPipeline()
	for n, v := range counters {
		pipe.HIncrBy(ctx, k, n, v)
	}
	pipe.PExpire(ctx, k, ttl)
	_, err := pipe.Exec(ctx)
	return err
}

var setPostScript = redis.NewScript(`
local cur = redis.call('HGET', KEYS[1], ARGV[1])
local v = ARGV[2] .. '|' .. ARGV[3]
local kept = 0
if cur then
  local st = string.match(cur, '[^|]*$')
  if st == 'complete' or st == 'truncated' or st == 'failed' then
    v = ARGV[2] .. '|' .. st
    kept = 1
  end
end
redis.call('HSET', KEYS[1], ARGV[1], v)
redis.call('PEXPIRE', KEYS[1], ARGV[4])
return kept
`)

// SetPost records a post's comment-collection state for the job. A post
// the job already collected keeps its state (the job's refresh of an older
// post may finish before the timeline reaches it) and SetPost reports it, so
// the caller does not read it again.
func (t *Tracker) SetPost(ctx context.Context, job string, postID int64, createdUnix int64, reported int64, state string) (collected bool, err error) {
	n, err := setPostScript.Run(ctx, t.r, []string{key(job, "posts")}, strconv.FormatInt(postID, 10),
		strconv.FormatInt(createdUnix, 10)+"|"+strconv.FormatInt(reported, 10), state, ttl.Milliseconds()).Int()
	return n == 1, err
}

// UpdatePostState changes only the state part of a recorded post.
func (t *Tracker) UpdatePostState(ctx context.Context, job string, postID int64, state string) error {
	k := key(job, "posts")
	f := strconv.FormatInt(postID, 10)
	cur, err := t.r.HGet(ctx, k, f).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	parts := strings.SplitN(cur, "|", 3)
	if len(parts) != 3 {
		parts = []string{"0", "0", ""}
	}
	parts[2] = state
	return t.r.HSet(ctx, k, f, strings.Join(parts, "|")).Err()
}

// PostState is one entry of Posts.
type PostState struct {
	PostID    int64
	CreatedAt time.Time
	Reported  int64
	State     string
}

// Posts returns every post recorded for the job.
func (t *Tracker) Posts(ctx context.Context, job string) ([]PostState, error) {
	m, err := t.r.HGetAll(ctx, key(job, "posts")).Result()
	if err != nil {
		return nil, err
	}
	out := make([]PostState, 0, len(m))
	for f, v := range m {
		id, _ := strconv.ParseInt(f, 10, 64)
		parts := strings.SplitN(v, "|", 3)
		if len(parts) != 3 {
			continue
		}
		c, _ := strconv.ParseInt(parts[0], 10, 64)
		r, _ := strconv.ParseInt(parts[1], 10, 64)
		out = append(out, PostState{PostID: id, CreatedAt: time.Unix(c, 0).UTC(), Reported: r, State: parts[2]})
	}
	return out, nil
}

// Full passes. A post task that reads a known post's comments from the
// start (a recrawl, or a full job on a post crawled before) begins a pass
// for its lineage, appends the top-level comments of every page it reads
// (id and reply count) to the post's seen list, and marks the post when it
// reached the last page. The finalizer then compares the post's stored
// comments with the seen list. Entries are packed 12 bytes each, appended,
// so a retried page only repeats entries, and several lineages of one post
// only add to the list.

// BeginPass records the start of lineage's full pass over post. Calling it
// again keeps the first start.
func (t *Tracker) BeginPass(ctx context.Context, job, lineage string, postID int64, start time.Time) error {
	k := key(job, "passes")
	pipe := t.r.TxPipeline()
	pipe.HSetNX(ctx, k, lineage, strconv.FormatInt(postID, 10)+"|"+strconv.FormatInt(start.UnixMilli(), 10))
	pipe.PExpire(ctx, k, ttl)
	_, err := pipe.Exec(ctx)
	return err
}

// InPass reports whether lineage began a full pass.
func (t *Tracker) InPass(ctx context.Context, job, lineage string) (bool, error) {
	return t.r.HExists(ctx, key(job, "passes"), lineage).Result()
}

func seenKey(job string, postID int64) string { return key(job, "seen:"+strconv.FormatInt(postID, 10)) }

// SeenComment is a top-level comment a pass read, with the reply count
// Facebook gave for it.
type SeenComment struct {
	ID      int64
	Replies int64
}

const seenSize = 12

// AddSeen appends comments to post's seen list.
func (t *Tracker) AddSeen(ctx context.Context, job string, postID int64, cs []SeenComment) error {
	if len(cs) == 0 {
		return nil
	}
	buf := make([]byte, 0, seenSize*len(cs))
	for _, c := range cs {
		buf = binary.BigEndian.AppendUint64(buf, uint64(c.ID))
		buf = binary.BigEndian.AppendUint32(buf, uint32(min(max(c.Replies, 0), math.MaxUint32)))
	}
	k := seenKey(job, postID)
	pipe := t.r.TxPipeline()
	pipe.Append(ctx, k, string(buf))
	pipe.PExpire(ctx, k, ttl)
	_, err := pipe.Exec(ctx)
	return err
}

// EndPass marks post as read to its last page by lineage's pass; reported
// is the comment count Facebook gave on that page (0 = none).
func (t *Tracker) EndPass(ctx context.Context, job, lineage string, postID int64, postCreated time.Time, reported int64) error {
	v, err := t.r.HGet(ctx, key(job, "passes"), lineage).Result()
	if err != nil {
		return fmt.Errorf("jobs: end pass: %w", err)
	}
	_, start, _ := strings.Cut(v, "|")
	k := key(job, "passed")
	pipe := t.r.TxPipeline()
	pipe.HSet(ctx, k, strconv.FormatInt(postID, 10),
		strconv.FormatInt(postCreated.Unix(), 10)+"|"+start+"|"+strconv.FormatInt(reported, 10))
	pipe.PExpire(ctx, k, ttl)
	_, err = pipe.Exec(ctx)
	return err
}

// Pass is a post read in full during the job.
type Pass struct {
	PostID      int64
	PostCreated time.Time
	Start       time.Time
	Reported    int64 // Facebook's comment count at the end of the pass (0 = unknown)
}

// Passes returns the posts read in full, and every post with a seen list
// (complete or not), so the caller can clear them all.
func (t *Tracker) Passes(ctx context.Context, job string) (done []Pass, posts []int64, err error) {
	m, err := t.r.HGetAll(ctx, key(job, "passed")).Result()
	if err != nil {
		return nil, nil, err
	}
	for f, v := range m {
		id, _ := strconv.ParseInt(f, 10, 64)
		parts := strings.Split(v, "|")
		if len(parts) != 3 {
			continue
		}
		cu, _ := strconv.ParseInt(parts[0], 10, 64)
		su, _ := strconv.ParseInt(parts[1], 10, 64)
		rep, _ := strconv.ParseInt(parts[2], 10, 64)
		done = append(done, Pass{PostID: id, PostCreated: time.Unix(cu, 0).UTC(), Start: time.UnixMilli(su).UTC(), Reported: rep})
	}
	begun, err := t.r.HVals(ctx, key(job, "passes")).Result()
	if err != nil {
		return nil, nil, err
	}
	uniq := map[int64]bool{}
	for _, v := range begun {
		p, _, _ := strings.Cut(v, "|")
		if id, err := strconv.ParseInt(p, 10, 64); err == nil && !uniq[id] {
			uniq[id] = true
			posts = append(posts, id)
		}
	}
	return done, posts, nil
}

// Seen returns post's seen comments: id to reply count (the latest read).
func (t *Tracker) Seen(ctx context.Context, job string, postID int64) (map[int64]int64, error) {
	b, err := t.r.Get(ctx, seenKey(job, postID)).Bytes()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	out := make(map[int64]int64, len(b)/seenSize)
	for i := 0; i+seenSize <= len(b); i += seenSize {
		out[int64(binary.BigEndian.Uint64(b[i:]))] = int64(binary.BigEndian.Uint32(b[i+8:]))
	}
	return out, nil
}

// ClearPasses drops the pass records and seen lists of posts.
func (t *Tracker) ClearPasses(ctx context.Context, job string, posts []int64) error {
	pipe := t.r.TxPipeline()
	for _, p := range posts {
		pipe.Del(ctx, seenKey(job, p))
	}
	pipe.Del(ctx, key(job, "passed"), key(job, "passes"))
	_, err := pipe.Exec(ctx)
	return err
}

// Status is the live state of a job.
type Status struct {
	Pending      int64
	Spawned      int64
	Done         int64
	Counters     map[string]int64
	LastActivity time.Time
}

func (t *Tracker) Status(ctx context.Context, job string) (Status, error) {
	pipe := t.r.Pipeline()
	pending := pipe.Get(ctx, key(job, "pending"))
	spawned := pipe.SCard(ctx, key(job, "spawned"))
	done := pipe.SCard(ctx, key(job, "done"))
	counters := pipe.HGetAll(ctx, key(job, "counters"))
	last := pipe.Get(ctx, key(job, "last"))
	_, err := pipe.Exec(ctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		return Status{}, err
	}
	s := Status{Spawned: spawned.Val(), Done: done.Val(), Counters: map[string]int64{}}
	s.Pending, _ = strconv.ParseInt(pending.Val(), 10, 64)
	for k, v := range counters.Val() {
		s.Counters[k], _ = strconv.ParseInt(v, 10, 64)
	}
	if l, err := strconv.ParseInt(last.Val(), 10, 64); err == nil {
		s.LastActivity = time.Unix(l, 0).UTC()
	}
	return s, nil
}

// Cancel flags a job; workers check it between pages.
func (t *Tracker) Cancel(ctx context.Context, job string) error {
	return t.r.Set(ctx, key(job, "cancel"), "1", ttl).Err()
}

func (t *Tracker) Cancelled(ctx context.Context, job string) bool {
	if job == "" {
		return false
	}
	n, err := t.r.Exists(ctx, key(job, "cancel")).Result()
	return err == nil && n > 0
}

// ReadyToFinalize returns up to n jobs ready for finalisation. They stay
// in the set until Finalized, so a finaliser that stops before finishing
// them leaves them to the next one.
func (t *Tracker) ReadyToFinalize(ctx context.Context, n int64) ([]string, error) {
	return t.r.SRandMemberN(ctx, FinalizeSet, n).Result()
}

// Finalized removes a finalised job from the set.
func (t *Tracker) Finalized(ctx context.Context, job string) error {
	return t.r.SRem(ctx, FinalizeSet, job).Err()
}

// Expire shortens the retention of a finished job's keys.
func (t *Tracker) Expire(ctx context.Context, job string, d time.Duration) error {
	pipe := t.r.Pipeline()
	for _, n := range []string{"spawned", "done", "pending", "counters", "last", "posts", "cancel", "passes", "passed"} {
		pipe.PExpire(ctx, key(job, n), d)
	}
	_, err := pipe.Exec(ctx)
	return err
}
