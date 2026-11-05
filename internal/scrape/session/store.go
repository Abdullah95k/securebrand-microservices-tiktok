package session

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/proxy"
)

// Store keeps each crawler's sessions and its exits' page-load pacing, so
// the next crawler resumes them instead of loading a page on every exit at
// once, a burst of page loads (the burst Facebook walled in fbs's pilot). A running crawler checkpoints them every
// few minutes under a lease it renews; when it stops (or its lease runs out
// because it died without stopping), its last snapshot is free for another
// crawler to take, once.
type Store interface {
	// Checkpoint replaces owner's snapshot and renews its lease for lease.
	Checkpoint(ctx context.Context, owner string, s Snapshot, lease time.Duration) error
	// Release ends owner's lease at once: it stopped, and its snapshot is
	// free to take.
	Release(ctx context.Context, owner string) error
	// TakeOrphans removes and returns the snapshots of crawlers other than
	// self whose lease has ended. Each snapshot goes to one crawler.
	TakeOrphans(ctx context.Context, self string) ([]Snapshot, error)
}

// Snapshot is what one crawler leaves for the next.
type Snapshot struct {
	Owner   string    `json:"owner"`
	SavedAt time.Time `json:"saved_at"`
	// Final marks the snapshot a crawler wrote as it stopped; a periodic one
	// left behind means it stopped without saving (it crashed, or was
	// killed).
	Final    bool                       `json:"final"`
	Sessions []Saved                    `json:"sessions"`
	Exits    map[string]proxy.ExitState `json:"exits"`
}

// Saved is a session kept across a restart. It names its exit, never the
// proxy URL, which holds the proxy's credentials.
type Saved struct {
	ID           string            `json:"id"`
	Tier         string            `json:"tier"`
	Exit         string            `json:"exit"` // tier|host:port; empty for direct egress
	Profile      string            `json:"profile"`
	SessionID    string            `json:"session_id"`
	Req          uint64            `json:"req"`
	Requests     int               `json:"requests"`
	MaxRequests  int               `json:"max_requests"`
	Tokens       Tokens            `json:"tokens"`
	Bootstrapped time.Time         `json:"bootstrapped"`
	Cookies      map[string]string `json:"cookies"`
	SavedAt      time.Time         `json:"saved_at"`
}

// SavedSessionTTL is how long a saved session stays good to resume: past
// it, its tokens have likely expired, and a page load is due anyway.
const SavedSessionTTL = 12 * time.Hour

// RedisStore keeps the snapshots in Redis: one key per crawler, a lease key
// per crawler, and the set of crawlers that have a snapshot. NS (the Kafka
// topic prefix) keeps deployments sharing a Redis apart.
type RedisStore struct {
	R  redis.Cmdable
	NS string
}

// Every key is in one hash slot.
func (s *RedisStore) tag() string                  { return "{" + s.NS + "tks:sessions}" }
func (s *RedisStore) ownersKey() string            { return s.tag() + ":owners" }
func (s *RedisStore) snapKey(owner string) string  { return s.tag() + ":snap:" + owner }
func (s *RedisStore) leaseKey(owner string) string { return s.tag() + ":lease:" + owner }

func (s *RedisStore) Checkpoint(ctx context.Context, owner string, snap Snapshot, lease time.Duration) error {
	b, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	// Kept past every pause it holds, so a later start still waits it out.
	keep := SavedSessionTTL
	for _, st := range snap.Exits {
		keep = max(keep, time.Until(st.PausedUntil)+time.Hour)
	}
	_, err = s.R.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, s.snapKey(owner), b, keep)
		p.Set(ctx, s.leaseKey(owner), 1, lease)
		p.SAdd(ctx, s.ownersKey(), owner)
		p.Expire(ctx, s.ownersKey(), keep)
		return nil
	})
	return err
}

func (s *RedisStore) Release(ctx context.Context, owner string) error {
	return s.R.Del(ctx, s.leaseKey(owner)).Err()
}

func (s *RedisStore) TakeOrphans(ctx context.Context, self string) ([]Snapshot, error) {
	owners, err := s.R.SMembers(ctx, s.ownersKey()).Result()
	if err != nil {
		return nil, err
	}
	var out []Snapshot
	for _, o := range owners {
		if o == self {
			continue
		}
		alive, err := s.R.Exists(ctx, s.leaseKey(o)).Result()
		if err != nil {
			return out, err
		}
		if alive > 0 {
			continue // still running: its sessions are in use
		}
		b, err := s.R.GetDel(ctx, s.snapKey(o)).Bytes()
		_ = s.R.SRem(ctx, s.ownersKey(), o).Err()
		switch {
		case errors.Is(err, redis.Nil):
			continue // taken by another crawler, or expired
		case err != nil:
			return out, err
		}
		var snap Snapshot
		if json.Unmarshal(b, &snap) != nil {
			continue
		}
		if time.Since(snap.SavedAt) > SavedSessionTTL {
			snap.Sessions = nil // its tokens have likely expired; its pauses still count
		}
		out = append(out, snap)
	}
	return out, nil
}
