// Package gdpr implements data-subject protections: pseudonymous author
// keys, the suppression set crawlers consult before publishing, and the
// erasure workflow.
package gdpr

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	fbv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/fb/v1"
)

// AuthorKey is the stable pseudonymous key of a Facebook author id.
func AuthorKey(secret []byte, authorID string) []byte {
	if authorID == "" {
		return nil
	}
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(authorID))
	return m.Sum(nil)
}

// Suppression is the in-memory set of erased authors and pages.
type Suppression struct {
	pool    *pgxpool.Pool
	mu      sync.RWMutex
	authors map[string]bool
	pages   map[int64]bool
}

func NewSuppression(pool *pgxpool.Pool) *Suppression {
	return &Suppression{pool: pool, authors: map[string]bool{}, pages: map[int64]bool{}}
}

// AuthorSuppressed reports whether the author's data must be redacted.
func (s *Suppression) AuthorSuppressed(key []byte) bool {
	if s == nil || len(key) == 0 {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.authors[hex.EncodeToString(key)]
}

// PageSuppressed reports whether a page must not be crawled.
func (s *Suppression) PageSuppressed(id int64) bool {
	if s == nil || id == 0 {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.pages[id]
}

// Load refreshes the set from Postgres.
func (s *Suppression) Load(ctx context.Context) error {
	if s.pool == nil {
		return nil
	}
	rows, err := s.pool.Query(ctx, "SELECT subject_type, subject_key FROM suppression")
	if err != nil {
		return err
	}
	defer rows.Close()
	authors, pages := map[string]bool{}, map[int64]bool{}
	for rows.Next() {
		var typ, k string
		if err := rows.Scan(&typ, &k); err != nil {
			return err
		}
		switch typ {
		case "author":
			authors[k] = true
		case "page":
			if id, err := strconv.ParseInt(k, 10, 64); err == nil {
				pages[id] = true
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.authors, s.pages = authors, pages
	s.mu.Unlock()
	return nil
}

// Add marks a subject suppressed locally (used right after an erasure).
func (s *Suppression) Add(authorKey []byte, pageID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(authorKey) > 0 {
		s.authors[hex.EncodeToString(authorKey)] = true
	}
	if pageID != 0 {
		s.pages[pageID] = true
	}
}

// Run refreshes every interval until ctx is done.
func (s *Suppression) Run(ctx context.Context, every time.Duration) error {
	if err := s.Load(ctx); err != nil {
		slog.Warn("gdpr: suppression load failed", "err", err)
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := s.Load(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("gdpr: suppression load failed", "err", err)
			}
		}
	}
}

// Redact strips a comment by a suppressed author down to its ids and the
// author key, as an erasure leaves the stored row.
func Redact(c *fbv1.Comment) {
	c.Author = &fbv1.Author{AuthorKey: c.GetAuthor().GetAuthorKey()}
	c.Text, c.TextRanges, c.Attachments, c.Url, c.ExtraJson = "", nil, nil, "", ""
	c.Redacted = true
}

// nonPersonEntities are the text-range entity types that never name a
// person. Any other type (User, and types Facebook may add) is treated as
// a person.
var nonPersonEntities = map[string]bool{"Hashtag": true, "Page": true, "ExternalUrl": true, "Group": true, "Event": true}

// PersonEntity reports whether a text-range entity type may name a person.
func PersonEntity(entityType string) bool { return !nonPersonEntities[entityType] }

// ScrubMentions clears the id, profile URL and name of person mentions in
// text ranges (replies tag the person they answer): what links a mention
// to a Facebook profile. The text is content and stays as written, so the
// name as typed is still in it; the offsets stay too.
func ScrubMentions(rs []*fbv1.TextRange) {
	for _, r := range rs {
		if PersonEntity(r.EntityType) {
			r.EntityId, r.Url, r.Name = "", "", ""
		}
	}
}

// MentionedKeys returns the author keys (AuthorKey with key) of the people
// rs mention, in order, nil for a range that names no person.
func MentionedKeys(key []byte, rs []*fbv1.TextRange) [][]byte {
	out := make([][]byte, len(rs))
	for i, r := range rs {
		if r.EntityId != "" && PersonEntity(r.EntityType) {
			out[i] = AuthorKey(key, r.EntityId)
		}
	}
	return out
}

// ScrubSuppressedMentions clears the mentions in rs of suppressed authors;
// keys are rs's MentionedKeys.
func ScrubSuppressedMentions(s *Suppression, rs []*fbv1.TextRange, keys [][]byte) {
	for i, r := range rs {
		if i < len(keys) && s.AuthorSuppressed(keys[i]) {
			ScrubMentions([]*fbv1.TextRange{r})
		}
	}
}

// ScrubMentionsJSON is ScrubMentions for text ranges stored as JSON (proto
// field names). Anything it cannot read comes back as nil.
func ScrubMentionsJSON(raw []byte) []byte {
	if len(raw) == 0 {
		return raw
	}
	var rs []map[string]any
	if err := json.Unmarshal(raw, &rs); err != nil {
		return nil
	}
	for _, r := range rs {
		t, _ := r["entity_type"].(string)
		if PersonEntity(t) {
			delete(r, "entity_id")
			delete(r, "url")
			delete(r, "name")
		}
	}
	out, err := json.Marshal(rs)
	if err != nil {
		return nil
	}
	return out
}
