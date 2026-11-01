package gdpr

import (
	"context"
	"encoding/hex"
	"strconv"

	"github.com/jackc/pgx/v5"
)

// lockKey is the advisory lock that orders writes of crawled data against
// erasures (tks's own number: fbs's and igs's locks are 4_200_* and
// 4_300_*, ADR 0006 §5). An erasure holds it exclusively while it adds its
// suppression entry (and, for an author, redacts the stored comments); the
// persister holds it shared while it checks the suppression table and
// writes. So a write either commits before the erasure, which then removes
// or redacts it, or sees the suppression entry and drops the data. Before
// M2 only the crawlers checked suppression, from a list reloaded every
// minute, so records already in Kafka when an erasure ran wrote erased
// data back (fbs reference, Known issues).
const lockKey int64 = 4_400_002

// Guard takes the shared lock in tx and returns which of the given authors
// and pages are suppressed. Writers of crawled data call it first in the
// transaction that writes the data.
func Guard(ctx context.Context, tx pgx.Tx, authorKeys [][]byte, pageIDs []int64) (*Suppression, error) {
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock_shared($1)", lockKey); err != nil {
		return nil, err
	}
	return Lookup(ctx, tx, authorKeys, pageIDs)
}

// lockExclusive is the erasure's side of Guard.
func lockExclusive(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", lockKey)
	return err
}

// Querier runs a query: a pool or a transaction.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Lookup returns the suppressed subset of the given authors and pages, as a
// Suppression holding only those entries. Without the lock it is a hint:
// an erasure may commit right after it.
func Lookup(ctx context.Context, q Querier, authorKeys [][]byte, pageIDs []int64) (*Suppression, error) {
	s := NewSuppression(nil)
	authors := make([]string, 0, len(authorKeys))
	for _, k := range authorKeys {
		if len(k) > 0 {
			authors = append(authors, hex.EncodeToString(k))
		}
	}
	pages := make([]string, 0, len(pageIDs))
	for _, id := range pageIDs {
		if id != 0 {
			pages = append(pages, strconv.FormatInt(id, 10))
		}
	}
	if len(authors) == 0 && len(pages) == 0 {
		return s, nil
	}
	rows, err := q.Query(ctx, `SELECT subject_type, subject_key FROM suppression
		WHERE (subject_type = 'author' AND subject_key = ANY($1)) OR (subject_type = 'page' AND subject_key = ANY($2))`, authors, pages)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var typ, key string
		if err := rows.Scan(&typ, &key); err != nil {
			return nil, err
		}
		if typ == "author" {
			s.authors[key] = true
		} else if id, err := strconv.ParseInt(key, 10, 64); err == nil {
			s.pages[id] = true
		}
	}
	return s, rows.Err()
}
