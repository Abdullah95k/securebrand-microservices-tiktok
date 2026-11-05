package discovery

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DB is what the store needs of a pool or a transaction.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Keyword is a search term.
type Keyword struct {
	ID             int64      `json:"id"`
	Term           string     `json:"term"`
	Normalized     string     `json:"normalized"`
	Language       string     `json:"language"`
	Group          string     `json:"group,omitempty"`
	Enabled        bool       `json:"enabled"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	LastSearchedAt *time.Time `json:"last_searched_at,omitempty"`
}

// ErrInvalid is a keyword the store refuses; ErrDuplicate one whose
// spelling folds to a keyword of the same language already listed.
var (
	ErrInvalid   = errors.New("discovery: invalid keyword")
	ErrDuplicate = errors.New("discovery: a keyword with this spelling already exists in this language")
	ErrNotFound  = errors.New("discovery: no such keyword")
)

const keywordColumns = `id, term, normalized, language, grp, enabled, created_at, updated_at, last_searched_at`

func scanKeyword(row pgx.Row) (Keyword, error) {
	var k Keyword
	err := row.Scan(&k.ID, &k.Term, &k.Normalized, &k.Language, &k.Group, &k.Enabled, &k.CreatedAt, &k.UpdatedAt, &k.LastSearchedAt)
	return k, err
}

// maxTermLen bounds a keyword: a search term, not a sentence.
const maxTermLen = 100

func checkTerm(term, lang string) (string, error) {
	term = strings.TrimSpace(term)
	if !Languages[lang] {
		return "", fmt.Errorf("%w: language %q must be en, ar, ckb or kmr", ErrInvalid, lang)
	}
	n := Normalize(term)
	if n == "" || len([]rune(term)) > maxTermLen {
		return "", fmt.Errorf("%w: term must be 1 to %d characters", ErrInvalid, maxTermLen)
	}
	return n, nil
}

// AddKeyword adds a keyword. A spelling that folds to a keyword already
// listed in the language returns that keyword, created false, unchanged.
func AddKeyword(ctx context.Context, db DB, term, lang, group string, enabled bool) (Keyword, bool, error) {
	term = strings.TrimSpace(term)
	n, err := checkTerm(term, lang)
	if err != nil {
		return Keyword{}, false, err
	}
	k, err := scanKeyword(db.QueryRow(ctx, `INSERT INTO discovery_keywords (term, normalized, language, grp, enabled)
		VALUES ($1,$2,$3,$4,$5) ON CONFLICT (normalized, language) DO NOTHING RETURNING `+keywordColumns,
		term, n, lang, strings.TrimSpace(group), enabled))
	if err == nil {
		return k, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Keyword{}, false, err
	}
	k, err = scanKeyword(db.QueryRow(ctx, `SELECT `+keywordColumns+` FROM discovery_keywords WHERE normalized = $1 AND language = $2`, n, lang))
	return k, false, err
}

// KeywordFilter narrows ListKeywords.
type KeywordFilter struct {
	Language string
	Group    string
	Enabled  *bool
	AfterID  int64
	Limit    int
}

// ListKeywords lists keywords by id.
func ListKeywords(ctx context.Context, db DB, f KeywordFilter) ([]Keyword, error) {
	q := `SELECT ` + keywordColumns + ` FROM discovery_keywords WHERE id > $1`
	args := []any{f.AfterID}
	if f.Language != "" {
		args = append(args, f.Language)
		q += ` AND language = $` + strconv.Itoa(len(args))
	}
	if f.Group != "" {
		args = append(args, f.Group)
		q += ` AND grp = $` + strconv.Itoa(len(args))
	}
	if f.Enabled != nil {
		args = append(args, *f.Enabled)
		q += ` AND enabled = $` + strconv.Itoa(len(args))
	}
	if f.Limit <= 0 {
		f.Limit = 1000
	}
	args = append(args, f.Limit)
	q += ` ORDER BY id LIMIT $` + strconv.Itoa(len(args))
	rows, err := db.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Keyword, error) { return scanKeyword(r) })
}

// KeywordUpdate changes a keyword; nil fields stay.
type KeywordUpdate struct {
	Term    *string
	Group   *string
	Enabled *bool
}

// UpdateKeyword applies u to keyword id.
func UpdateKeyword(ctx context.Context, db DB, id int64, u KeywordUpdate) (Keyword, error) {
	var term, norm *string
	if u.Term != nil {
		var lang string
		if err := db.QueryRow(ctx, `SELECT language FROM discovery_keywords WHERE id = $1`, id).Scan(&lang); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return Keyword{}, ErrNotFound
			}
			return Keyword{}, err
		}
		t := strings.TrimSpace(*u.Term)
		n, err := checkTerm(t, lang)
		if err != nil {
			return Keyword{}, err
		}
		term, norm = &t, &n
	}
	var group *string
	if u.Group != nil {
		g := strings.TrimSpace(*u.Group)
		group = &g
	}
	k, err := scanKeyword(db.QueryRow(ctx, `UPDATE discovery_keywords SET term = COALESCE($2, term), normalized = COALESCE($3, normalized),
		grp = COALESCE($4, grp), enabled = COALESCE($5, enabled), updated_at = now() WHERE id = $1 RETURNING `+keywordColumns,
		id, term, norm, group, u.Enabled))
	var pe *pgconn.PgError
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return Keyword{}, ErrNotFound
	case errors.As(err, &pe) && pe.Code == "23505":
		return Keyword{}, ErrDuplicate
	}
	return k, err
}

// DeleteKeyword removes keyword id. The accounts it found stay, with
// their sources.
func DeleteKeyword(ctx context.Context, db DB, id int64) error {
	tag, err := db.Exec(ctx, `DELETE FROM discovery_keywords WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ImportSeed adds the seed's keywords that are not listed yet. Keywords
// already listed keep their state: a keyword someone disabled stays so.
func ImportSeed(ctx context.Context, db DB, seed []SeedKeyword) (added int, err error) {
	for _, s := range seed {
		_, created, err := AddKeyword(ctx, db, s.Term, s.Language, s.Group, s.Enabled)
		if err != nil {
			return added, fmt.Errorf("%s %q: %w", s.Language, s.Term, err)
		}
		if created {
			added++
		}
	}
	return added, nil
}
