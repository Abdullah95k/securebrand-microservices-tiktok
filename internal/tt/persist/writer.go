// Package persist writes tks's records (securebrand.tt.v1) to schema tt
// (docs/tiktok ADR 0007 §4): batched upserts by observation time, so a
// redelivered or replayed record changes nothing, metric rows only when a
// post's counts changed, and every write checked against the suppression
// table under the erasure lock (gdpr.GuardAccounts).
//
// It is internal/persist's Writer for TikTok's tables; the consumer that
// feeds it (batching, bisection, the quarantine topic) is internal/persist's,
// through the Sink in this package.
package persist

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/gdpr"
	sharedpersist "github.com/abdullah95k/securebrand-microservices-tiktok/internal/persist"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/store"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

// accountSubject is schema tt's suppression subject type for an account,
// keyed by its user id (ADR 0007 §6).
const accountSubject = "account"

// Writer performs batched upserts into schema tt. DB must be a pool pinned
// to it (ttstore.Open).
type Writer struct {
	DB *pgxpool.Pool
	// CommentsRetention (privacy.retention.comments_days): comments of posts
	// older than this are not written, since retention would drop their
	// partition again. Zero keeps everything.
	CommentsRetention time.Duration
}

// guard takes the erasure lock in tx and returns which of the given
// authors and accounts are erased.
func guard(ctx context.Context, tx pgx.Tx, keys [][]byte, userIDs []int64) (*gdpr.Suppression, error) {
	return gdpr.GuardAccounts(ctx, tx, keys, userIDs, accountSubject)
}

func observed(o *commonv1.Observation) time.Time {
	if o == nil || o.ObservedAt == nil {
		return time.Now().UTC()
	}
	return o.ObservedAt.AsTime()
}

func hashOf(o *commonv1.Observation) []byte {
	if o == nil {
		return nil
	}
	return o.ContentHash
}

// latest keeps the most recently observed message per key, without NUL
// characters, which Postgres cannot store.
func latest[T interface {
	GetObs() *commonv1.Observation
	proto.Message
}](xs []T, key func(T) string) []T {
	idx := map[string]int{}
	out := make([]T, 0, len(xs))
	for _, x := range xs {
		sharedpersist.StripNUL(x.ProtoReflect())
		k := key(x)
		if i, ok := idx[k]; ok {
			if !observed(x.GetObs()).Before(observed(out[i].GetObs())) {
				out[i] = x
			}
			continue
		}
		idx[k] = len(out)
		out = append(out, x)
	}
	return out
}

func suppressed(entity string, n int) {
	if n > 0 {
		metrics.PersistRows.WithLabelValues(entity, "suppressed").Add(float64(n))
	}
}

func written(entity string, n int64, start time.Time) {
	metrics.PersistRows.WithLabelValues(entity, "written").Add(float64(n))
	metrics.PersistBatch.WithLabelValues(entity).Observe(time.Since(start).Seconds())
}

// withPartitions runs fn and, if a partition of table was missing, creates
// the ones holding times and runs it once more. Posts and their comments
// share their month.
func (w *Writer) withPartitions(ctx context.Context, table string, times []time.Time, fn func() error) error {
	err := fn()
	if !store.IsNoPartition(err) {
		return err
	}
	for _, t := range times {
		tables := []string{table}
		if table == "posts" {
			tables = append(tables, "comments")
		}
		for _, tb := range tables {
			if perr := ttstore.Tables.EnsureFor(ctx, w.DB, tb, t); perr != nil {
				return errors.Join(err, perr)
			}
		}
	}
	return fn()
}

func copyTemp(ctx context.Context, tx pgx.Tx, tmp, like string, cols []string, rows [][]any) error {
	if _, err := tx.Exec(ctx, fmt.Sprintf("CREATE TEMP TABLE %s ON COMMIT DROP AS SELECT %s FROM %s WITH NO DATA",
		tmp, strings.Join(cols, ","), like)); err != nil {
		return err
	}
	_, err := tx.CopyFrom(ctx, pgx.Identifier{tmp}, cols, pgx.CopyFromRows(rows))
	return err
}

// optionalInsert runs an insert whose missing partition must not fail the
// batch (daily metrics and monthly snapshots are created ahead by the
// scheduler), in a savepoint so the transaction survives it.
func optionalInsert(ctx context.Context, tx pgx.Tx, q string, args ...any) (int64, error) {
	sp, err := tx.Begin(ctx)
	if err != nil {
		return 0, err
	}
	tag, err := sp.Exec(ctx, q, args...)
	if err != nil {
		_ = sp.Rollback(ctx)
		if store.IsNoPartition(err) {
			return 0, nil
		}
		return 0, err
	}
	return tag.RowsAffected(), sp.Commit(ctx)
}

// set returns "c = EXCLUDED.c" for every column but skip's, then extra.
func set(cols []string, skip map[string]string, extra ...string) string {
	out := make([]string, 0, len(cols)+len(extra))
	for _, c := range cols {
		if v, ok := skip[c]; ok {
			if v != "" {
				out = append(out, c+" = "+v)
			}
			continue
		}
		out = append(out, c+" = EXCLUDED."+c)
	}
	return strings.Join(append(out, extra...), ", ")
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// mediaUUID is a media id the crawler made (a UUIDv5), or nil.
func mediaUUID(s string) *string {
	if s == "" || uuid.Validate(s) != nil {
		return nil
	}
	return &s
}

var pj = protojson.MarshalOptions{UseProtoNames: true}

func protoJSON(m proto.Message) []byte {
	if m == nil || !m.ProtoReflect().IsValid() {
		return nil
	}
	b, err := pj.Marshal(m)
	if err != nil {
		return nil
	}
	return sharedpersist.JSONBSafe(b)
}

func rangesJSON(rs []*ttv1.TextRange) []byte {
	if len(rs) == 0 {
		return nil
	}
	parts := make([]json.RawMessage, 0, len(rs))
	for _, r := range rs {
		if b := protoJSON(r); b != nil {
			parts = append(parts, b)
		}
	}
	b, _ := json.Marshal(parts)
	return b
}

func enumName[E interface{ String() string }](e E, prefix string) string {
	return strings.ToLower(strings.TrimPrefix(e.String(), prefix))
}

// Accounts upserts accounts with their username history and, when they
// changed, a snapshot. Exact counts and the bio link come only from the
// profile page (T31): an account read from a list (counts rounded) keeps
// the page's, unless no page was ever read; a list read without a bio
// keeps the stored one (a removed bio shows at the next page read). A
// status-only record (private, unavailable, banned) updates the status. An erased account stays erased.
//
// Every record of a batch is applied, oldest first, not only the newest
// per account: a check reads the profile page and then the list, whose
// author is newer but carries neither exact counts nor the bio link. Both
// come on the account's partition, in the order they were read.
func (w *Writer) Accounts(ctx context.Context, as []*ttv1.Account) error {
	as = slices.Clone(as)
	for _, a := range as {
		sharedpersist.StripNUL(a.ProtoReflect())
	}
	slices.SortStableFunc(as, func(x, y *ttv1.Account) int { return observed(x.Obs).Compare(observed(y.Obs)) })
	start := time.Now()
	var n int64
	for _, a := range as {
		if a.UserId == 0 {
			metrics.PersistRows.WithLabelValues("account", "rejected").Inc()
			continue
		}
		err := pgx.BeginFunc(ctx, w.DB, func(tx pgx.Tx) error {
			sup, err := guard(ctx, tx, nil, []int64{a.UserId})
			if err != nil {
				return err
			}
			if sup.PageSuppressed(a.UserId) {
				suppressed("account", 1)
				return nil
			}
			n++
			return writeAccount(ctx, tx, a)
		})
		if err != nil {
			return fmt.Errorf("persist: account %d: %w", a.UserId, err)
		}
	}
	written("account", n, start)
	return nil
}

func accountStatus(s ttv1.AccountStatus) string {
	if s == ttv1.AccountStatus_ACCOUNT_STATUS_UNSPECIFIED {
		return "active"
	}
	return enumName(s, "ACCOUNT_STATUS_")
}

func writeAccount(ctx context.Context, tx pgx.Tx, a *ttv1.Account) error {
	seen := observed(a.Obs)
	status := accountStatus(a.Status)
	if status != "active" {
		tag, err := tx.Exec(ctx, `UPDATE accounts SET status = $2, last_seen_at = $3
			WHERE user_id = $1 AND status <> 'suppressed' AND last_seen_at <= $3`, a.UserId, status, seen)
		if err != nil || tag.RowsAffected() > 0 || a.SecUid == "" {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO accounts (user_id, sec_uid, username, author_key, status, first_seen_at, last_seen_at)
			VALUES ($1, $2, $3, $4, $5, $6, $6) ON CONFLICT DO NOTHING`, a.UserId, a.SecUid, nilIfEmpty(a.Username), a.AuthorKey, status, seen)
		return err
	}
	if a.SecUid == "" || len(a.AuthorKey) == 0 {
		// An account row is keyed by both; the parsers never publish one
		// without them.
		metrics.PersistRows.WithLabelValues("account", "rejected").Inc()
		return nil
	}
	page := a.Source == ttv1.AccountSource_ACCOUNT_SOURCE_PROFILE_PAGE
	full := page || a.Source == ttv1.AccountSource_ACCOUNT_SOURCE_USER_DETAIL
	var refreshed, pageRead *time.Time
	if full {
		refreshed = &seen
	}
	if page {
		pageRead = &seen
	}
	c := a.GetCounts()
	if c == nil {
		c = &ttv1.AccountCounts{} // no counts: the stored ones stay
	}
	exact := c.Exact
	// Counts: exact ones always; rounded ones only while no page was read.
	counts := func(col string) string {
		return fmt.Sprintf("CASE WHEN $29 OR t.page_read_at IS NULL THEN COALESCE(EXCLUDED.%[1]s, t.%[1]s) ELSE t.%[1]s END", col)
	}
	var changed bool
	err := tx.QueryRow(ctx, `
		INSERT INTO accounts AS t (user_id, sec_uid, username, nickname, bio, bio_link, bio_link_risk, avatar_url, avatar_media_id,
			verified, private, organization, language, created_at, username_modified_at, nickname_modified_at,
			follower_count, following_count, heart_count, video_count, likes_given_count, friend_count,
			author_key, status, content_hash, first_seen_at, last_seen_at, profile_refreshed_at, page_read_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22,
			$23, 'active', $24, $25, $25, $26, $27)
		ON CONFLICT (user_id) DO UPDATE SET
			sec_uid = EXCLUDED.sec_uid, username = COALESCE(EXCLUDED.username, t.username),
			nickname = COALESCE(EXCLUDED.nickname, t.nickname),
			bio = CASE WHEN $28 OR EXCLUDED.bio <> '' THEN EXCLUDED.bio ELSE t.bio END,
			bio_link = CASE WHEN $28 THEN EXCLUDED.bio_link ELSE t.bio_link END,
			bio_link_risk = CASE WHEN $28 THEN EXCLUDED.bio_link_risk ELSE t.bio_link_risk END,
			avatar_url = COALESCE(EXCLUDED.avatar_url, t.avatar_url), avatar_media_id = COALESCE(EXCLUDED.avatar_media_id, t.avatar_media_id),
			verified = COALESCE(EXCLUDED.verified, t.verified), private = COALESCE(EXCLUDED.private, t.private),
			organization = COALESCE(EXCLUDED.organization, t.organization), language = COALESCE(EXCLUDED.language, t.language),
			created_at = COALESCE(EXCLUDED.created_at, t.created_at),
			username_modified_at = COALESCE(EXCLUDED.username_modified_at, t.username_modified_at),
			nickname_modified_at = COALESCE(EXCLUDED.nickname_modified_at, t.nickname_modified_at),
			follower_count = `+counts("follower_count")+`, following_count = `+counts("following_count")+`,
			heart_count = `+counts("heart_count")+`, video_count = `+counts("video_count")+`,
			likes_given_count = `+counts("likes_given_count")+`, friend_count = `+counts("friend_count")+`,
			author_key = EXCLUDED.author_key, status = 'active', content_hash = EXCLUDED.content_hash,
			last_seen_at = EXCLUDED.last_seen_at,
			profile_refreshed_at = COALESCE(EXCLUDED.profile_refreshed_at, t.profile_refreshed_at),
			page_read_at = COALESCE(EXCLUDED.page_read_at, t.page_read_at)
		WHERE t.status <> 'suppressed' AND t.last_seen_at <= EXCLUDED.last_seen_at
		RETURNING old.user_id IS NULL
			OR old.username IS DISTINCT FROM new.username OR old.nickname IS DISTINCT FROM new.nickname
			OR old.follower_count IS DISTINCT FROM new.follower_count OR old.following_count IS DISTINCT FROM new.following_count
			OR old.heart_count IS DISTINCT FROM new.heart_count OR old.video_count IS DISTINCT FROM new.video_count
			OR old.verified IS DISTINCT FROM new.verified OR old.private IS DISTINCT FROM new.private`,
		a.UserId, a.SecUid, nilIfEmpty(a.Username), nilIfEmpty(a.Nickname), a.Bio, a.BioLink, a.BioLinkRisk,
		nilIfEmpty(a.AvatarUrl), mediaUUID(a.AvatarMediaId), a.Verified, a.Private, a.Organization, nilIfEmpty(a.Language),
		tsp(a.CreatedAt), tsp(a.UsernameModifiedAt), tsp(a.NicknameModifiedAt),
		c.Followers, c.Following, c.Hearts, c.Videos, c.LikesGiven, c.Friends,
		a.AuthorKey, hashOf(a.Obs), seen, refreshed, pageRead, page, exact).Scan(&changed)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil // erased, or an older observation than the stored one
	}
	if err != nil {
		return err
	}
	if a.Username != "" {
		if _, err := tx.Exec(ctx, `INSERT INTO account_usernames (user_id, username, first_seen_at, last_seen_at)
			VALUES ($1, $2, $3, $3) ON CONFLICT (user_id, username) DO UPDATE SET
				first_seen_at = LEAST(account_usernames.first_seen_at, EXCLUDED.first_seen_at),
				last_seen_at = GREATEST(account_usernames.last_seen_at, EXCLUDED.last_seen_at)`, a.UserId, a.Username, seen); err != nil {
			return err
		}
	}
	if !changed {
		return nil
	}
	_, err = optionalInsert(ctx, tx, `INSERT INTO account_snapshots (user_id, captured_at, username, nickname, follower_count,
			following_count, heart_count, video_count, verified, private)
		SELECT user_id, $2, username, nickname, follower_count, following_count, heart_count, video_count, verified, private
		FROM accounts WHERE user_id = $1 ON CONFLICT DO NOTHING`, a.UserId, seen)
	return err
}

var musicCols = []string{"music_id", "title", "author_name", "original", "duration_ms", "album", "cover_media_id",
	"copyrighted", "commerce", "content_hash", "first_seen_at", "last_seen_at"}

// Music upserts posts' sounds.
func (w *Writer) Music(ctx context.Context, ms []*ttv1.Music) error {
	ms = latest(ms, func(m *ttv1.Music) string { return strconv.FormatInt(m.MusicId, 10) })
	rows := make([][]any, 0, len(ms))
	for _, m := range ms {
		if m.MusicId == 0 {
			continue
		}
		seen := observed(m.Obs)
		rows = append(rows, []any{m.MusicId, nilIfEmpty(m.Title), nilIfEmpty(m.AuthorName), m.Original, m.DurationMs,
			nilIfEmpty(m.Album), mediaUUID(m.CoverMediaId), m.Copyrighted, m.Commerce, hashOf(m.Obs), seen, seen})
	}
	if len(rows) == 0 {
		return nil
	}
	start := time.Now()
	var n int64
	err := pgx.BeginFunc(ctx, w.DB, func(tx pgx.Tx) error {
		if err := copyTemp(ctx, tx, "tmp_music", "music", musicCols, rows); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `INSERT INTO music AS t (`+strings.Join(musicCols, ",")+`)
			SELECT `+strings.Join(musicCols, ",")+` FROM tmp_music
			ON CONFLICT (music_id) DO UPDATE SET `+set(musicCols, map[string]string{"music_id": "", "first_seen_at": ""})+`
			WHERE t.last_seen_at <= EXCLUDED.last_seen_at
			  AND (t.content_hash IS DISTINCT FROM EXCLUDED.content_hash OR t.last_seen_at < EXCLUDED.last_seen_at - interval '1 day')`)
		n = tag.RowsAffected()
		return err
	})
	if err != nil {
		return fmt.Errorf("persist: music: %w", err)
	}
	written("music", n, start)
	return nil
}

var mediaCols = []string{"media_id", "source_media_id", "kind", "variant", "user_id", "post_id", "music_id", "comment_id",
	"position", "author_key", "source_url", "source_expires_at", "width", "height", "duration_ms", "bitrate", "mime",
	"post_created_at", "discovered_at"}

// Media upserts media metadata without touching download state; media of
// an erased account or author are dropped.
func (w *Writer) Media(ctx context.Context, ms []*ttv1.Media) error {
	ms = latest(ms, func(m *ttv1.Media) string { return m.MediaId })
	if len(ms) == 0 {
		return nil
	}
	start := time.Now()
	ids := make([]int64, len(ms))
	keys := make([][]byte, len(ms))
	for i, m := range ms {
		ids[i], keys[i] = m.UserId, m.AuthorKey
	}
	var n int64
	var dropped int
	err := pgx.BeginFunc(ctx, w.DB, func(tx pgx.Tx) error {
		sup, err := guard(ctx, tx, keys, ids)
		if err != nil {
			return err
		}
		n, dropped = 0, 0
		rows := make([][]any, 0, len(ms))
		for _, m := range ms {
			if mediaUUID(m.MediaId) == nil {
				metrics.PersistRows.WithLabelValues("media", "rejected").Inc()
				continue
			}
			if sup.PageSuppressed(m.UserId) || sup.AuthorSuppressed(m.AuthorKey) {
				dropped++
				continue
			}
			var userID *int64
			if m.UserId != 0 {
				userID = &m.UserId
			}
			var authorKey []byte
			if len(m.AuthorKey) > 0 {
				authorKey = m.AuthorKey
			}
			rows = append(rows, []any{m.MediaId, m.MediaId, enumName(m.Kind, "MEDIA_KIND_"), m.Variant, userID, m.PostId,
				m.MusicId, m.CommentId, m.Position, authorKey, nilIfEmpty(m.SourceUrl), tsp(m.SourceExpiresAt), nilIfZero32(m.Width),
				nilIfZero32(m.Height), nilIfZero(m.DurationMs), nilIfZero(m.Bitrate), nilIfEmpty(m.Mime), tsp(m.PostCreatedAt), observed(m.Obs)})
		}
		if len(rows) == 0 {
			return nil
		}
		if err := copyTemp(ctx, tx, "tmp_media", "media", mediaCols, rows); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `INSERT INTO media AS t (`+strings.Join(mediaCols, ",")+`)
			SELECT `+strings.Join(mediaCols, ",")+` FROM tmp_media
			ON CONFLICT (media_id) DO UPDATE SET source_url = EXCLUDED.source_url,
				source_expires_at = EXCLUDED.source_expires_at, width = COALESCE(EXCLUDED.width, t.width),
				height = COALESCE(EXCLUDED.height, t.height), duration_ms = COALESCE(EXCLUDED.duration_ms, t.duration_ms),
				bitrate = COALESCE(EXCLUDED.bitrate, t.bitrate), mime = COALESCE(EXCLUDED.mime, t.mime)
			WHERE t.source_url IS DISTINCT FROM EXCLUDED.source_url AND t.status NOT IN ('deleted', 'expired')`)
		n = tag.RowsAffected()
		return err
	})
	if err != nil {
		return fmt.Errorf("persist: media: %w", err)
	}
	written("media", n, start)
	suppressed("media", dropped)
	return nil
}

func tsp(t *timestamppb.Timestamp) *time.Time {
	if t == nil {
		return nil
	}
	v := t.AsTime()
	return &v
}

func nilIfZero(v int64) *int64 {
	if v == 0 {
		return nil
	}
	return &v
}

func nilIfZero32(v int32) *int32 {
	if v == 0 {
		return nil
	}
	return &v
}

// execer runs a statement: a pool or a transaction.
type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}
