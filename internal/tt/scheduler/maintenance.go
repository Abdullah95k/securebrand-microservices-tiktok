package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/store"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

// Maintenance keeps schema tt's partitions ahead of time and applies
// retention (privacy.retention; ADR 0007 §4, §6): comment and metrics
// partitions are dropped whole; finished jobs, media rows never handed
// over, inactive people and old canary runs are deleted in batches.
type Maintenance struct {
	DB  *pgxpool.Pool
	Cfg *config.Config
	Now func() time.Time
}

// Tick runs every step whatever the others did: a partition that can't be
// created now (its table locked by a long reader) must not hold up
// retention, which is a privacy obligation.
func (m *Maintenance) Tick(ctx context.Context) error {
	var errs []error
	now := time.Now().UTC()
	if m.Now != nil {
		now = m.Now().UTC()
	}
	ret := m.Cfg.Privacy.Retention
	// Posts go back as far as a backfill reads (or an account's own window,
	// at most the cap), and one month more for the month in progress.
	monthsBack := max(m.Cfg.TT.Freshness.BackfillDays, m.Cfg.Limits.MaxAgeCapDays)/30 + 1
	// Partitions retention would drop again are not created.
	notBefore := map[string]time.Time{}
	if ret.CommentsDays > 0 {
		notBefore["comments"] = now.AddDate(0, 0, -ret.CommentsDays)
	}
	if ret.MetricsDays > 0 {
		notBefore["post_metrics"] = now.AddDate(0, 0, -ret.MetricsDays)
		notBefore["account_snapshots"] = now.AddDate(0, 0, -ret.MetricsDays)
	}
	n, err := ttstore.EnsurePartitions(ctx, m.DB, now, store.PartitionWindow{
		MonthsBack: monthsBack, MonthsAhead: m.Cfg.Scheduler.PartitionMonthsAhead, DaysBack: 1, DaysAhead: 14, NotBefore: notBefore})
	if err != nil {
		errs = append(errs, err)
	}
	if n > 0 {
		slog.Info("partitions created", "count", n)
	}
	// Comments are kept as long as a backfill reaches back (365 days); the
	// metrics history and the daily account snapshots as long as metrics.
	drops := []struct {
		table string
		days  int
	}{{"comments", ret.CommentsDays}, {"post_metrics", ret.MetricsDays}, {"account_snapshots", ret.MetricsDays}}
	for _, d := range drops {
		if d.days <= 0 {
			continue
		}
		dropped, err := store.DropPartitionsBefore(ctx, m.DB, d.table, now.AddDate(0, 0, -d.days))
		if err != nil {
			errs = append(errs, err)
		}
		if len(dropped) > 0 {
			slog.Info("retention: partitions dropped", "table", d.table, "partitions", dropped)
		}
	}
	stmts := []struct {
		q    string
		days int
	}{
		{`DELETE FROM jobs WHERE job_id IN (SELECT job_id FROM jobs WHERE finished_at < $2::timestamptz - make_interval(days => $1) LIMIT 5000)`, ret.JobsDays},
		// Rows never handed over (variants the policy leaves out, media of
		// jobs without media, failed downloads): their signed links expired
		// long ago.
		{`DELETE FROM media WHERE media_id IN (SELECT media_id FROM media WHERE status IN ('discovered','failed','skipped','source_expired')
			AND discovered_at < $2::timestamptz - make_interval(days => $1) LIMIT 5000)`, ret.MediaRowsDays},
		// Rows of media handed over stay, as the record that it was, but
		// without what ties them to a person or its content. An empty
		// source_media_id marks a row done (media_unscrubbed_idx).
		{`UPDATE media SET source_media_id = '', source_url = NULL, comment_id = NULL, author_key = NULL, sha256 = NULL,
			s3_key = NULL, last_error = NULL
			WHERE media_id IN (SELECT media_id FROM media WHERE status IN ('deleted','expired') AND source_media_id <> ''
				AND discovered_at < $2::timestamptz - make_interval(days => $1) LIMIT 5000)`, ret.MediaRowsDays},
		// Comment authors and mentioned users not seen for a while.
		{`DELETE FROM people WHERE author_key IN (SELECT author_key FROM people
			WHERE last_seen_at < $2::timestamptz - make_interval(days => $1) LIMIT 5000)`, ret.AuthorsInactiveDays},
		{`DELETE FROM canary_runs WHERE started_at < $2::timestamptz - make_interval(days => $1)`, ret.JobsDays},
	}
	for _, s := range stmts {
		if s.days <= 0 {
			continue
		}
		if _, err := m.DB.Exec(ctx, s.q, s.days, now); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
