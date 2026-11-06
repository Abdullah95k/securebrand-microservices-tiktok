package persist

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"

	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	sharedpersist "github.com/abdullah95k/securebrand-microservices-tiktok/internal/persist"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/discovery"
)

// Discovery part D (docs/tiktok/discovery-plan.md §9; ADR 0007 §23): the
// people on monitored accounts' following and followers lists. Every link
// read is kept (first and last seen); a link new to its list, in the same
// transaction, records a sighting for the leader to count, as part B's
// rows do, so a re-read or a redelivered batch records nothing again.

// followDirection is follows.direction for a record's direction, "" for
// none.
func followDirection(d ttv1.FollowDirection) string {
	switch d {
	case ttv1.FollowDirection_FOLLOW_DIRECTION_FOLLOWING:
		return discovery.SourceFollowing
	case ttv1.FollowDirection_FOLLOW_DIRECTION_FOLLOWER:
		return discovery.SourceFollower
	}
	return ""
}

// Follows upserts follow links and the people on them.
func (w *Writer) Follows(ctx context.Context, fs []*ttv1.Follow) error {
	if len(fs) == 0 {
		return nil
	}
	start := time.Now()
	// One row per link, the newest observation's.
	type key struct {
		user   int64
		dir    string
		person int64
	}
	latest := map[key]*ttv1.Follow{}
	var order []key
	for _, f := range fs {
		sharedpersist.StripNUL(f.ProtoReflect())
		dir := followDirection(f.Direction)
		p := f.GetPerson()
		if f.UserId == 0 || dir == "" || p.GetUserId() == 0 || len(p.GetAuthorKey()) == 0 || p.GetUserId() == f.UserId {
			metrics.PersistRows.WithLabelValues("follow", "rejected").Inc()
			continue
		}
		k := key{f.UserId, dir, p.GetUserId()}
		prev, ok := latest[k]
		if !ok {
			order = append(order, k)
		}
		if !ok || !observed(f.Obs).Before(observed(prev.Obs)) {
			latest[k] = f
		}
	}
	if len(order) == 0 {
		return nil
	}
	n := len(order)
	users, persons := make([]int64, 0, n), make([]int64, 0, n)
	dirs := make([]string, 0, n)
	keys := make([][]byte, 0, n)
	private := make([]*bool, 0, n)
	videos, followers, following := make([]*int64, 0, n), make([]*int64, 0, n), make([]*int64, 0, n)
	at := make([]time.Time, 0, n)
	jobs := make([]*string, 0, n)
	people := map[string][]any{}
	byKey := make(map[key]*ttv1.Follow, n)
	for _, k := range order {
		f := latest[k]
		p := f.Person
		seen := observed(f.Obs)
		users, dirs, persons, keys = append(users, k.user), append(dirs, k.dir), append(persons, k.person), append(keys, p.AuthorKey)
		private, videos = append(private, f.Private), append(videos, f.VideoCount)
		followers, following = append(followers, f.FollowerCount), append(following, f.FollowingCount)
		at = append(at, seen)
		var job *string
		if ids := f.GetObs().GetJobIds(); len(ids) > 0 && mediaUUID(ids[0]) != nil {
			job = &ids[0]
		}
		jobs = append(jobs, job)
		byKey[k] = f
		if prev, ok := people[string(p.AuthorKey)]; !ok || !prev[9].(time.Time).After(seen) {
			people[string(p.AuthorKey)] = []any{p.AuthorKey, p.UserId, nilIfEmpty(p.SecUid), nilIfEmpty(p.Username),
				nilIfEmpty(p.Nickname), nilIfEmpty(p.AvatarUrl), p.Verified, nilIfEmpty(p.PredictedAgeGroup), seen, seen}
		}
	}
	got := sightingCounts{}
	links := map[[2]string]int64{} // direction, new|seen
	err := pgx.BeginFunc(ctx, w.DB, func(tx pgx.Tx) error {
		clear(got)
		clear(links)
		rows, err := tx.Query(ctx, `INSERT INTO follows AS t (user_id, direction, person_user_id, author_key, person_private,
				person_videos, person_followers, person_following, first_seen_at, last_seen_at, first_job_id, last_job_id)
			SELECT d.user_id, d.direction, d.person, d.author_key, d.private, d.videos, d.followers, d.following, d.at, d.at,
				d.job::uuid, d.job::uuid
			FROM unnest($1::bigint[], $2::text[], $3::bigint[], $4::bytea[], $5::boolean[], $6::bigint[], $7::bigint[],
				$8::bigint[], $9::timestamptz[], $10::text[])
				AS d(user_id, direction, person, author_key, private, videos, followers, following, at, job)
			ON CONFLICT (user_id, direction, person_user_id) DO UPDATE SET
				first_seen_at = LEAST(t.first_seen_at, EXCLUDED.first_seen_at),
				last_seen_at = GREATEST(t.last_seen_at, EXCLUDED.last_seen_at),
				last_job_id = CASE WHEN EXCLUDED.last_seen_at >= t.last_seen_at THEN COALESCE(EXCLUDED.last_job_id, t.last_job_id) ELSE t.last_job_id END,
				author_key = EXCLUDED.author_key,
				person_private = CASE WHEN EXCLUDED.last_seen_at >= t.last_seen_at THEN COALESCE(EXCLUDED.person_private, t.person_private) ELSE t.person_private END,
				person_videos = CASE WHEN EXCLUDED.last_seen_at >= t.last_seen_at THEN COALESCE(EXCLUDED.person_videos, t.person_videos) ELSE t.person_videos END,
				person_followers = CASE WHEN EXCLUDED.last_seen_at >= t.last_seen_at THEN COALESCE(EXCLUDED.person_followers, t.person_followers) ELSE t.person_followers END,
				person_following = CASE WHEN EXCLUDED.last_seen_at >= t.last_seen_at THEN COALESCE(EXCLUDED.person_following, t.person_following) ELSE t.person_following END
			RETURNING new.user_id, new.direction, new.person_user_id, old.user_id IS NULL`,
			users, dirs, persons, keys, private, videos, followers, following, at, jobs)
		if err != nil {
			return err
		}
		var news []sighting
		for rows.Next() {
			var k key
			var inserted bool
			if err := rows.Scan(&k.user, &k.dir, &k.person, &inserted); err != nil {
				rows.Close()
				return err
			}
			if !inserted {
				links[[2]string{k.dir, "seen"}]++
				continue
			}
			links[[2]string{k.dir, "new"}]++
			f := byKey[k]
			if !w.on(k.dir) {
				continue
			}
			in := discovery.InFollowers
			if k.dir == discovery.SourceFollowing {
				in = discovery.InFollowing
			}
			if s, ok := personSighting(k.dir, in, k.user, f.Person, observed(f.Obs)); ok {
				s.private, s.videos = f.Private, f.VideoCount
				news = append(news, s)
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if err := writePeople(ctx, tx, people); err != nil {
			return err
		}
		// A stable order: the leader counts sightings by id.
		slices.SortStableFunc(news, func(a, b sighting) int { return a.at.Compare(b.at) })
		return recordSightings(ctx, tx, news, got)
	})
	if err != nil {
		return fmt.Errorf("persist: follows: %w", err)
	}
	got.publish()
	var fresh, again int64
	for k, v := range links {
		metrics.FollowLinks.WithLabelValues(k[0], k[1]).Add(float64(v))
		if k[1] == "new" {
			fresh += v
		} else {
			again += v
		}
	}
	written("follow", fresh, start)
	metrics.PersistRows.WithLabelValues("follow", "unchanged").Add(float64(again))
	return nil
}
