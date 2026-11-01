package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	fbv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/api"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakefb"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/pilot"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/redisx"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/topics"
)

type jobView struct {
	ID           string           `json:"job_id"`
	Status       string           `json:"status"`
	Mode         string           `json:"mode"`
	PageID       *int64           `json:"page_id"`
	Counters     map[string]int64 `json:"counters"`
	Completeness struct {
		Posts             int64   `json:"posts"`
		PostsComplete     int64   `json:"posts_complete"`
		MedianRatio       float64 `json:"median_ratio"`
		MinRatio          float64 `json:"min_ratio"`
		CommentsReported  int64   `json:"comments_reported"`
		CommentsCollected int64   `json:"comments_collected"`
	} `json:"completeness"`
}

func decodeBody[T any](t *testing.T, r *http.Response, want int) T {
	t.Helper()
	defer func() { _ = r.Body.Close() }()
	b, _ := io.ReadAll(r.Body)
	if r.StatusCode != want {
		t.Fatalf("status %d (want %d): %s", r.StatusCode, want, b)
	}
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatalf("decode %s: %v", b, err)
	}
	return v
}

func (s *Stack) runJob(t *testing.T, key, page, mode string) jobView {
	t.Helper()
	body := fmt.Sprintf(`{"page":%q,"mode":%q}`, page, mode)
	j := decodeBody[jobView](t, s.Do(t, key, "POST", "/v1/jobs", []byte(body)), http.StatusAccepted)
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		v := decodeBody[jobView](t, s.Do(t, key, "GET", "/v1/jobs/"+j.ID, nil), http.StatusOK)
		switch v.Status {
		case "completed", "partial", "failed", "cancelled":
			return v
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("job %s did not finish", j.ID)
	return jobView{}
}

func count(t *testing.T, s *Stack, q string, args ...any) int {
	t.Helper()
	var n int
	if err := s.DB.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}

func waitFor(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestEndToEnd(t *testing.T) {
	s := Start(t, Options{})
	ctx := context.Background()
	key := s.Key(t, api.ScopeJobsWrite, api.ScopeDataRead, api.ScopePagesWrite)
	piiKey := s.Key(t, api.ScopeDataRead, api.ScopePIIRead)
	adminKey := s.Key(t, api.ScopeAdmin)
	page := s.Fake.World.Pages()[0]

	// 1. Full crawl of a page via the API.
	start := time.Now()
	j := s.runJob(t, key, "https://www.facebook.com/"+page.Vanity, "auto")
	t.Logf("job %s %s in %s: counters=%v completeness=%+v", j.ID, j.Status, time.Since(start).Round(time.Millisecond), j.Counters, j.Completeness)
	if j.Status != "completed" || j.Mode != "full" {
		t.Fatalf("job = %+v", j)
	}
	truth, _ := s.Fake.World.TruthFor(page.ID, time.Time{})
	wantComments := 0
	for _, p := range truth.Posts {
		wantComments += p.TotalComments
	}
	if got := count(t, s, `SELECT count(*) FROM posts WHERE page_id = $1`, page.ID); got != len(truth.Posts) {
		t.Fatalf("posts = %d, want %d", got, len(truth.Posts))
	}
	if got := count(t, s, `SELECT count(*) FROM comments WHERE page_id = $1`, page.ID); got != wantComments {
		t.Fatalf("comments = %d, want %d", got, wantComments)
	}
	if j.Completeness.MedianRatio != 1 || j.Completeness.MinRatio != 1 || j.Completeness.CommentsCollected != int64(wantComments) {
		t.Fatalf("completeness = %+v (want all 1.0, %d comments)", j.Completeness, wantComments)
	}
	if j.Counters["requests"] == 0 || j.Counters["wire_bytes"] == 0 || j.PageID == nil || *j.PageID != page.ID {
		t.Fatalf("counters/page id = %v %v", j.Counters, j.PageID)
	}
	// The pilot's load report reads the same counters from the jobs table.
	if rep, err := pilot.Load(ctx, s.DB, start.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	} else {
		var total pilot.Row
		for _, r := range rep.Rows {
			total.Jobs += r.Jobs
			total.Requests += r.Requests
			total.WireBytes += r.WireBytes
			total.Posts += r.Posts
		}
		if total.Jobs == 0 || total.Requests < j.Counters["requests"] || total.WireBytes < j.Counters["wire_bytes"] || total.Posts == 0 {
			t.Fatalf("load report %+v, job counters %v", rep, j.Counters)
		}
	}
	if n := count(t, s, `SELECT count(*) FROM pages WHERE page_id = $1 AND name = $2 AND followers_count > 0`, page.ID, page.Name); n != 1 {
		t.Fatal("page profile not stored")
	}
	// A job belongs to the client that made it (and admins): others get
	// 404. Before M2 any client could read, cancel and download another's.
	other := s.Key(t, api.ScopeJobsWrite, api.ScopeDataRead)
	for _, c := range []struct{ key, method, path string }{
		{other, "GET", "/v1/jobs/" + j.ID}, {other, "GET", "/v1/jobs/" + j.ID + "/results"}, {other, "POST", "/v1/jobs/" + j.ID + "/cancel"},
	} {
		resp := s.Do(t, c.key, c.method, c.path, nil)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("another client: %s %s = %d", c.method, c.path, resp.StatusCode)
		}
	}
	if v := decodeBody[jobView](t, s.Do(t, adminKey, "GET", "/v1/jobs/"+j.ID, nil), http.StatusOK); v.ID != j.ID {
		t.Fatalf("admin read %+v", v)
	}
	if v := decodeBody[jobView](t, s.Do(t, key, "GET", "/v1/jobs/"+j.ID, nil), http.StatusOK); v.Status != "completed" {
		t.Fatalf("the owner's read %+v", v)
	}
	if n := count(t, s, `SELECT count(*) FROM comments WHERE page_id = $1 AND depth = 2`, page.ID); n == 0 {
		t.Fatal("expected depth-2 replies")
	}

	// 2. The fb.job.v1 event announces completion.
	cl, _ := kgo.NewClient(kgo.SeedBrokers(s.Cfg.Kafka.Brokers...), kgo.ConsumeTopics(s.Producer.Topic(topics.Job)),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	defer cl.Close()
	var ev fbv1.JobEvent
	waitFor(t, "job event", 20*time.Second, func() bool {
		pctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		for _, r := range cl.PollFetches(pctx).Records() {
			if proto.Unmarshal(r.Value, &ev) == nil && ev.JobId == j.ID {
				return true
			}
		}
		return false
	})
	if ev.Status != fbv1.JobStatus_JOB_STATUS_COMPLETED || ev.Completeness.GetCommentsCollected() != int64(wantComments) {
		t.Fatalf("job event = %+v", &ev)
	}

	// 3. API reads; author identity only with pii:read.
	type postsResp struct {
		Posts []api.Post `json:"posts"`
	}
	pr := decodeBody[postsResp](t, s.Do(t, key, "GET", "/v1/pages/"+page.Vanity+"/posts?limit=500", nil), http.StatusOK)
	if len(pr.Posts) != len(truth.Posts) {
		t.Fatalf("api posts = %d", len(pr.Posts))
	}
	var busiest api.Post
	for _, p := range pr.Posts {
		if p.CommentCount != nil && (busiest.CommentCount == nil || *p.CommentCount > *busiest.CommentCount) {
			busiest = p
		}
		if p.CommentsState != "complete" || p.Completeness == nil || *p.Completeness != 1 {
			t.Fatalf("post %d coverage = %s %v", p.PostID, p.CommentsState, p.Completeness)
		}
	}
	type commentsResp struct {
		Comments []api.Comment `json:"comments"`
	}
	path := "/v1/posts/" + strconv.FormatInt(busiest.PostID, 10) + "/comments?depth=0&limit=5"
	plain := decodeBody[commentsResp](t, s.Do(t, key, "GET", path, nil), http.StatusOK)
	withPII := decodeBody[commentsResp](t, s.Do(t, piiKey, "GET", path, nil), http.StatusOK)
	if len(plain.Comments) == 0 || plain.Comments[0].AuthorKey == "" || (plain.Comments[0].Author != nil && plain.Comments[0].Author.Name != nil) {
		t.Fatalf("comments without pii:read must hide names: %+v", plain.Comments)
	}
	if withPII.Comments[0].Author == nil || withPII.Comments[0].Author.Name == nil {
		t.Fatalf("comments with pii:read must include author: %+v", withPII.Comments[0])
	}
	if r := s.Do(t, "fbs_bad_key", "GET", "/v1/jobs", nil); r.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bad key status = %d", r.StatusCode)
	}

	// 4. Media: downloaded to S3, deleted after the analysis service acks.
	var ready int
	waitFor(t, "media ready", 60*time.Second, func() bool {
		ready = count(t, s, `SELECT count(*) FROM media WHERE status = 'ready' AND page_id = $1`, page.ID)
		pending := count(t, s, `SELECT count(*) FROM media m WHERE page_id = $1 AND status = 'discovered'
			AND (kind IN ('photo','comment_photo','sticker','comment_gif') OR variant IN ('sd','thumbnail'))`, page.ID)
		return ready > 0 && pending == 0
	})
	rows, _ := s.DB.Query(ctx, `SELECT media_id::text, s3_key FROM media WHERE status = 'ready'`)
	var ids, keys []string
	for rows.Next() {
		var id, k string
		_ = rows.Scan(&id, &k)
		ids, keys = append(ids, id), append(keys, k)
	}
	rows.Close()
	if _, err := s.S3.StatObject(ctx, s.Cfg.S3.MediaBucket, keys[0], minio.StatObjectOptions{}); err != nil {
		t.Fatalf("object missing: %v", err)
	}
	// TTL sweep: expire one object without an ack.
	if _, err := s.DB.Exec(ctx, `UPDATE media SET delete_after = now() - interval '1 minute' WHERE media_id = $1`, ids[0]); err != nil {
		t.Fatal(err)
	}
	if err := s.Janitor.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	if n := count(t, s, `SELECT count(*) FROM media WHERE media_id = $1 AND status = 'expired'`, ids[0]); n != 1 {
		t.Fatal("TTL sweep did not expire the object")
	}
	b := s.Producer.NewBatch()
	for _, id := range ids[1:] {
		_ = b.AddProto(ctx, topics.MediaProcessd, id, &fbv1.MediaProcessed{EventId: id, MediaId: id, Consumer: "analysis",
			Status: fbv1.ProcessStatus_PROCESS_STATUS_PROCESSED, ProcessedAt: timestamppb.Now()})
	}
	if err := b.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "media deleted after ack", 30*time.Second, func() bool {
		return count(t, s, `SELECT count(*) FROM media WHERE status = 'ready'`) == 0
	})
	for _, k := range keys {
		if _, err := s.S3.StatObject(ctx, s.Cfg.S3.MediaBucket, k, minio.StatObjectOptions{}); err == nil {
			t.Fatalf("object %s still present after ack/TTL", k)
		}
	}
	t.Logf("media: %d objects downloaded, acked and deleted", ready)

	// 5. Incremental re-scrape picks up new posts and comments.
	if err := s.Fake.World.Mutate(page.ID, 1, 7); err != nil {
		t.Fatal(err)
	}
	truth2, _ := s.Fake.World.TruthFor(page.ID, time.Time{})
	want2 := 0
	for _, p := range truth2.Posts {
		want2 += p.TotalComments
	}
	j2 := s.runJob(t, key, page.Vanity, "auto")
	t.Logf("incremental job %s %s: counters=%v", j2.ID, j2.Status, j2.Counters)
	if j2.Mode != "incremental" || j2.Status != "completed" {
		t.Fatalf("incremental job = %+v", j2)
	}
	if got := count(t, s, `SELECT count(*) FROM posts WHERE page_id = $1`, page.ID); got != len(truth2.Posts) {
		t.Fatalf("posts after incremental = %d, want %d", got, len(truth2.Posts))
	}
	if got := count(t, s, `SELECT count(*) FROM comments WHERE page_id = $1`, page.ID); got != want2 {
		t.Fatalf("comments after incremental = %d, want %d", got, want2)
	}
	if j2.Counters["requests"] >= j.Counters["requests"] {
		t.Fatalf("incremental used %d requests, full used %d: expected far fewer", j2.Counters["requests"], j.Counters["requests"])
	}

	// 6. Erasure of an author redacts their comments and survives a re-scrape.
	var authorID string
	if err := s.DB.QueryRow(ctx, `SELECT a.author_id FROM comment_authors a JOIN comments c ON c.author_key = a.author_key
		WHERE c.page_id = $1 AND NOT c.is_op GROUP BY a.author_id ORDER BY count(*) DESC LIMIT 1`, page.ID).Scan(&authorID); err != nil {
		t.Fatal(err)
	}
	er := decodeBody[api.Erasure](t, s.Do(t, adminKey, "POST", "/v1/erasure-requests",
		[]byte(fmt.Sprintf(`{"subject_type":"author","author_id":%q}`, authorID))), http.StatusAccepted)
	if err := s.Eraser.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	done := decodeBody[api.Erasure](t, s.Do(t, adminKey, "GET", "/v1/erasure-requests/"+er.ID.String(), nil), http.StatusOK)
	redacted := count(t, s, `SELECT count(*) FROM comments WHERE redacted AND text IS NULL`)
	if done.Status != "done" || redacted == 0 {
		t.Fatalf("erasure = %+v, redacted = %d", done, redacted)
	}
	if err := s.Crawler.Suppression.Load(ctx); err != nil {
		t.Fatal(err)
	}
	j3 := s.runJob(t, key, page.Vanity, "full")
	if j3.Status != "completed" {
		t.Fatalf("re-scrape job = %+v", j3)
	}
	if n := count(t, s, `SELECT count(*) FROM comments WHERE redacted AND text IS NULL`); n != redacted {
		t.Fatalf("re-scrape restored erased data: redacted %d -> %d", redacted, n)
	}
	if n := count(t, s, `SELECT count(*) FROM comment_authors WHERE author_id = $1`, authorID); n != 0 {
		t.Fatal("erased author re-created")
	}

	// 7. Politeness: no identity exceeded its configured pacing.
	minAllowed := 1/s.Cfg.FB.IdentityRPS - 0.002
	for id, st := range s.Fake.Stats().Identities {
		if id == "anon" {
			continue // cookieless page loads and media downloads, not one identity
		}
		if st.Requests > 3 && st.MinInterval >= 0 && st.MinInterval < minAllowed/float64(s.Cfg.FB.IdentityBurst) {
			t.Errorf("identity %s paced too fast: %.4fs", id, st.MinInterval)
		}
	}
}

func TestBlockedIdentitiesRetryAndTruncation(t *testing.T) {
	// Every 4th GraphQL request of an identity is rate limited; tasks must
	// retry on other identities and still collect everything.
	//
	// Up to a quarter of a task's attempts can stall (the first request of
	// the attempt is its identity's 4th), so with the default cap of 5
	// stalls in a row about one task in a thousand was dead-lettered and
	// the job ended partial: the test failed now and then (igs's PR #1).
	// The cap is TestExhausted's subject; here it is 12, which a task
	// reaches about once in 17 million, while a job that needs more than
	// 8 retries in a row of one task (4 minutes) times out instead.
	s := Start(t, Options{
		Spec:   fakefb.Spec{Seed: 7, Pages: 1, PostsPerPage: 6, PostSpacing: 20 * time.Hour, MaxComments: 40, ReplyProbability: 0.3, MaxReplies: 12, Depth2Prob: 0.2},
		Faults: fakefb.Faults{RateLimitEvery: 4},
		Config: func(c *config.Config) { c.Crawler.MaxAttempts = 12 },
	})
	key := s.Key(t, api.ScopeJobsWrite, api.ScopeDataRead)
	page := s.Fake.World.Pages()[0]
	j := s.runJobWithin(t, key, page.Vanity, 4*time.Minute)
	truth, _ := s.Fake.World.TruthFor(page.ID, time.Time{})
	want := 0
	for _, p := range truth.Posts {
		want += p.TotalComments
	}
	got := count(t, s, `SELECT count(*) FROM comments WHERE page_id = $1`, page.ID)
	t.Logf("job %s %s: comments %d/%d counters=%v", j.ID, j.Status, got, want, j.Counters)
	if j.Status != "completed" || got != want {
		t.Fatalf("blocked run did not recover: status=%s comments=%d/%d", j.Status, got, want)
	}
	// The crawlers reported every GraphQL response, rate limits included,
	// to the fleet windows AIMD reads (page loads count too, so the windows
	// hold at least what fakefb served over GraphQL). How AIMD moves the
	// ceiling from them is unit tested; whether one 10s window holds enough
	// blocks to move it depends on timing here.
	ctx := context.Background()
	stats := s.Fake.Stats()
	served := 0
	for _, n := range stats.ByQuery {
		served += n
	}
	var total, blocked int64
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(200 * time.Millisecond) { // reporters flush every 2s
		total, blocked = 0, 0
		iter := s.Redis.Scan(ctx, 0, redisx.KeyPrefix+s.Cfg.Kafka.TopicPrefix+"aimd:w:*", 100).Iterator()
		for iter.Next(ctx) {
			h := s.Redis.HGetAll(ctx, iter.Val()).Val()
			n, _ := strconv.ParseInt(h["total"], 10, 64)
			b, _ := strconv.ParseInt(h["blocked"], 10, 64)
			total, blocked = total+n, blocked+b
		}
		if err := iter.Err(); err != nil {
			t.Fatal(err)
		}
		if (total >= int64(served) && blocked == int64(stats.RateLimited)) || time.Now().After(deadline) {
			break
		}
	}
	t.Logf("fleet windows: %d responses, %d blocked; fakefb served %d GraphQL responses, %d rate limited", total, blocked, served, stats.RateLimited)
	if total < int64(served) || blocked != int64(stats.RateLimited) {
		t.Fatalf("AIMD windows do not hold what fakefb served")
	}
}

func (s *Stack) runJobWithin(t *testing.T, key, page string, d time.Duration) jobView {
	t.Helper()
	j := decodeBody[jobView](t, s.Do(t, key, "POST", "/v1/jobs", []byte(fmt.Sprintf(`{"page":%q}`, page))), http.StatusAccepted)
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		v := decodeBody[jobView](t, s.Do(t, key, "GET", "/v1/jobs/"+j.ID, nil), http.StatusOK)
		switch v.Status {
		case "completed", "partial", "failed", "cancelled":
			return v
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("job %s did not finish within %s", j.ID, d)
	return jobView{}
}

// A page whose HTML names no id (as a few real ones do) is still crawled
// when it was registered by its numeric id: the dispatcher resolves the id
// from the reference.
func TestPageWithoutAnIDInItsHTML(t *testing.T) {
	s := Start(t, Options{
		Spec:   fakefb.Spec{Seed: 11, Pages: 1, PostsPerPage: 3, PostSpacing: 20 * time.Hour, MaxComments: 5},
		Faults: fakefb.Faults{NoPageID: true},
	})
	key := s.Key(t, api.ScopeJobsWrite, api.ScopeDataRead)
	page := s.Fake.World.Pages()[0]
	j := s.runJobWithin(t, key, strconv.FormatInt(page.ID, 10), 2*time.Minute)
	posts := count(t, s, `SELECT count(*) FROM posts WHERE page_id = $1`, page.ID)
	t.Logf("job %s %s: posts %d counters=%v", j.ID, j.Status, posts, j.Counters)
	if j.Status != "completed" || posts != 3 {
		t.Fatalf("page registered by id: status=%s posts=%d/3", j.Status, posts)
	}
}

// max_comments_per_post caps a post's comments, not each segment of its
// comment stream: a post task yields every crawler.yield_pages pages (2
// here) and its continuation goes on counting. Before M2 the count
// started again at every continuation, so a cap above one segment's
// comments never applied (fbs reference, Known issues).
func TestCommentCapSpansSegments(t *testing.T) {
	s := Start(t, Options{
		Spec: fakefb.Spec{Seed: 5, Pages: 1, PostsPerPage: 6, PostSpacing: 20 * time.Hour, MaxComments: 1200},
	})
	const capPerPost = 120
	key := s.Key(t, api.ScopeJobsWrite, api.ScopeDataRead)
	page := s.Fake.World.Pages()[0]
	body := fmt.Sprintf(`{"page":%q,"max_comments_per_post":%d,"include_replies":false}`, page.Vanity, capPerPost)
	j := decodeBody[jobView](t, s.Do(t, key, "POST", "/v1/jobs", []byte(body)), http.StatusAccepted)
	waitFor(t, "the job", 3*time.Minute, func() bool {
		v := decodeBody[jobView](t, s.Do(t, key, "GET", "/v1/jobs/"+j.ID, nil), http.StatusOK)
		j = v
		return v.Status != "running" && v.Status != "queued" && v.Status != "finalizing"
	})
	truth, _ := s.Fake.World.TruthFor(page.ID, time.Time{})
	capped := 0
	for _, p := range truth.Posts {
		got := count(t, s, `SELECT count(*) FROM comments WHERE post_id = $1`, p.PostID)
		t.Logf("post %d: %d of %d top-level comments stored", p.PostID, got, p.TopLevel)
		switch {
		case p.TopLevel <= capPerPost:
			if got != p.TopLevel {
				t.Errorf("post %d under the cap: %d of %d comments", p.PostID, got, p.TopLevel)
			}
		default:
			// The cap is checked after each page (25 comments in fakefb).
			capped++
			if got < capPerPost || got >= capPerPost+25 {
				t.Errorf("post %d over the cap: %d comments stored, want %d to %d", p.PostID, got, capPerPost, capPerPost+24)
			}
		}
	}
	if capped == 0 {
		t.Fatal("no post has more comments than the cap: pick another seed")
	}
	if j.Status != "completed" {
		t.Fatalf("job %s", j.Status)
	}
}
