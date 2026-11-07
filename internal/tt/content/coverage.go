package content

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"
	coveragev1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/coverage/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/parse"
)

// sharedCounters are tks's job counters with a shared key in the contract
// (coverage.v1's counters); the rest go as "tt.<name>".
var sharedCounters = map[string]string{
	"requests": "requests", "wire_bytes": "bytes", "posts": "posts", "comments": "comments", "replies": "replies",
	"blocked": "blocked",
}

// reasonCounters are tks's job counters that say why data is missing, by
// the contract's reason class.
var reasonCounters = map[string]string{
	"accounts_private": "private", "accounts_unavailable": "removed", "profile_unavailable": "removed",
	"lists_truncated": "wall", "blocked": "rate_limited", "drift": "parse_error",
}

// Job kinds (internal/tt/dispatch), repeated here so content does not
// import the dispatcher.
const (
	jobOnDemand = "on_demand"
	jobBackfill = "backfill"
	jobEntry    = "entry"
	modeFull    = "full"
	modeRefresh = "refresh"
)

func jobKind(kind, mode string) coveragev1.JobKind {
	switch {
	case kind == jobEntry:
		return coveragev1.JobKind_JOB_KIND_DISCOVERY_CHECK
	case kind == jobOnDemand:
		return coveragev1.JobKind_JOB_KIND_ON_DEMAND
	case kind == jobBackfill || mode == modeFull:
		return coveragev1.JobKind_JOB_KIND_BACKFILL
	case mode == modeRefresh:
		return coveragev1.JobKind_JOB_KIND_REFRESH
	}
	// scheduled checks, and the following and followers list reads
	return coveragev1.JobKind_JOB_KIND_SCHEDULED
}

func jobStatus(s commonv1.JobStatus) coveragev1.JobStatus {
	switch s {
	case commonv1.JobStatus_JOB_STATUS_COMPLETED:
		return coveragev1.JobStatus_JOB_STATUS_COMPLETED
	case commonv1.JobStatus_JOB_STATUS_PARTIAL:
		return coveragev1.JobStatus_JOB_STATUS_PARTIAL
	case commonv1.JobStatus_JOB_STATUS_FAILED:
		return coveragev1.JobStatus_JOB_STATUS_FAILED
	case commonv1.JobStatus_JOB_STATUS_CANCELLED:
		return coveragev1.JobStatus_JOB_STATUS_CANCELLED
	}
	return coveragev1.JobStatus_JOB_STATUS_UNSPECIFIED
}

// JobCoverage maps a finished job's tt.job.v1 event to
// collection.coverage.v1 (key: the job ID), one per job (job_count 1).
// mode is the job's crawl mode (jobs.mode). The event id is the same for
// the same job and status, as tt.job.v1's is, so a job finalised twice
// is one event.
func JobCoverage(ev *commonv1.JobEvent, mode string) *coveragev1.JobCoverage {
	sum := sha256.Sum256([]byte(Platform + "|job|" + ev.JobId + "|" + ev.Status.String()))
	m := &coveragev1.JobCoverage{
		EventId: hex.EncodeToString(sum[:]), Platform: Platform, Collector: Collector, JobId: ev.JobId,
		Kind: jobKind(ev.Kind, mode), Status: jobStatus(ev.Status), SubjectRef: ev.SubjectRef,
		CreatedAt: ev.CreatedAt, FinishedAt: ev.FinishedAt, Counters: map[string]int64{}, JobCount: 1,
		Provenance: &contentv1.Provenance{Collector: Collector, CollectorVersion: collectorVersion, JobIds: []string{ev.JobId},
			Acquisition: contentv1.Acquisition_ACQUISITION_SCRAPED_LOGGED_OUT, ParserVersion: strconv.Itoa(parse.Version),
			Mode: crawlMode(mode)},
	}
	if ev.SubjectId != 0 {
		m.SubjectId = idOf("account", ev.SubjectId)
	}
	reasons := map[string]int64{}
	for k, v := range ev.Counters {
		if v == 0 {
			continue
		}
		if s, ok := sharedCounters[k]; ok {
			m.Counters[s] += v
		} else {
			m.Counters[Platform+"."+k] = v
		}
		if c, ok := reasonCounters[k]; ok {
			reasons[c] += v
		}
	}
	if c := ev.Completeness; c != nil {
		m.Completeness = &coveragev1.Completeness{Posts: c.Posts, PostsComplete: c.PostsComplete, PostsTruncated: c.PostsTruncated,
			CommentsReported: c.CommentsReported, CommentsCollected: c.CommentsCollected}
		if c.Posts > 0 {
			m.Completeness.MedianRatio, m.Completeness.MinRatio = proto.Float64(c.MedianRatio), proto.Float64(c.MinRatio)
		}
		if c.PostsTruncated > 0 {
			reasons["wall"] += c.PostsTruncated
		}
	}
	classes := make([]string, 0, len(reasons))
	for c := range reasons {
		classes = append(classes, c)
	}
	sort.Strings(classes)
	for _, c := range classes {
		m.Reasons = append(m.Reasons, &coveragev1.Reason{Class: c, Count: reasons[c]})
	}
	for _, e := range ev.Errors {
		// The reaper's: a job closed without progress.
		m.Reasons = append(m.Reasons, &coveragev1.Reason{Class: "stalled", Count: 1, Detail: e})
	}
	return m
}

func crawlMode(mode string) contentv1.CrawlMode {
	switch mode {
	case modeFull:
		return contentv1.CrawlMode_CRAWL_MODE_FULL
	case "incremental":
		return contentv1.CrawlMode_CRAWL_MODE_INCREMENTAL
	case modeRefresh:
		return contentv1.CrawlMode_CRAWL_MODE_REFRESH
	}
	return contentv1.CrawlMode_CRAWL_MODE_UNSPECIFIED
}

// checkCoverage is tks's own check of a JobCoverage: the contract has no
// conformance check for it (v1.2.0).
func checkCoverage(m *coveragev1.JobCoverage) error {
	var errs []error
	if m.GetEventId() == "" || m.GetJobId() == "" {
		errs = append(errs, errors.New("event_id and job_id are required"))
	}
	if m.GetPlatform() != Platform || m.GetCollector() == "" {
		errs = append(errs, fmt.Errorf("platform %q, collector %q", m.GetPlatform(), m.GetCollector()))
	}
	if m.GetStatus() == coveragev1.JobStatus_JOB_STATUS_UNSPECIFIED || m.GetKind() == coveragev1.JobKind_JOB_KIND_UNSPECIFIED {
		errs = append(errs, errors.New("status and kind are required"))
	}
	if m.GetFinishedAt() == nil || m.GetJobCount() < 1 {
		errs = append(errs, errors.New("finished_at and job_count are required"))
	}
	if id := m.GetSubjectId(); id != "" && !strings.HasPrefix(id, ID("account", "")) {
		errs = append(errs, fmt.Errorf("subject_id %q is not an account ID", id))
	}
	return errors.Join(errs...)
}

// PostComments is a post's comment coverage as the finalizer wrote it
// (tt.posts: comments_state, comments_collected, replies_collected,
// completeness, comments_synced_at).
type PostComments struct {
	PostID, UserID int64
	// State is tks's: complete, partial (the cap per post), truncated.
	State             string
	CommentsDisabled  bool
	Reported          int64
	Collected         int64
	RepliesCollected  int64
	Completeness      float64
	SyncedAt          time.Time
	JobID             string
	ExitCountry       string
	CommentsReadError bool
}

// PostCoverage maps a post's comment coverage to a partial
// content.post.v1 event that carries only Post.coverage (the contract's
// partial events: unset fields are unchanged). It goes through the gate on
// a stream of its own ("coverage/<post id>"), so it does not disturb the
// post's content stream, and goes out when the state or the counts
// changed (not the sync time alone).
func PostCoverage(c PostComments) *Event {
	if c.PostID == 0 || c.UserID == 0 || c.SyncedAt.IsZero() {
		return nil
	}
	at := c.SyncedAt.UTC()
	o := &commonv1.Observation{ObservedAt: timestamppb.New(at), ParserVersion: uint32(parse.Version), ExitCountry: c.ExitCountry}
	if c.JobID != "" {
		o.JobIds = []string{c.JobID}
	}
	env := envelope("post", "post", c.PostID, o, true)
	cov := &contentv1.ItemCoverage{
		CommentsState:     commentsState(c.State, c.CommentsDisabled),
		CommentsCollected: proto.Int64(c.Collected), CommentsReported: proto.Int64(c.Reported),
		Completeness:     proto.Float64(c.Completeness),
		RepliesCollected: proto.Int64(c.RepliesCollected), RepliesAvailable: proto.Bool(true),
		SyncedAt: exact(timestamppb.New(at)),
	}
	switch {
	case c.CommentsReadError:
		cov.Reason = "error"
	case cov.CommentsState == contentv1.CommentsState_COMMENTS_STATE_PARTIAL:
		cov.Reason = "limit"
	case cov.CommentsState == contentv1.CommentsState_COMMENTS_STATE_TRUNCATED:
		cov.Reason = "wall"
	}
	m := &contentv1.Post{Envelope: env, AccountId: idOf("account", c.UserID), Coverage: cov}
	e := newEvent(m, env, topicPost, func(x proto.Message) { x.(*contentv1.Post).Coverage.SyncedAt = nil }, false)
	e.Gate = "coverage/" + env.Id
	return e
}

// commentsState maps tks's comments state to the contract's.
func commentsState(s string, disabled bool) contentv1.CommentsState {
	if disabled {
		return contentv1.CommentsState_COMMENTS_STATE_DISABLED
	}
	switch s {
	case "complete":
		return contentv1.CommentsState_COMMENTS_STATE_COMPLETE
	case "partial":
		return contentv1.CommentsState_COMMENTS_STATE_PARTIAL
	case "truncated":
		return contentv1.CommentsState_COMMENTS_STATE_TRUNCATED
	}
	return contentv1.CommentsState_COMMENTS_STATE_NOT_READ
}
