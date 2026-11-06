package content

import (
	"bytes"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-contracts/conformance"
	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"
	coveragev1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/coverage/v1"
	discoveryv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/discovery/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
)

var prod = conformance.Options{Production: true}

func storedFile() StoredFile {
	return StoredFile{MediaID: "11111111-1111-5111-8111-111111111111", Kind: ttv1.MediaKind_MEDIA_KIND_VIDEO, Variant: "sd",
		UserID: 6900000000000000001, PostID: proto.Int64(7400000000000000001), Width: 720, Height: 1280, DurationMs: 15000,
		Bucket: "tks-media", Key: "tks/media/11/x.mp4", ContentType: "video/mp4", Size: 1234, SHA256: bytes.Repeat([]byte{0xab}, 32),
		StoredAt: t0, JobIDs: []string{"00000000-0000-0000-0000-000000000001"}}
}

// A stored file's ready event conforms, carries the deadline from its
// stored time and the required consumers, and has one id per announcement.
func TestMediaReady(t *testing.T) {
	h := Handoff{DeleteAfter: 72 * time.Hour, RequiredConsumers: []string{"sb-analysis"}}
	m := MediaReady(storedFile(), 1, h)
	if err := Check(m, prod); err != nil {
		t.Fatal(err)
	}
	if m.MediaId != "tt:media:11111111-1111-5111-8111-111111111111" || m.OwnerId != "tt:post:7400000000000000001" ||
		m.OwnerKind != "post" || m.AccountId != "tt:account:6900000000000000001" || m.Type != contentv1.MediaType_MEDIA_TYPE_VIDEO ||
		m.Role != contentv1.MediaRole_MEDIA_ROLE_CONTENT || m.Object.Sha256 != "abababababababababababababababababababababababababababababababab" {
		t.Errorf("mapped %v", m)
	}
	if !m.DeleteAfter.AsTime().Equal(t0.Add(72*time.Hour)) || m.GetWidth() != 720 || m.GetDurationMs() != 15000 {
		t.Errorf("deadline %v, size %dx%d", m.DeleteAfter.AsTime(), m.GetWidth(), m.GetHeight())
	}
	if again := MediaReady(storedFile(), 1, h); again.EventId != m.EventId {
		t.Error("the same announcement has two event ids")
	}
	second := MediaReady(storedFile(), 2, h)
	if second.EventId == m.EventId || !second.DeleteAfter.AsTime().Equal(m.DeleteAfter.AsTime()) {
		t.Error("a second announcement needs its own id and the same deadline")
	}
	if err := Check(MediaReady(storedFile(), 5, h), prod); err == nil {
		t.Error("a fifth announcement passed the contract's check")
	}
	if err := Check(MediaReady(storedFile(), 1, Handoff{DeleteAfter: 12 * time.Hour, RequiredConsumers: h.RequiredConsumers}), prod); err == nil {
		t.Error("a deadline under 24 hours passed the contract's check")
	}
	// An avatar is the account's; a file without an owner is not mapped.
	f := storedFile()
	f.Kind, f.PostID = ttv1.MediaKind_MEDIA_KIND_AVATAR, nil
	if a := MediaReady(f, 1, h); a.OwnerId != "tt:account:6900000000000000001" || a.Role != contentv1.MediaRole_MEDIA_ROLE_PROFILE_PICTURE {
		t.Errorf("avatar %v", a)
	}
	f.UserID = 0
	if MediaReady(f, 1, h) != nil {
		t.Error("a file without an owner was mapped")
	}
	if MediaID(m.MediaId) != f.MediaID || MediaID(f.MediaID) != f.MediaID {
		t.Error("MediaID")
	}
}

// A job's tt.job.v1 event maps to JobCoverage: the shared counter keys,
// tks's others under "tt.", the reasons, the kind from kind and mode.
func TestJobCoverage(t *testing.T) {
	ev := &commonv1.JobEvent{EventId: "j:partial", JobId: "00000000-0000-0000-0000-000000000009",
		Status: commonv1.JobStatus_JOB_STATUS_PARTIAL, Kind: "scheduled", SubjectRef: "u:fake.user", SubjectId: 6900000000000000001,
		CreatedAt: timestamppb.New(t0), FinishedAt: timestamppb.New(t0.Add(time.Minute)),
		Counters: map[string]int64{"requests": 12, "wire_bytes": 3400, "posts": 5, "comments": 40, "replies": 3, "lists_truncated": 1,
			"threads_spawned": 2, "accounts_private": 0},
		Completeness: &commonv1.Completeness{Posts: 2, PostsComplete: 1, PostsTruncated: 1, MedianRatio: 0.9, MinRatio: 0.5,
			CommentsReported: 50, CommentsCollected: 43}}
	m := JobCoverage(ev, "incremental")
	if err := Check(m, prod); err != nil {
		t.Fatal(err)
	}
	if m.Kind != coveragev1.JobKind_JOB_KIND_SCHEDULED || m.Status != coveragev1.JobStatus_JOB_STATUS_PARTIAL ||
		m.SubjectId != "tt:account:6900000000000000001" || m.JobCount != 1 {
		t.Errorf("mapped %v", m)
	}
	want := map[string]int64{"requests": 12, "bytes": 3400, "posts": 5, "comments": 40, "replies": 3, "tt.lists_truncated": 1,
		"tt.threads_spawned": 2}
	if len(m.Counters) != len(want) {
		t.Errorf("counters %v", m.Counters)
	}
	for k, v := range want {
		if m.Counters[k] != v {
			t.Errorf("counter %s = %d, want %d", k, m.Counters[k], v)
		}
	}
	if len(m.Reasons) != 1 || m.Reasons[0].Class != "wall" || m.Reasons[0].Count != 2 {
		t.Errorf("reasons %v", m.Reasons)
	}
	if m.Completeness.GetMedianRatio() != 0.9 || m.Completeness.PostsTruncated != 1 {
		t.Errorf("completeness %v", m.Completeness)
	}
	if JobCoverage(ev, "incremental").EventId != m.EventId {
		t.Error("the same job's coverage has two event ids")
	}
	for _, tc := range []struct {
		kind, mode string
		want       coveragev1.JobKind
	}{
		{"backfill", "full", coveragev1.JobKind_JOB_KIND_BACKFILL},
		{"scheduled", "full", coveragev1.JobKind_JOB_KIND_BACKFILL},
		{"scheduled", "refresh", coveragev1.JobKind_JOB_KIND_REFRESH},
		{"on_demand", "auto", coveragev1.JobKind_JOB_KIND_ON_DEMAND},
		{"entry", "entry", coveragev1.JobKind_JOB_KIND_DISCOVERY_CHECK},
		{"follows", "followers", coveragev1.JobKind_JOB_KIND_SCHEDULED},
	} {
		if got := jobKind(tc.kind, tc.mode); got != tc.want {
			t.Errorf("%s/%s: %s, want %s", tc.kind, tc.mode, got, tc.want)
		}
	}
	// The reaper's: a stalled job with its reason.
	stalled := JobCoverage(&commonv1.JobEvent{JobId: "00000000-0000-0000-0000-00000000000a", Status: commonv1.JobStatus_JOB_STATUS_FAILED,
		Kind: "scheduled", FinishedAt: timestamppb.New(t0), Errors: []string{"job state lost"}}, "")
	if err := Check(stalled, prod); err != nil || len(stalled.Reasons) != 1 || stalled.Reasons[0].Class != "stalled" {
		t.Errorf("stalled job: %v, %v", err, stalled.Reasons)
	}
}

// A post's comment coverage is a partial post event on a stream of its
// own: it conforms, maps the state, and changes its hash only when the
// coverage does (not its sync time).
func TestPostCoverage(t *testing.T) {
	c := PostComments{PostID: 7400000000000000001, UserID: 6900000000000000001, State: "partial", Reported: 120, Collected: 100,
		RepliesCollected: 7, Completeness: 0.89, SyncedAt: t0, JobID: "00000000-0000-0000-0000-000000000001"}
	e := PostCoverage(c)
	e.Seal()
	if err := Check(e.Msg, prod); err != nil {
		t.Fatal(err)
	}
	p := e.Msg.(*contentv1.Post)
	if !p.Envelope.Partial || e.Gate != "coverage/tt:post:7400000000000000001" || e.Key != "tt:post:7400000000000000001" ||
		p.Coverage.CommentsState != contentv1.CommentsState_COMMENTS_STATE_PARTIAL || p.Coverage.Reason != "limit" ||
		p.Coverage.GetCommentsCollected() != 100 || !p.Coverage.GetRepliesAvailable() {
		t.Errorf("mapped %v (gate %s)", p, e.Gate)
	}
	later := c
	later.SyncedAt = t0.Add(3 * time.Hour)
	if !bytes.Equal(PostCoverage(later).Hash, e.Hash) {
		t.Error("the sync time alone changed the coverage's hash")
	}
	later.Collected = 101
	if bytes.Equal(PostCoverage(later).Hash, e.Hash) {
		t.Error("a new count did not change the coverage's hash")
	}
	for _, tc := range []struct {
		state    string
		disabled bool
		want     contentv1.CommentsState
	}{
		{"complete", false, contentv1.CommentsState_COMMENTS_STATE_COMPLETE},
		{"truncated", false, contentv1.CommentsState_COMMENTS_STATE_TRUNCATED},
		{"complete", true, contentv1.CommentsState_COMMENTS_STATE_DISABLED},
		{"none", false, contentv1.CommentsState_COMMENTS_STATE_NOT_READ},
	} {
		if got := commentsState(tc.state, tc.disabled); got != tc.want {
			t.Errorf("%s (disabled %v): %s", tc.state, tc.disabled, got)
		}
	}
}

// A candidate maps to discovery.candidate.v1 with its outcome, sources and
// evidence; its hash ignores the sighting counts, and conforms.
func TestCandidate(t *testing.T) {
	uid := int64(6900000000000000005)
	user, score := "found.user", 0.75
	checked := t0.Add(time.Hour)
	cd := &storedCandidate{ref: "sec:MS4wfound", state: "dormant", userID: &uid, username: &user, reason: proto.String("private"),
		score: &score, evidence: []byte(`[{"signal":"bio_place","strength":"strong"},{"signal":"post_lang","strength":"weak"}]`),
		found: t0, updated: checked, checked: &checked,
		sources: []storedSource{{how: "commenter", via: "6900000000000000001", first: t0, seen: 3,
			evidence: []byte(`{"found_in":"comment","post_id":"7400000000000000001","comment_id":"7600000000000000001"}`)},
			{how: "search", via: "kw-1", first: t0.Add(time.Minute), seen: 1}}}
	e := cd.event()
	e.Seal()
	m := e.Msg.(*discoveryv1.Candidate)
	if err := Check(m, prod); err != nil {
		t.Fatal(err)
	}
	if m.Outcome != discoveryv1.Outcome_OUTCOME_DORMANT || m.OutcomeReason != "private" || !m.GetIsPrivate() ||
		m.Source != discoveryv1.Source_SOURCE_COMMENTER || m.Account.Id != "tt:account:6900000000000000005" ||
		m.Account.AltIds["sec_uid"] != "MS4wfound" || m.Account.Handle != "found.user" || m.EventId == "" || e.Key != "tt/sec:MS4wfound" {
		t.Errorf("mapped %v", m)
	}
	if len(m.Evidence) != 2 || m.Evidence[0].ContentId != "tt:comment:7600000000000000001" || m.Evidence[0].Sightings != 3 ||
		m.Evidence[1].Via != "search:kw-1" {
		t.Errorf("evidence %v", m.Evidence)
	}
	if r := m.Relevance; r.GetScore() != 0.75 || len(r.MatchedRules) != 2 || r.MatchedRules[0] != "bio_place" {
		t.Errorf("relevance %v", r)
	}
	seen := *cd
	seen.sources = append([]storedSource{}, cd.sources...)
	seen.sources[0].seen = 9
	if !bytes.Equal(seen.event().Hash, e.Hash) {
		t.Error("a new sighting alone changed the candidate's hash")
	}
	for _, tc := range []struct {
		state, reason string
		want          discoveryv1.Outcome
	}{
		{"pending", "", discoveryv1.Outcome_OUTCOME_PENDING},
		{"checking", "", discoveryv1.Outcome_OUTCOME_PENDING},
		{"qualified", "", discoveryv1.Outcome_OUTCOME_PENDING},
		{"added", "", discoveryv1.Outcome_OUTCOME_ADDED},
		{"not_iraqi", "", discoveryv1.Outcome_OUTCOME_NOT_RELEVANT},
		{"dormant", "inactive", discoveryv1.Outcome_OUTCOME_DORMANT},
		{"dormant", "unreadable", discoveryv1.Outcome_OUTCOME_UNAVAILABLE},
		{"blocked", "", discoveryv1.Outcome_OUTCOME_EXCLUDED},
	} {
		if got, _ := outcome(tc.state, tc.reason); got != tc.want {
			t.Errorf("%s (%s): %s, want %s", tc.state, tc.reason, got, tc.want)
		}
	}
	// A candidate known by username only.
	bare := &storedCandidate{ref: "u:someone", state: "pending", found: t0, updated: t0,
		sources: []storedSource{{how: "following", via: "6900000000000000001", first: t0, seen: 1}}}
	b := bare.event()
	b.Seal()
	if err := Check(b.Msg, prod); err != nil || b.Msg.(*discoveryv1.Candidate).Account.Handle != "someone" {
		t.Errorf("username-only candidate: %v", err)
	}
}
