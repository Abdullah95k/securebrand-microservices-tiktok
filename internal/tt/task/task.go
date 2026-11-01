// Package task builds and identifies tks's crawl tasks (securebrand.tks.task.v1).
// It is internal/crawl/task on tks's envelope: the same deterministic ids,
// continuations and retries, with TikTok's scopes (account, post, thread,
// media) and account references (u:<username>, sec:<secUid>, id:<user id>;
// docs/tiktok ADR 0007 §1). The copied Facebook package stays until M9 removes
// the Facebook code; a fix to one belongs in both until then.
package task

import (
	"crypto/sha256"
	"encoding/base32"
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
)

var b32 = base32.StdEncoding.WithPadding(base32.NoPadding)

func hashID(parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return strings.ToLower(b32.EncodeToString(h[:20]))
}

// ScopeKey identifies the stream a task paginates; used for leases. An
// account is keyed by its user id once known, by its reference before.
func ScopeKey(t *tkstaskv1.Task) string {
	switch s := t.Scope.(type) {
	case *tkstaskv1.Task_Account:
		if s.Account.UserId != 0 {
			return "account:" + strconv.FormatInt(s.Account.UserId, 10)
		}
		return "accountref:" + s.Account.AccountRef
	case *tkstaskv1.Task_Post:
		return "post:" + strconv.FormatInt(s.Post.PostId, 10)
	case *tkstaskv1.Task_Thread:
		return "thread:" + strconv.FormatInt(s.Thread.CommentId, 10)
	case *tkstaskv1.Task_Media:
		return "media:" + s.Media.GetMedia().GetMediaId()
	}
	return "unknown"
}

// Finalize sets the deterministic task id (and lineage for a new stream).
func Finalize(t *tkstaskv1.Task) *tkstaskv1.Task {
	if t.LineageId == "" {
		t.LineageId = hashID("lineage", t.JobId, t.Kind.String(), ScopeKey(t), t.Mode.String())
	}
	t.TaskId = hashID(t.JobId, t.Kind.String(), ScopeKey(t), t.Mode.String(), t.LineageId, strconv.FormatUint(uint64(t.Seq), 10))
	if t.EnqueuedAt == nil {
		t.EnqueuedAt = timestamppb.Now()
	}
	return t
}

// Continuation returns the next segment of the same stream.
func Continuation(t *tkstaskv1.Task, cursor string) *tkstaskv1.Task {
	c := cloneShallow(t)
	c.Seq = t.Seq + 1
	c.Cursor = cursor
	c.Attempt, c.Stalls, c.Waits = 0, 0, 0
	c.EnqueuedAt = nil
	return Finalize(c)
}

// Retry returns the same task (same id) with attempt incremented. The worker
// sets Stalls, which only it can tell from the task's checkpoint.
func Retry(t *tkstaskv1.Task, cursor string) *tkstaskv1.Task {
	c := cloneShallow(t)
	c.Attempt = t.Attempt + 1
	if cursor != "" {
		c.Cursor = cursor
	}
	return c
}

func cloneShallow(t *tkstaskv1.Task) *tkstaskv1.Task {
	c := &tkstaskv1.Task{
		TaskId: t.TaskId, LineageId: t.LineageId, Seq: t.Seq, JobId: t.JobId, Lane: t.Lane, Kind: t.Kind,
		Mode: t.Mode, Cursor: t.Cursor, Limits: t.Limits, Affinity: t.Affinity, Attempt: t.Attempt, Stalls: t.Stalls, Waits: t.Waits, Trace: t.Trace,
	}
	switch s := t.Scope.(type) {
	case *tkstaskv1.Task_Account:
		c.Scope = &tkstaskv1.Task_Account{Account: proto.Clone(s.Account).(*tkstaskv1.AccountScope)}
	case *tkstaskv1.Task_Post:
		c.Scope = &tkstaskv1.Task_Post{Post: proto.Clone(s.Post).(*tkstaskv1.PostScope)}
	case *tkstaskv1.Task_Thread:
		c.Scope = &tkstaskv1.Task_Thread{Thread: proto.Clone(s.Thread).(*tkstaskv1.ThreadScope)}
	case *tkstaskv1.Task_Media:
		c.Scope = s
	}
	return c
}

// NewAccount creates the root task of an account job: one check of its post
// list (and, when due, of the account itself).
func NewAccount(jobID string, lane tkstaskv1.Lane, mode commonv1.CrawlMode, s *tkstaskv1.AccountScope, lim *tkstaskv1.Limits) *tkstaskv1.Task {
	return Finalize(&tkstaskv1.Task{
		JobId: jobID, Lane: lane, Kind: tkstaskv1.Kind_KIND_ACCOUNT, Mode: mode, Limits: lim,
		Scope: &tkstaskv1.Task_Account{Account: s},
	})
}

// NewPost creates a comments task for a post.
func NewPost(parent *tkstaskv1.Task, mode commonv1.CrawlMode, s *tkstaskv1.PostScope) *tkstaskv1.Task {
	return Finalize(&tkstaskv1.Task{
		JobId: parent.JobId, Lane: parent.Lane, Kind: tkstaskv1.Kind_KIND_POST, Mode: mode, Limits: parent.Limits,
		Scope: &tkstaskv1.Task_Post{Post: s}, Trace: parent.Trace,
	})
}

// NewThread creates a replies task.
func NewThread(parent *tkstaskv1.Task, mode commonv1.CrawlMode, s *tkstaskv1.ThreadScope) *tkstaskv1.Task {
	return Finalize(&tkstaskv1.Task{
		JobId: parent.JobId, Lane: parent.Lane, Kind: tkstaskv1.Kind_KIND_THREAD, Mode: mode, Limits: parent.Limits,
		Scope: &tkstaskv1.Task_Thread{Thread: s}, Trace: parent.Trace,
	})
}

// NewMedia creates a download task. Media tasks are not part of job
// accounting: a job completes when its data is stored, not when media is.
func NewMedia(jobID string, m *ttv1.Media) *tkstaskv1.Task {
	return Finalize(&tkstaskv1.Task{
		JobId: jobID, Kind: tkstaskv1.Kind_KIND_MEDIA, Lane: tkstaskv1.Lane_LANE_P1,
		Scope: &tkstaskv1.Task_Media{Media: &tkstaskv1.MediaScope{Media: m}},
	})
}

// DefaultLimits builds a job's limits from config. Media downloads follow
// tt.media.include, off by default (build brief D5), not the shared
// limits.include_media the copied Facebook pipeline still uses; the age
// window is tt.freshness.backfill_days.
func DefaultLimits(c config.Limits, tt config.TT, backfill bool) *tkstaskv1.Limits {
	vp := c.VideoPolicy
	if backfill && c.BackfillVideoPolicy != "" {
		vp = c.BackfillVideoPolicy
	}
	return &tkstaskv1.Limits{
		MaxAgeSeconds:       int64(tt.Freshness.BackfillDays) * 86400,
		MaxPosts:            c.MaxPosts,
		MaxCommentsPerPost:  tt.Comments.MaxPerPost,
		IncludeComments:     c.IncludeComments,
		IncludeReplies:      tt.Comments.IncludeReplies,
		IncludeMedia:        tt.Media.Include,
		IncludeCommentMedia: tt.Media.Include && c.IncludeCommentMedia,
		VideoPolicy:         ParseVideoPolicy(vp),
	}
}

// ParseVideoPolicy maps config strings to the enum.
func ParseVideoPolicy(s string) tkstaskv1.VideoPolicy {
	switch strings.ToLower(s) {
	case "none":
		return tkstaskv1.VideoPolicy_VIDEO_POLICY_NONE
	case "thumbnail":
		return tkstaskv1.VideoPolicy_VIDEO_POLICY_THUMBNAIL
	case "hd":
		return tkstaskv1.VideoPolicy_VIDEO_POLICY_HD
	default:
		return tkstaskv1.VideoPolicy_VIDEO_POLICY_SD
	}
}

// NormalizeRef turns what a client may give for an account into a
// canonical reference: "u:<lowercase username>" (from a username, @username
// or a tiktok.com/@username link), "sec:<secUid>" or "id:<user id>".
func NormalizeRef(in string) (string, error) {
	s := strings.TrimSpace(in)
	if s == "" {
		return "", fmt.Errorf("empty account reference")
	}
	switch {
	case strings.HasPrefix(s, "id:"):
		id, err := strconv.ParseInt(s[3:], 10, 64)
		if err != nil || id <= 0 {
			return "", fmt.Errorf("invalid account reference %q", in)
		}
		return "id:" + strconv.FormatInt(id, 10), nil
	case strings.HasPrefix(s, "sec:"):
		sec := s[4:]
		if sec == "" || strings.ContainsFunc(sec, func(r rune) bool { return !secRune(r) }) {
			return "", fmt.Errorf("invalid account reference %q", in)
		}
		return "sec:" + sec, nil
	case strings.HasPrefix(s, "u:"):
		s = s[2:]
	default:
		if i := strings.Index(s, "tiktok.com/"); i >= 0 {
			s = s[i+len("tiktok.com/"):]
		}
		if j := strings.IndexAny(s, "/?#"); j >= 0 {
			s = s[:j]
		}
		s = strings.TrimPrefix(s, "@")
	}
	if !ValidUsername(s) {
		return "", fmt.Errorf("invalid account reference %q", in)
	}
	return "u:" + strings.ToLower(s), nil
}

// ValidUsername reports whether s can be a TikTok username: letters,
// digits, underscores and periods, not only periods. It becomes a URL path,
// so nothing else passes.
func ValidUsername(s string) bool {
	if s == "" || len(s) > 64 || strings.Trim(s, ".") == "" {
		return false
	}
	for _, r := range s {
		if r != '.' && r != '_' && !secRune(r) || r == '-' {
			return false
		}
	}
	return true
}

// secRune: a secUid is URL-safe base64.
func secRune(r rune) bool {
	return r == '-' || r == '_' || r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z'
}

// ScopeFromRef fills an account scope from a normalised reference.
func ScopeFromRef(ref string) *tkstaskv1.AccountScope {
	s := &tkstaskv1.AccountScope{AccountRef: ref}
	switch {
	case strings.HasPrefix(ref, "u:"):
		s.Username = ref[2:]
	case strings.HasPrefix(ref, "sec:"):
		s.SecUid = ref[4:]
	case strings.HasPrefix(ref, "id:"):
		s.UserId, _ = strconv.ParseInt(ref[3:], 10, 64)
	}
	return s
}

// ProfilePath is the profile page's path for a username.
func ProfilePath(username string) string { return "/@" + username }

// MaxAgeCutoff returns the oldest post time a task accepts.
func MaxAgeCutoff(l *tkstaskv1.Limits, now time.Time) time.Time {
	if l.GetMaxAgeSeconds() <= 0 {
		return time.Time{}
	}
	return now.Add(-time.Duration(l.MaxAgeSeconds) * time.Second)
}
