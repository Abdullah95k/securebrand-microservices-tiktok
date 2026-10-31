// Package task builds and identifies crawl tasks.
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

	fbv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fb/v1"
	taskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
)

var b32 = base32.StdEncoding.WithPadding(base32.NoPadding)

func hashID(parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return strings.ToLower(b32.EncodeToString(h[:20]))
}

// ScopeKey identifies the stream a task paginates; used for leases.
func ScopeKey(t *taskv1.Task) string {
	switch s := t.Scope.(type) {
	case *taskv1.Task_Page:
		if s.Page.PageId != 0 {
			return "page:" + strconv.FormatInt(s.Page.PageId, 10)
		}
		return "pageref:" + s.Page.PageRef
	case *taskv1.Task_Post:
		return "post:" + strconv.FormatInt(s.Post.PostId, 10)
	case *taskv1.Task_Thread:
		return "thread:" + strconv.FormatInt(s.Thread.CommentId, 10)
	case *taskv1.Task_Media:
		return "media:" + s.Media.GetMedia().GetMediaId()
	}
	return "unknown"
}

// Finalize sets the deterministic task id (and lineage for a new stream).
func Finalize(t *taskv1.Task) *taskv1.Task {
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
func Continuation(t *taskv1.Task, cursor string) *taskv1.Task {
	c := cloneShallow(t)
	c.Seq = t.Seq + 1
	c.Cursor = cursor
	c.Attempt, c.Stalls = 0, 0
	c.EnqueuedAt = nil
	return Finalize(c)
}

// Retry returns the same task (same id) with attempt incremented. The worker
// sets Stalls, which only it can tell from the task's checkpoint.
func Retry(t *taskv1.Task, cursor string) *taskv1.Task {
	c := cloneShallow(t)
	c.Attempt = t.Attempt + 1
	if cursor != "" {
		c.Cursor = cursor
	}
	return c
}

func cloneShallow(t *taskv1.Task) *taskv1.Task {
	c := &taskv1.Task{
		TaskId: t.TaskId, LineageId: t.LineageId, Seq: t.Seq, JobId: t.JobId, Lane: t.Lane, Kind: t.Kind,
		Mode: t.Mode, Cursor: t.Cursor, Limits: t.Limits, Affinity: t.Affinity, Attempt: t.Attempt, Stalls: t.Stalls, Trace: t.Trace,
	}
	switch s := t.Scope.(type) {
	case *taskv1.Task_Page:
		c.Scope = &taskv1.Task_Page{Page: proto.Clone(s.Page).(*taskv1.PageScope)}
	case *taskv1.Task_Post:
		c.Scope = &taskv1.Task_Post{Post: proto.Clone(s.Post).(*taskv1.PostScope)}
	case *taskv1.Task_Thread:
		c.Scope = &taskv1.Task_Thread{Thread: proto.Clone(s.Thread).(*taskv1.ThreadScope)}
	case *taskv1.Task_Media:
		c.Scope = s
	}
	return c
}

// NewPage creates the root task of a page job.
func NewPage(jobID string, lane taskv1.Lane, mode fbv1.CrawlMode, ref string, pageID int64, url string, lim *taskv1.Limits) *taskv1.Task {
	return Finalize(&taskv1.Task{
		JobId: jobID, Lane: lane, Kind: taskv1.Kind_KIND_PAGE, Mode: mode, Limits: lim,
		Scope: &taskv1.Task_Page{Page: &taskv1.PageScope{PageRef: ref, PageId: pageID, Url: url}},
	})
}

// NewPost creates a comments task for a post.
func NewPost(parent *taskv1.Task, mode fbv1.CrawlMode, s *taskv1.PostScope) *taskv1.Task {
	return Finalize(&taskv1.Task{
		JobId: parent.JobId, Lane: parent.Lane, Kind: taskv1.Kind_KIND_POST, Mode: mode, Limits: parent.Limits,
		Scope: &taskv1.Task_Post{Post: s}, Trace: parent.Trace,
	})
}

// NewThread creates a replies task.
func NewThread(parent *taskv1.Task, mode fbv1.CrawlMode, s *taskv1.ThreadScope) *taskv1.Task {
	return Finalize(&taskv1.Task{
		JobId: parent.JobId, Lane: parent.Lane, Kind: taskv1.Kind_KIND_THREAD, Mode: mode, Limits: parent.Limits,
		Scope: &taskv1.Task_Thread{Thread: s}, Trace: parent.Trace,
	})
}

// NewMedia creates a download task. Media tasks are not part of job
// accounting: a job completes when its data is stored, not when media is.
func NewMedia(jobID string, m *fbv1.Media) *taskv1.Task {
	return Finalize(&taskv1.Task{
		JobId: jobID, Kind: taskv1.Kind_KIND_MEDIA, Lane: taskv1.Lane_LANE_P1,
		Scope: &taskv1.Task_Media{Media: &taskv1.MediaScope{Media: m}},
	})
}

// DefaultLimits builds job limits from config.
func DefaultLimits(c config.Limits, backfill bool) *taskv1.Limits {
	vp := c.VideoPolicy
	if backfill && c.BackfillVideoPolicy != "" {
		vp = c.BackfillVideoPolicy
	}
	return &taskv1.Limits{
		MaxAgeSeconds:       int64(c.MaxAgeDays) * 86400,
		MaxPosts:            c.MaxPosts,
		MaxDepth:            c.MaxDepth,
		IncludeComments:     c.IncludeComments,
		IncludeReplies:      c.IncludeReplies,
		IncludeMedia:        c.IncludeMedia,
		IncludeCommentMedia: c.IncludeCommentMedia,
		VideoPolicy:         ParseVideoPolicy(vp),
	}
}

// ParseVideoPolicy maps config strings to the enum.
func ParseVideoPolicy(s string) taskv1.VideoPolicy {
	switch strings.ToLower(s) {
	case "none":
		return taskv1.VideoPolicy_VIDEO_POLICY_NONE
	case "thumbnail":
		return taskv1.VideoPolicy_VIDEO_POLICY_THUMBNAIL
	case "hd":
		return taskv1.VideoPolicy_VIDEO_POLICY_HD
	default:
		return taskv1.VideoPolicy_VIDEO_POLICY_SD
	}
}

// NormalizeRef turns a page URL, vanity or numeric id into a canonical
// reference: "id:<n>" or "v:<lowercase vanity>".
func NormalizeRef(in string) (string, error) {
	s := strings.TrimSpace(in)
	if s == "" {
		return "", fmt.Errorf("empty page reference")
	}
	if strings.HasPrefix(s, "id:") || strings.HasPrefix(s, "v:") {
		return s, nil
	}
	if i := strings.Index(s, "facebook.com/"); i >= 0 {
		s = s[i+len("facebook.com/"):]
	} else if strings.Contains(s, "://") {
		if j := strings.Index(s[strings.Index(s, "://")+3:], "/"); j >= 0 {
			s = s[strings.Index(s, "://")+3+j+1:]
		}
	}
	if strings.HasPrefix(s, "profile.php") {
		if i := strings.Index(s, "id="); i >= 0 {
			id := s[i+3:]
			if j := strings.IndexAny(id, "&#/"); j >= 0 {
				id = id[:j]
			}
			if _, err := strconv.ParseInt(id, 10, 64); err == nil {
				return "id:" + id, nil
			}
		}
		return "", fmt.Errorf("invalid profile.php reference %q", in)
	}
	if strings.HasPrefix(s, "pages/") {
		// pages/<name>/<id>
		seg := strings.Split(strings.Trim(s, "/"), "/")
		if len(seg) >= 3 {
			if _, err := strconv.ParseInt(seg[2], 10, 64); err == nil {
				return "id:" + seg[2], nil
			}
		}
	}
	if j := strings.IndexAny(s, "/?#"); j >= 0 {
		s = s[:j]
	}
	if s == "" {
		return "", fmt.Errorf("invalid page reference %q", in)
	}
	if _, err := strconv.ParseInt(s, 10, 64); err == nil {
		return "id:" + s, nil
	}
	for _, r := range s {
		if !validVanityRune(r) {
			return "", fmt.Errorf("invalid page reference %q", in)
		}
	}
	return "v:" + strings.ToLower(s), nil
}

func validVanityRune(r rune) bool {
	return r == '.' || r == '-' || r == '_' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}

// RefID returns the numeric id of an "id:<n>" reference, and 0 for any other.
func RefID(ref string) int64 {
	s, ok := strings.CutPrefix(ref, "id:")
	if !ok {
		return 0
	}
	id, err := strconv.ParseInt(s, 10, 64)
	if err != nil || id <= 0 {
		return 0
	}
	return id
}

// RefPath returns the URL path for a normalised reference.
func RefPath(ref string) string {
	switch {
	case strings.HasPrefix(ref, "id:"):
		return "/" + strings.TrimPrefix(ref, "id:")
	case strings.HasPrefix(ref, "v:"):
		return "/" + strings.TrimPrefix(ref, "v:")
	}
	return "/" + ref
}

// MaxAgeCutoff returns the oldest post time a task accepts.
func MaxAgeCutoff(l *taskv1.Limits, now time.Time) time.Time {
	if l.GetMaxAgeSeconds() <= 0 {
		return time.Time{}
	}
	return now.Add(-time.Duration(l.MaxAgeSeconds) * time.Second)
}
