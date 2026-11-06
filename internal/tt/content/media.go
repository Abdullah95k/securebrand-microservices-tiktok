package content

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"
	mediav1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/media/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
)

// StoredFile is a media file tks-media-downloader stored, as the
// downloader and the ledger know it: the file's own ids, owner and
// object, from its task or its row in tt.media.
type StoredFile struct {
	// MediaID is tks's UUIDv5 (tt.media.media_id).
	MediaID string
	Kind    ttv1.MediaKind
	Variant string
	// The owner: the comment, else the post, else the sound, else the
	// account (UserID is also the file's account).
	UserID    int64
	PostID    *int64
	CommentID *int64
	MusicID   *int64
	Width     int32
	Height    int32
	// DurationMs is 0 when unknown.
	DurationMs int64
	// The object: SHA256 is the raw digest.
	Bucket, Key, ContentType string
	Size                     int64
	SHA256                   []byte
	StoredAt                 time.Time
	JobIDs                   []string
}

// Handoff is a MediaReady's settings (tt.content.media).
type Handoff struct {
	DeleteAfter       time.Duration
	RequiredConsumers []string
}

// MediaID returns tks's media UUID from the contract's media ID
// ("tt:media:<uuid>"); a bare UUID is returned as it is (tks's own topics).
func MediaID(id string) string {
	return strings.TrimPrefix(id, ID("media", ""))
}

// MediaReady maps a stored file to content.media.ready.v1 (key: the media
// ID). attempt is 1 for the first announcement and one more for each
// after a consumer's retryable failure; the event id is the same for the
// same announcement, so a redelivery is one event. delete_after is the
// stored time plus the setting, the same on every announcement: a hard
// deadline for the deleting service (tks deletes nothing, D5). Nil when
// the file has no owner or kind the contract knows.
func MediaReady(f StoredFile, attempt int32, h Handoff) *mediav1.MediaReady {
	t, role := mediaRole(f.Kind)
	owner, ownerKind := mediaOwner(f.UserID, f.PostID, f.CommentID, f.MusicID)
	if f.MediaID == "" || t == contentv1.MediaType_MEDIA_TYPE_UNSPECIFIED || owner == "" {
		return nil
	}
	id := ID("media", f.MediaID)
	sum := sha256.Sum256([]byte(id + "|ready|" + strconv.FormatInt(int64(attempt), 10)))
	m := &mediav1.MediaReady{
		EventId: hex.EncodeToString(sum[:]), Platform: Platform, MediaId: id,
		OwnerId: owner, OwnerKind: ownerKind, Type: t, Role: role, Variant: f.Variant,
		Object: &mediav1.StorageObject{Bucket: f.Bucket, Key: f.Key, SizeBytes: f.Size, Sha256: hex.EncodeToString(f.SHA256),
			ContentType: f.ContentType},
		StoredAt:          timestamppb.New(f.StoredAt),
		DeleteAfter:       timestamppb.New(f.StoredAt.Add(h.DeleteAfter)),
		RequiredConsumers: h.RequiredConsumers,
		Attempt:           attempt,
		Provenance: &contentv1.Provenance{Collector: Collector, CollectorVersion: collectorVersion, JobIds: f.JobIDs,
			Acquisition: contentv1.Acquisition_ACQUISITION_SCRAPED_LOGGED_OUT},
	}
	if f.UserID != 0 {
		m.AccountId = idOf("account", f.UserID)
	}
	if f.Width > 0 && f.Height > 0 {
		m.Width, m.Height = proto.Int32(f.Width), proto.Int32(f.Height)
	}
	if f.DurationMs > 0 {
		m.DurationMs = proto.Int64(f.DurationMs)
	}
	return m
}

// mediaOwner is a file's owner ID and kind: its comment, else its post,
// else its sound, else its account.
func mediaOwner(user int64, post, comment, music *int64) (string, string) {
	switch {
	case comment != nil && *comment != 0:
		return idOf("comment", *comment), "comment"
	case post != nil && *post != 0:
		return idOf("post", *post), "post"
	case music != nil && *music != 0:
		return idOf("audio", *music), "audio"
	case user != 0:
		return idOf("account", user), "account"
	}
	return "", ""
}
