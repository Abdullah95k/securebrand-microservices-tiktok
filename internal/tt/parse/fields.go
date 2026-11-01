package parse

import (
	"crypto/sha1" //nolint:gosec // required by UUIDv5
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/walk"
)

// Report lists fields that could not be found (the walker's report).
type Report = walk.Report

// Version is recorded in Observation.parser_version on every record.
const Version = 1

// Options are what every parser needs besides the answer.
type Options struct {
	// AuthorKey returns a person's author key, HMAC-SHA256 of
	// "tt:user:<id>" under tks's secret (ADR 0007 §1). Nil leaves keys
	// empty (tests, the probe); the crawler always sets it.
	AuthorKey func(userID int64) []byte
	// Query and Variant name the registry operation that read the answer
	// (Observation.query_name and doc_id).
	Query, Variant string
}

func (o Options) obs() *commonv1.Observation {
	return &commonv1.Observation{QueryName: o.Query, DocId: o.Variant, ParserVersion: Version,
		Platform: commonv1.Platform_PLATFORM_TIKTOK}
}

func (o Options) key(userID int64) []byte {
	if o.AuthorKey == nil || userID == 0 {
		return nil
	}
	return o.AuthorKey(userID)
}

// Dropped counts records a parser left out, by reason.
type Dropped map[string]int

// Reasons for Dropped.
const (
	DropIDRange  = "id_out_of_range" // an id that is not a decimal int64
	DropNoUserID = "person_without_id"
	DropNoID     = "record_without_id"
)

func (d Dropped) add(reason string) { d[reason]++ }

// ID reads a TikTok id: a decimal string (or number) that must fit int64
// (ADR 0007 §1). ok is false for an absent, zero or empty id; inRange is
// false for one that does not fit, which callers drop and count instead
// of wrapping.
func ID(r gjson.Result) (id int64, ok, inRange bool) {
	s := ""
	switch r.Type {
	case gjson.String:
		s = strings.TrimSpace(r.Str)
	case gjson.Number:
		s = r.Raw
	default:
		return 0, false, true
	}
	if s == "" || s == "0" {
		return 0, false, true
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v < 0 {
		return 0, false, false
	}
	return v, true, true
}

// IDTime is the time a post or comment id carries in its top 32 bits
// (T8, confirmed): the upload time, which for a scheduled post is before
// it went public.
func IDTime(id int64) time.Time { return time.Unix(id>>32, 0).UTC() }

// count reads a count, preferring statsV2's exact string over stats'
// number, which TikTok rounds (har-findings §3).
func count(v2, v1 gjson.Result) *int64 {
	if v2.Exists() && v2.Type != gjson.Null {
		if n, err := strconv.ParseInt(strings.TrimSpace(v2.String()), 10, 64); err == nil {
			return proto.Int64(n)
		}
	}
	if v1.Exists() && v1.Type == gjson.Number {
		return proto.Int64(v1.Int())
	}
	return nil
}

func optBool(r gjson.Result) *bool {
	if r.Type == gjson.True || r.Type == gjson.False {
		return proto.Bool(r.Bool())
	}
	return nil
}

func unix(r gjson.Result) *timestamppb.Timestamp { return walk.Unix(r) }

// mediaNS namespaces tks's deterministic media ids, so they never collide
// with fbs's or igs's (ADR 0007 §1).
var mediaNS = uuid.Must(uuid.Parse("1191d4a7-c535-4a55-991b-8ad65bd560d1"))

// MediaID is the UUIDv5 of "<owner id>|<kind>|<variant>".
func MediaID(owner int64, kind ttv1.MediaKind, variant string) string {
	name := strconv.FormatInt(owner, 10) + "|" + strings.ToLower(strings.TrimPrefix(kind.String(), "MEDIA_KIND_")) + "|" + variant
	// UUIDv5 is defined over SHA-1 (RFC 9562); this is an identifier, not security.
	return uuid.NewHash(sha1.New(), mediaNS, []byte(name), 5).String() //nolint:gosec
}

// URLExpiry reads the expiry of a signed TikTok CDN link: x-expires on
// images, expire on video files (har-findings §6), both Unix seconds.
func URLExpiry(raw string) *timestamppb.Timestamp {
	u, err := url.Parse(raw)
	if err != nil {
		return nil
	}
	q := u.Query()
	for _, k := range []string{"x-expires", "expire"} {
		if v, err := strconv.ParseInt(q.Get(k), 10, 64); err == nil && v > 0 {
			return timestamppb.New(time.Unix(v, 0).UTC())
		}
	}
	return nil
}

// statusCode reads an answer's status: statusCode on the web app's calls
// and pages, status_code on comments.
func statusCode(r gjson.Result) (int64, bool) {
	s := walk.First(r, "statusCode", "status_code")
	return s.Int(), s.Exists()
}

func itoa(v int64) string { return strconv.FormatInt(v, 10) }
