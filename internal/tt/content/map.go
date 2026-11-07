// Package content publishes the shared contract's events (securebrand.content.v1,
// Abdullah95k/securebrand-contracts) from tks's persister: for each record
// of tt.*.v1 whose contract content changed, one event on content.*, keyed
// and headed as the contract's registry says (M15;
// docs/tiktok/contract-mapping.md). tt.* stays as it is (standing
// instruction 1), and tks registers nothing in a shared registry.
//
// The mapping is pure (this file); the change gate (gate.go) keeps one row
// per event stream in tt.content_state, so an event goes out only when its
// content hash changed, counts at least once a day, and never for an older
// observation than the last one sent. The emitter (emit.go) produces the
// events before the gate's transaction commits: a failed produce rolls the
// gate back and the persister's batch is retried, and a commit that fails
// after the produce repeats the same event ids, which consumers ignore.
package content

import (
	"crypto/sha256"
	"encoding/hex"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
)

// Platform is TikTok's code in the contract's registry.
const Platform = "tt"

// Collector is tks's name in provenance.collector.
const Collector = "tks"

// roundedFrom is where TikTok starts rounding post and comment counts
// (T31): from 10,000 a count is served rounded.
const roundedFrom = 10_000

// ID returns the contract ID "tt:<kind>:<native id>".
func ID(kind, native string) string { return Platform + ":" + kind + ":" + native }

func idOf(kind string, n int64) string { return ID(kind, strconv.FormatInt(n, 10)) }

// collectorVersion is the build's VCS revision, or "dev".
var collectorVersion = func() string {
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			if s.Key == "vcs.revision" && s.Value != "" {
				return s.Value
			}
		}
	}
	return "dev"
}()

// Event is one mapped contract message, before the gate decides whether it
// goes out.
type Event struct {
	// Gate is the change gate's key: "<message kind>/<contract id>".
	Gate string
	// Topic and Key are where it goes (registry/topics.yaml).
	Topic, Key string
	// Observed is the observation's time; the envelope's version is its
	// microseconds.
	Observed time.Time
	// Hash is the contract's content hash (SHA-256 of the content fields
	// without the envelope and, for content, without counts).
	Hash []byte
	// TextHash is set for posts and comments read whole: a different text
	// later marks them edited.
	TextHash []byte
	// Daily: also sent once a day when unchanged (counts; the volume rule).
	Daily bool
	// Msg is the message; its envelope's event id and content hash are set
	// by Seal.
	Msg proto.Message
	env *contentv1.Envelope
	// seal sets the event id of a message without an envelope (a
	// candidate) from its content hash.
	seal func(hash string)
	// edited sets the message's edited flag and time.
	edited func(at time.Time)
}

// Seal sets the envelope's content hash and event id: SHA-256 of
// "<id>|<observed µs>|<content hash>", the same for a redelivered record.
func (e *Event) Seal() {
	if e.env == nil {
		if e.seal != nil {
			e.seal(hex.EncodeToString(e.Hash))
		}
		return
	}
	e.env.ContentHash = hex.EncodeToString(e.Hash)
	sum := sha256.Sum256([]byte(e.env.Id + "|" + strconv.FormatInt(e.env.Version, 10) + "|" + e.env.ContentHash))
	e.env.EventId = hex.EncodeToString(sum[:])
}

// Edited marks the message edited at t (a post's caption or a comment's
// text changed since an earlier version).
func (e *Event) Edited(at time.Time) {
	if e.edited != nil {
		e.edited(at)
	}
}

var deterministic = proto.MarshalOptions{Deterministic: true}

// hashOf is SHA-256 over m without its envelope (if it has one), after
// strip.
func hashOf(m proto.Message, strip func(proto.Message)) []byte {
	c := proto.Clone(m)
	if fd := c.ProtoReflect().Descriptor().Fields().ByName("envelope"); fd != nil {
		c.ProtoReflect().Clear(fd)
	}
	if strip != nil {
		strip(c)
	}
	b, _ := deterministic.Marshal(c)
	sum := sha256.Sum256(b)
	return sum[:]
}

func textHash(s string) []byte {
	sum := sha256.Sum256([]byte(s))
	return sum[:]
}

func observedAt(o *commonv1.Observation) time.Time {
	if o == nil || o.ObservedAt == nil {
		return time.Time{}
	}
	return o.ObservedAt.AsTime().UTC()
}

func mode(m commonv1.CrawlMode) contentv1.CrawlMode {
	switch m {
	case commonv1.CrawlMode_CRAWL_MODE_FULL:
		return contentv1.CrawlMode_CRAWL_MODE_FULL
	case commonv1.CrawlMode_CRAWL_MODE_INCREMENTAL:
		return contentv1.CrawlMode_CRAWL_MODE_INCREMENTAL
	case commonv1.CrawlMode_CRAWL_MODE_REFRESH:
		return contentv1.CrawlMode_CRAWL_MODE_REFRESH
	case commonv1.CrawlMode_CRAWL_MODE_RECRAWL:
		return contentv1.CrawlMode_CRAWL_MODE_RECRAWL
	}
	return contentv1.CrawlMode_CRAWL_MODE_UNSPECIFIED
}

// envelope builds a message's envelope from the record's observation.
func envelope(kind, entity string, native int64, o *commonv1.Observation, partial bool) *contentv1.Envelope {
	n := strconv.FormatInt(native, 10)
	at := observedAt(o)
	return &contentv1.Envelope{
		Platform: Platform, Kind: kind, Id: ID(entity, n), NativeId: n,
		ObservedAt: timestamppb.New(at), Version: at.UnixMicro(), Partial: partial,
		Provenance: &contentv1.Provenance{
			Collector: Collector, CollectorVersion: collectorVersion,
			ParserVersion:   strconv.FormatUint(uint64(o.GetParserVersion()), 10),
			SourceOperation: o.GetQueryName(), SourceVariant: o.GetDocId(), JobIds: o.GetJobIds(),
			Acquisition: contentv1.Acquisition_ACQUISITION_SCRAPED_LOGGED_OUT, Mode: mode(o.GetMode()),
			ExitCountry: o.GetExitCountry(),
		},
	}
}

func exact(t *timestamppb.Timestamp) *contentv1.Time {
	if t == nil || t.AsTime().IsZero() {
		return nil
	}
	return &contentv1.Time{At: t, Precision: contentv1.TimePrecision_TIME_PRECISION_EXACT}
}

// count is a served count: exact, or rounded when TikTok rounds it.
func count(v *int64, rounded bool) *contentv1.Count {
	if v == nil {
		return nil
	}
	e := contentv1.CountExactness_COUNT_EXACTNESS_EXACT
	if rounded {
		e = contentv1.CountExactness_COUNT_EXACTNESS_ROUNDED
	}
	return &contentv1.Count{Value: proto.Int64(*v), Exactness: e}
}

// itemCount is a post's or comment's count: rounded from 10,000 (T31).
func itemCount(v *int64) *contentv1.Count { return count(v, v != nil && *v >= roundedFrom) }

func extras(kv map[string]any) *structpb.Struct {
	if len(kv) == 0 {
		return nil
	}
	s, err := structpb.NewStruct(kv)
	if err != nil {
		return nil
	}
	return s
}

func mediaRef(id string, t contentv1.MediaType, r contentv1.MediaRole, pos int32) *contentv1.MediaRef {
	if id == "" {
		return nil
	}
	return &contentv1.MediaRef{MediaId: ID("media", id), Type: t, Role: r, Position: pos}
}

// AccountURL is an account's page.
func AccountURL(username string) string {
	if username == "" {
		return ""
	}
	return "https://www.tiktok.com/@" + username
}

// language is TikTok's language code, "un" (undetermined) as unknown.
func language(l string) string {
	if l == "un" {
		return ""
	}
	return l
}

// personRef is a person as an AccountRef (a comment's author, a mention).
func personRef(p *ttv1.Person) *contentv1.AccountRef {
	if p == nil {
		return nil
	}
	r := &contentv1.AccountRef{Platform: Platform, Handle: p.Username, DisplayName: p.Nickname,
		Url: AccountURL(p.Username), Verified: p.Verified, PictureUrl: p.AvatarUrl}
	if p.UserId != nil && *p.UserId != 0 {
		r.NativeId = strconv.FormatInt(*p.UserId, 10)
		r.Id = ID("account", r.NativeId)
	}
	if p.SecUid != "" {
		r.AltIds = map[string]string{"sec_uid": p.SecUid}
	}
	if r.Id == "" && r.Handle == "" {
		return nil
	}
	return r
}

// text maps a caption or comment with its ranges (UTF-16 offsets).
func text(s, lang string, rs []*ttv1.TextRange) *contentv1.Text {
	if s == "" {
		return nil
	}
	t := &contentv1.Text{Text: s, PlatformLanguage: language(lang)}
	for _, r := range rs {
		sp := &contentv1.TextSpan{Offset: r.Offset, Length: r.Length, Unit: contentv1.OffsetUnit_OFFSET_UNIT_UTF16, Value: r.Name}
		switch r.Type {
		case "mention":
			sp.Type = contentv1.SpanType_SPAN_TYPE_MENTION
			sp.Account = personRef(r.Person)
		case "hashtag":
			sp.Type = contentv1.SpanType_SPAN_TYPE_HASHTAG
		default:
			continue
		}
		t.Spans = append(t.Spans, sp)
	}
	return t
}

// Owner is what the persister knows of an account when it maps the
// account's posts: their URLs and Post.account.
type Owner struct {
	Username, Nickname, SecUID string
	Verified                   *bool
}

// fullSource reports whether an account record carries the account itself
// (the profile page or the user detail call), not a list's author with
// rounded counts and no bio.
func fullSource(a *ttv1.Account) bool {
	return a.Source == ttv1.AccountSource_ACCOUNT_SOURCE_PROFILE_PAGE || a.Source == ttv1.AccountSource_ACCOUNT_SOURCE_USER_DETAIL
}

func accountStatus(s ttv1.AccountStatus) (contentv1.Availability, bool) {
	switch s {
	case ttv1.AccountStatus_ACCOUNT_STATUS_UNSPECIFIED, ttv1.AccountStatus_ACCOUNT_STATUS_ACTIVE:
		return contentv1.Availability_AVAILABILITY_VISIBLE, true
	case ttv1.AccountStatus_ACCOUNT_STATUS_PRIVATE:
		return contentv1.Availability_AVAILABILITY_PRIVATE, true
	case ttv1.AccountStatus_ACCOUNT_STATUS_UNAVAILABLE:
		return contentv1.Availability_AVAILABILITY_UNAVAILABLE, true
	case ttv1.AccountStatus_ACCOUNT_STATUS_BANNED:
		return contentv1.Availability_AVAILABILITY_BANNED, true
	}
	return 0, false
}

// Account maps an account record to content.account.v1 and, with its
// counts, content.account_snapshot.v1. A list's author (rounded counts, no
// bio) maps to nothing: the daily page read carries the account, so its
// events never flap between the two sources. A status-only record (private,
// unavailable, banned) is a partial event with the status. Scope is the
// registry's decision and stays unset.
func Account(a *ttv1.Account) []*Event {
	avail, ok := accountStatus(a.Status)
	if !ok || a.UserId == 0 {
		return nil
	}
	at := observedAt(a.Obs)
	if avail != contentv1.Availability_AVAILABILITY_VISIBLE || !fullSource(a) {
		if avail == contentv1.Availability_AVAILABILITY_VISIBLE {
			return nil
		}
		env := envelope("account", "account", a.UserId, a.Obs, true)
		m := &contentv1.Account{Envelope: env, Status: &contentv1.Status{Availability: avail, Since: timestamppb.New(at)}}
		return []*Event{newEvent(m, env, topicAccount, nil, false)}
	}
	env := envelope("account", "account", a.UserId, a.Obs, false)
	m := &contentv1.Account{
		Envelope: env, Handle: a.Username, DisplayName: a.Nickname, Url: AccountURL(a.Username),
		Verified: a.Verified, IsPrivate: a.Private, IsBusiness: a.Organization,
		PlatformLanguage: language(a.Language), CreatedAt: exact(a.CreatedAt),
		HandleChangedAt: exact(a.UsernameModifiedAt), NameChangedAt: exact(a.NicknameModifiedAt),
		Picture: mediaRef(a.AvatarMediaId, contentv1.MediaType_MEDIA_TYPE_IMAGE, contentv1.MediaRole_MEDIA_ROLE_PROFILE_PICTURE, 0),
		Status:  &contentv1.Status{Availability: avail},
		Counts:  accountCounts(a.Counts),
	}
	if a.Organization != nil && *a.Organization {
		m.Type = contentv1.AccountType_ACCOUNT_TYPE_ORGANIZATION
	}
	if a.SecUid != "" {
		m.AltIds = map[string]string{"sec_uid": a.SecUid}
	}
	if a.Bio != "" {
		m.Bio = &contentv1.Text{Text: a.Bio}
	}
	if a.GetBioLink() != "" {
		m.Links = []*contentv1.Link{{Url: a.GetBioLink(), Source: contentv1.LinkSource_LINK_SOURCE_BIO}}
	}
	x := map[string]any{}
	if a.BioLinkRisk != nil {
		x["tt.bio_link_risk"] = float64(*a.BioLinkRisk)
	}
	if a.FollowingVisibility != nil {
		x["tt.following_visibility"] = float64(*a.FollowingVisibility)
	}
	m.Extras = extras(x)
	out := []*Event{newEvent(m, env, topicAccount, func(c proto.Message) { c.(*contentv1.Account).Counts = nil }, false)}
	if m.Counts != nil {
		senv := envelope("account_snapshot", "account", a.UserId, a.Obs, false)
		s := &contentv1.AccountSnapshot{Envelope: senv, Counts: m.Counts, Verified: a.Verified, IsPrivate: a.Private,
			Handle: a.Username, DisplayName: a.Nickname, Status: &contentv1.Status{Availability: avail}}
		out = append(out, newEvent(s, senv, topicAccountSnapshot, nil, false))
	}
	return out
}

func accountCounts(c *ttv1.AccountCounts) *contentv1.AccountCounts {
	if c == nil {
		return nil
	}
	r := !c.Exact
	out := &contentv1.AccountCounts{Followers: count(c.Followers, r), Following: count(c.Following, r),
		Posts: count(c.Videos, r), Likes: count(c.Hearts, r), LikesGiven: count(c.LikesGiven, r), Friends: count(c.Friends, r)}
	if proto.Size(out) == 0 {
		return nil
	}
	return out
}

func engagement(c *ttv1.PostCounts) *contentv1.Engagement {
	if c == nil {
		return nil
	}
	e := &contentv1.Engagement{Views: itemCount(c.Plays), Likes: itemCount(c.Likes), Comments: itemCount(c.Comments),
		Shares: itemCount(c.Shares), Saves: itemCount(c.Saves), Reposts: itemCount(c.Reposts)}
	if proto.Size(e) == 0 {
		return nil
	}
	return e
}

// PostURL is a post's page (/photo/ for photo posts); empty without the
// account's username.
func PostURL(username string, postID int64, photo bool) string {
	if username == "" {
		return ""
	}
	kind := "video"
	if photo {
		kind = "photo"
	}
	return AccountURL(username) + "/" + kind + "/" + strconv.FormatInt(postID, 10)
}

// Post maps a post record to content.post.v1. Counts ride along but are
// not content: a post whose counts alone changed is not sent again (its
// counts go on content.metrics.v1). An unavailable or private post is a
// partial event with the status and TikTok's code, never DELETED (a
// logged-out reader cannot tell deleted from removed). captions are the
// post's stored subtitle tracks (M17); a photo post's title is its own.
func Post(p *ttv1.Post, owner *Owner, captions []*contentv1.CaptionTrack) []*Event {
	if p.PostId == 0 {
		return nil
	}
	at := observedAt(p.Obs)
	switch p.Status {
	case ttv1.PostStatus_POST_STATUS_UNAVAILABLE, ttv1.PostStatus_POST_STATUS_PRIVATE:
		env := envelope("post", "post", p.PostId, p.Obs, true)
		if env.Provenance.ExitCountry == "" {
			env.Provenance.ExitCountry = strings.ToUpper(p.StatusRegion)
		}
		avail := contentv1.Availability_AVAILABILITY_UNAVAILABLE
		if p.Status == ttv1.PostStatus_POST_STATUS_PRIVATE {
			avail = contentv1.Availability_AVAILABILITY_PRIVATE
		}
		st := &contentv1.Status{Availability: avail, Since: timestamppb.New(at)}
		if p.StatusCode != nil {
			st.Reason = strconv.FormatInt(*p.StatusCode, 10)
		}
		m := &contentv1.Post{Envelope: env, AccountId: idOf("account", p.UserId), Status: st}
		return []*Event{newEvent(m, env, topicPost, nil, false)}
	}
	env := envelope("post", "post", p.PostId, p.Obs, false)
	photo := p.Kind == ttv1.PostKind_POST_KIND_PHOTO
	m := &contentv1.Post{
		Envelope: env, AccountId: idOf("account", p.UserId), PublishedAt: exact(p.CreatedAt),
		Text: text(p.Caption, p.TextLanguage, p.TextRanges), CreatedInCountry: strings.ToUpper(p.Region),
		Counts: engagement(p.Counts), Status: &contentv1.Status{Availability: contentv1.Availability_AVAILABILITY_VISIBLE},
		Labels: &contentv1.ContentLabels{Ad: p.Ad},
		Flags:  &contentv1.PostFlags{Pinned: p.Pinned, CommentsDisabled: p.CommentsDisabled},
	}
	if photo {
		m.Type, m.Subtype = contentv1.PostType_POST_TYPE_POST, "photo"
	} else {
		m.Type, m.Subtype = contentv1.PostType_POST_TYPE_SHORT_VIDEO, "video"
		m.Video = &contentv1.VideoInfo{}
		if p.DurationMs > 0 {
			m.Video.DurationMs = proto.Int64(p.DurationMs)
		}
		if p.Width > 0 && p.Height > 0 {
			m.Video.Width, m.Video.Height = proto.Int32(p.Width), proto.Int32(p.Height)
		}
		langs := map[string]bool{}
		for _, si := range p.Subtitles {
			if l := SubtitleLanguage(si.Language); l != "" && !langs[l] {
				langs[l] = true
				m.Video.CaptionLanguages = append(m.Video.CaptionLanguages, l)
			}
		}
		m.Captions = captions
	}
	if p.Title != "" {
		m.Title = &contentv1.Text{Text: p.Title, PlatformLanguage: language(p.TextLanguage)}
	}
	acct := &contentv1.AccountRef{Id: m.AccountId, Platform: Platform, NativeId: strconv.FormatInt(p.UserId, 10)}
	if owner != nil {
		m.Url = PostURL(owner.Username, p.PostId, photo)
		acct.Handle, acct.DisplayName, acct.Url, acct.Verified = owner.Username, owner.Nickname, AccountURL(owner.Username), owner.Verified
		if owner.SecUID != "" {
			acct.AltIds = map[string]string{"sec_uid": owner.SecUID}
		}
	}
	m.Account = acct
	seen := map[string]bool{}
	for _, h := range p.Hashtags {
		if n := strings.ToLower(h.Name); n != "" && !seen[n] {
			seen[n] = true
			m.Hashtags = append(m.Hashtags, n)
		}
	}
	mentioned := map[string]bool{}
	for _, r := range p.TextRanges {
		if r.Type != "mention" {
			continue
		}
		if ref := personRef(r.Person); ref != nil && !mentioned[ref.Id+"|"+ref.Handle] {
			mentioned[ref.Id+"|"+ref.Handle] = true
			m.Mentions = append(m.Mentions, ref)
		}
	}
	if p.MusicId != nil && *p.MusicId != 0 {
		m.Audio = &contentv1.AudioRef{AudioId: idOf("audio", *p.MusicId)}
	}
	if ref := mediaRef(p.CoverMediaId, contentv1.MediaType_MEDIA_TYPE_IMAGE, contentv1.MediaRole_MEDIA_ROLE_COVER, 0); ref != nil {
		m.Media = append(m.Media, ref)
	}
	for _, ph := range p.Photos {
		if ref := mediaRef(ph.MediaId, contentv1.MediaType_MEDIA_TYPE_IMAGE, contentv1.MediaRole_MEDIA_ROLE_CONTENT, ph.Position); ref != nil {
			m.Media = append(m.Media, ref)
		}
	}
	x := map[string]any{}
	if l := p.Labels; l != nil {
		m.Labels.AiGenerated = l.AiGenerated
		m.Labels.AiLabelText = l.AiDescription
		m.Labels.Warnings = l.Warnings
		if l.AigcLabelType != nil {
			x["tt.aigc_label_type"] = float64(*l.AigcLabelType)
		}
		if l.AnchorsJson != "" {
			x["tt.anchors"] = l.AnchorsJson
		}
	}
	if p.DiscoveredVia != ttv1.DiscoveredVia_DISCOVERED_VIA_UNSPECIFIED {
		x["tt.discovered_via"] = strings.ToLower(strings.TrimPrefix(p.DiscoveredVia.String(), "DISCOVERED_VIA_"))
	}
	m.Extras = extras(x)
	if p.Category != nil {
		m.Category = strconv.FormatInt(int64(*p.Category), 10)
	}
	if p.PlaylistId != "" {
		m.Playlists = []*contentv1.PlaylistRef{{PlaylistId: ID("playlist", p.PlaylistId)}}
	}
	e := newEvent(m, env, topicPost, func(c proto.Message) { c.(*contentv1.Post).Counts = nil }, false)
	e.TextHash = textHash(p.Caption)
	e.edited = func(at time.Time) {
		m.Flags.Edited = proto.Bool(true)
		m.Flags.EditedAt = exact(timestamppb.New(at))
	}
	return []*Event{e}
}

// PostMetrics maps a post's counts at a read to content.metrics.v1, keyed
// by the post; the gate sends it on change and at least once a day.
func PostMetrics(pm *ttv1.PostMetrics) []*Event {
	c := engagement(pm.Counts)
	if pm.PostId == 0 || c == nil {
		return nil
	}
	env := envelope("metrics", "post", pm.PostId, pm.Obs, false)
	m := &contentv1.Metrics{Envelope: env, TargetKind: "post", ParentId: idOf("account", pm.UserId), Counts: c}
	return []*Event{newEvent(m, env, topicMetrics, nil, true)}
}

// threading is a comment's place in its thread: root = TikTok's reply_id;
// parent = reply_to_reply_id when it names another reply, else the root
// (the contract's rule). A reply to a reply is at depth 2, below the reply
// it answers; TikTok shows both levels as one.
func threading(c *ttv1.Comment) (parent, root string, depth int32) {
	if c.Depth == 0 || c.ParentCommentId == nil || *c.ParentCommentId == 0 {
		return "", "", 0
	}
	root = idOf("comment", *c.ParentCommentId)
	if r := c.ReplyToCommentId; r != nil && *r != 0 && *r != *c.ParentCommentId {
		return idOf("comment", *r), root, 2
	}
	return root, root, 1
}

// Comment maps a comment or reply to content.comment.v1 and its counts to
// content.metrics.v1 (target comment, keyed by the post). A comment absent
// from a complete re-read is a partial event with status DELETED; one seen
// again is a new VISIBLE version.
func Comment(c *ttv1.Comment) []*Event {
	if c.CommentId == 0 || c.PostId == 0 {
		return nil
	}
	at := observedAt(c.Obs)
	parent, root, depth := threading(c)
	if c.Status == ttv1.CommentStatus_COMMENT_STATUS_DELETED {
		env := envelope("comment", "comment", c.CommentId, c.Obs, true)
		m := &contentv1.Comment{Envelope: env, PostId: idOf("post", c.PostId), ParentCommentId: parent, RootCommentId: root,
			Depth: depth, Status: &contentv1.Status{Availability: contentv1.Availability_AVAILABILITY_DELETED, Since: timestamppb.New(at)}}
		e := newEvent(m, env, topicComment, nil, false)
		e.Key = idOf("post", c.PostId)
		return []*Event{e}
	}
	if c.Redacted {
		return nil
	}
	env := envelope("comment", "comment", c.CommentId, c.Obs, false)
	m := &contentv1.Comment{
		Envelope: env, PostId: idOf("post", c.PostId), AccountId: idOf("account", c.UserId),
		ParentCommentId: parent, RootCommentId: root, Depth: depth,
		Text: text(c.Text, c.Language, c.TextRanges), PublishedAt: exact(c.CreatedAt),
		Flags:  &contentv1.CommentFlags{Pinned: c.PinnedByCreator, LikedByCreator: c.LikedByCreator},
		Counts: &contentv1.Engagement{Likes: itemCount(c.LikeCount), Replies: itemCount(c.ReplyCount)},
		Status: &contentv1.Status{Availability: contentv1.Availability_AVAILABILITY_VISIBLE},
	}
	if ref := personRef(c.Author); ref != nil {
		m.Author = &contentv1.Author{Account: ref}
		if c.Author.UserId != nil && *c.Author.UserId != 0 {
			m.Flags.ByPostOwner = proto.Bool(*c.Author.UserId == c.UserId)
		}
	}
	for i, id := range c.ImageMediaIds {
		if ref := mediaRef(id, contentv1.MediaType_MEDIA_TYPE_IMAGE, contentv1.MediaRole_MEDIA_ROLE_ATTACHMENT, int32(i)); ref != nil {
			m.Attachments = append(m.Attachments, ref)
		}
	}
	x := map[string]any{}
	if c.StickPosition != 0 {
		x["tt.stick_position"] = float64(c.StickPosition)
	}
	if len(c.Labels) > 0 {
		l := make([]any, len(c.Labels))
		for i, s := range c.Labels {
			l[i] = s
		}
		x["tt.comment_labels"] = l
	}
	if g := c.GetAuthor().GetPredictedAgeGroup(); g != "" {
		x["tt.predicted_age_group"] = g
	}
	m.Extras = extras(x)
	e := newEvent(m, env, topicComment, func(c proto.Message) { c.(*contentv1.Comment).Counts = nil }, false)
	e.Key = idOf("post", c.PostId)
	e.TextHash = textHash(c.Text)
	e.edited = func(at time.Time) {
		m.Flags.Edited = proto.Bool(true)
		m.Flags.EditedAt = exact(timestamppb.New(at))
	}
	out := []*Event{e}
	if c.LikeCount != nil || c.ReplyCount != nil {
		menv := envelope("metrics", "comment", c.CommentId, c.Obs, false)
		mm := &contentv1.Metrics{Envelope: menv, TargetKind: "comment", ParentId: idOf("post", c.PostId), Counts: m.Counts}
		me := newEvent(mm, menv, topicMetrics, nil, true)
		me.Key = idOf("post", c.PostId)
		out = append(out, me)
	}
	return out
}

// Audio maps a sound to content.audio.v1.
func Audio(mu *ttv1.Music) []*Event {
	if mu.MusicId == 0 {
		return nil
	}
	env := envelope("audio", "audio", mu.MusicId, mu.Obs, false)
	m := &contentv1.Audio{Envelope: env, Title: mu.Title, Artist: mu.AuthorName, Original: mu.Original, Album: mu.Album,
		Cover:       mediaRef(mu.CoverMediaId, contentv1.MediaType_MEDIA_TYPE_IMAGE, contentv1.MediaRole_MEDIA_ROLE_AUDIO_COVER, 0),
		Copyrighted: mu.Copyrighted, Commercial: mu.Commerce,
		Url: "https://www.tiktok.com/music/-" + strconv.FormatInt(mu.MusicId, 10)}
	if mu.DurationMs > 0 {
		m.DurationMs = proto.Int64(mu.DurationMs)
	}
	return []*Event{newEvent(m, env, topicAudio, nil, false)}
}

func mediaRole(k ttv1.MediaKind) (contentv1.MediaType, contentv1.MediaRole) {
	switch k {
	case ttv1.MediaKind_MEDIA_KIND_COVER, ttv1.MediaKind_MEDIA_KIND_ORIGIN_COVER, ttv1.MediaKind_MEDIA_KIND_DYNAMIC_COVER:
		return contentv1.MediaType_MEDIA_TYPE_IMAGE, contentv1.MediaRole_MEDIA_ROLE_COVER
	case ttv1.MediaKind_MEDIA_KIND_VIDEO:
		return contentv1.MediaType_MEDIA_TYPE_VIDEO, contentv1.MediaRole_MEDIA_ROLE_CONTENT
	case ttv1.MediaKind_MEDIA_KIND_PHOTO:
		return contentv1.MediaType_MEDIA_TYPE_IMAGE, contentv1.MediaRole_MEDIA_ROLE_CONTENT
	case ttv1.MediaKind_MEDIA_KIND_AVATAR:
		return contentv1.MediaType_MEDIA_TYPE_IMAGE, contentv1.MediaRole_MEDIA_ROLE_PROFILE_PICTURE
	case ttv1.MediaKind_MEDIA_KIND_MUSIC_COVER:
		return contentv1.MediaType_MEDIA_TYPE_IMAGE, contentv1.MediaRole_MEDIA_ROLE_AUDIO_COVER
	case ttv1.MediaKind_MEDIA_KIND_COMMENT_IMAGE:
		return contentv1.MediaType_MEDIA_TYPE_IMAGE, contentv1.MediaRole_MEDIA_ROLE_ATTACHMENT
	}
	return contentv1.MediaType_MEDIA_TYPE_UNSPECIFIED, contentv1.MediaRole_MEDIA_ROLE_UNSPECIFIED
}

// Media maps a media file's metadata to content.media.v1. Its signed
// source URL changes on every read, so it is not content: the event goes
// out when the file itself (size, dimensions, owner) is new or changed.
func Media(md *ttv1.Media) []*Event {
	t, role := mediaRole(md.Kind)
	if md.MediaId == "" || t == contentv1.MediaType_MEDIA_TYPE_UNSPECIFIED {
		return nil
	}
	owner, ownerKind := mediaOwner(md.UserId, md.PostId, md.CommentId, md.MusicId)
	if owner == "" {
		return nil
	}
	at := observedAt(md.Obs)
	env := envelope("media", "media", 0, md.Obs, false)
	env.Id, env.NativeId = ID("media", md.MediaId), md.MediaId
	env.ObservedAt, env.Version = timestamppb.New(at), at.UnixMicro()
	m := &contentv1.Media{Envelope: env, OwnerId: owner, OwnerKind: ownerKind, Type: t, Role: role,
		PlatformKind: strings.ToLower(strings.TrimPrefix(md.Kind.String(), "MEDIA_KIND_")), Variant: md.Variant,
		Position: md.Position, SourceUrl: md.SourceUrl, SourceExpiresAt: md.SourceExpiresAt, Mime: md.Mime}
	if md.Width > 0 && md.Height > 0 {
		m.Width, m.Height = proto.Int32(md.Width), proto.Int32(md.Height)
	}
	if md.DurationMs > 0 {
		m.DurationMs = proto.Int64(md.DurationMs)
	}
	if md.SizeBytes > 0 {
		m.SizeBytes = proto.Int64(md.SizeBytes)
	}
	if md.Bitrate > 0 {
		m.BitrateBps = proto.Int64(md.Bitrate)
	}
	return []*Event{newEvent(m, env, topicMedia, func(c proto.Message) {
		c.(*contentv1.Media).SourceUrl, c.(*contentv1.Media).SourceExpiresAt = "", nil
	}, false)}
}

func newEvent(m proto.Message, env *contentv1.Envelope, topic string, strip func(proto.Message), daily bool) *Event {
	return &Event{Gate: env.Kind + "/" + env.Id, Topic: topic, Key: env.Id, Observed: env.ObservedAt.AsTime(),
		Hash: hashOf(m, strip), Daily: daily, Msg: m, env: env}
}
