package parse

import (
	"strings"

	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/htmlx"
)

// PostBundle is one post with what its item carries besides the post.
type PostBundle struct {
	Post    *ttv1.Post
	Metrics *ttv1.PostMetrics // the counts at this read (captured_at is the emitter's)
	Music   *ttv1.Music
	Media   []*ttv1.Media // covers, photos, video renditions, the music cover
}

// PostListResult is one parsed page of a post list.
type PostListResult struct {
	Posts []PostBundle
	// Account is the posts' author as the first item carries it
	// (ACCOUNT_SOURCE_POST_LIST), with its avatar in AccountMedia: the
	// creator list refreshes the account on every check (C2, C3).
	Account      *ttv1.Account
	AccountMedia []*ttv1.Media
	// HasMore is hasMore on the app's list and hasMorePrevious on the
	// creator list, which has no cursor of its own (C2 run 1): its next
	// cursor is a time (client.NextCreatorCursor).
	HasMore bool
	// Cursor is the answer's cursor (the app's list), "" when it has none.
	Cursor string
	// Filtered is set when TikTok said it withheld posts from this viewer
	// (filter_reasons, T30).
	Filtered   bool
	StatusCode int64
	Dropped    Dropped
}

// PostList parses a page of /api/creator/item_list/ or /api/post/item_list/.
// Both carry complete items (T9). An empty body, which is how TikTok
// refuses a call (T4), is reported missing; an error statusCode is not
// drift and is returned for the caller to class. A page without items is
// not drift either: the creator list answers a window before its cursor
// (C3 §4).
func PostList(body []byte, o Options) (PostListResult, Report) {
	var rep Report
	res := PostListResult{Dropped: Dropped{}}
	if !gjson.ValidBytes(body) || len(strings.TrimSpace(string(body))) == 0 {
		rep.AddMissing("body")
		return res, rep
	}
	doc := gjson.ParseBytes(body)
	code, ok := statusCode(doc)
	res.StatusCode = code
	if !ok {
		rep.AddMissing("statusCode")
	}
	if code != 0 {
		return res, rep
	}
	res.HasMore = firstOf(doc, "hasMore", "hasMorePrevious").Bool()
	if c := doc.Get("cursor"); c.Exists() {
		res.Cursor = c.String()
	}
	res.Filtered = doc.Get("filter_reasons").Exists()
	items := doc.Get("itemList")
	if !items.Exists() {
		rep.AddOptional("itemList")
	}
	seen := map[int64]bool{}
	for _, it := range items.Array() {
		if id, ok, _ := ID(it.Get("id")); ok && seen[id] {
			continue // an item repeated in one page is read once
		}
		b, ok := post(it, ttv1.DiscoveredVia_DISCOVERED_VIA_LIST, o, &rep, res.Dropped)
		if !ok {
			continue
		}
		seen[b.Post.PostId] = true
		res.Posts = append(res.Posts, b)
		if res.Account == nil && it.Get("author.id").Exists() {
			res.Account, res.AccountMedia = account(it.Get("author"), it.Get("authorStats"), it.Get("authorStatsV2"),
				ttv1.AccountSource_ACCOUNT_SOURCE_POST_LIST, o, &Report{}, res.Dropped)
		}
	}
	return res, rep
}

// PostPageResult is one parsed post page.
type PostPageResult struct {
	// Post is nil when the page holds no post; StatusCode says why.
	Post       *PostBundle
	Account    *ttv1.Account
	StatusCode int64
	Dropped    Dropped
}

// PostPage parses a video post page (/@<username>/video/<id>): the
// rehydration document's webapp.video-detail. Photo pages are
// client-rendered shells (C2 run 1, T19) and report the scope missing.
func PostPage(html []byte, o Options) (PostPageResult, Report) {
	var rep Report
	res := PostPageResult{Dropped: Dropped{}}
	vd, ok := htmlx.Scope(html, htmlx.ScopeVideoDetail)
	if !ok {
		rep.AddMissing(htmlx.ScopeVideoDetail)
		return res, rep
	}
	res.StatusCode, _ = statusCode(vd)
	it := vd.Get("itemInfo.itemStruct")
	if !it.Get("id").Exists() {
		if res.StatusCode == 0 {
			rep.AddMissing("itemInfo.itemStruct.id")
		}
		return res, rep
	}
	if b, ok := post(it, ttv1.DiscoveredVia_DISCOVERED_VIA_POST_PAGE, o, &rep, res.Dropped); ok {
		res.Post = &b
	}
	if it.Get("author.id").Exists() {
		res.Account, _ = account(it.Get("author"), it.Get("authorStats"), it.Get("authorStatsV2"),
			ttv1.AccountSource_ACCOUNT_SOURCE_POST_LIST, o, &Report{}, res.Dropped)
	}
	return res, rep
}

// post builds a post from a list item or a page's itemStruct.
func post(it gjson.Result, via ttv1.DiscoveredVia, o Options, rep *Report, dropped Dropped) (PostBundle, bool) {
	id, ok, inRange := ID(it.Get("id"))
	if !ok {
		if inRange {
			dropped.add(DropNoID)
		} else {
			dropped.add(DropIDRange)
		}
		return PostBundle{}, false
	}
	created := unix(it.Get("createTime"))
	if created == nil {
		rep.AddMissing("item.createTime")
		return PostBundle{}, false
	}
	userID, _, uin := ID(it.Get("author.id"))
	if !uin {
		dropped.add(DropIDRange)
		return PostBundle{}, false
	}
	p := &ttv1.Post{Obs: o.obs(), PostId: id, UserId: userID, Kind: ttv1.PostKind_POST_KIND_VIDEO, CreatedAt: created,
		Caption: it.Get("desc").String(), Ad: optBool(it.Get("isAd")), PlaylistId: it.Get("playlistId").String(),
		Region: it.Get("locationCreated").String(), TextLanguage: it.Get("textLanguage").String(),
		Status: ttv1.PostStatus_POST_STATUS_VISIBLE, DiscoveredVia: via}
	if userID == 0 {
		rep.AddMissing("item.author.id")
	}
	if it.Get("privateItem").Bool() {
		p.Status = ttv1.PostStatus_POST_STATUS_PRIVATE
	}
	// The creator list flags pinned posts only (isPinnedItem: true, T10), so
	// on a list a missing flag means not pinned; a page says nothing.
	if pin := it.Get("isPinnedItem"); pin.Exists() {
		p.Pinned = proto.Bool(pin.Bool())
	} else if via == ttv1.DiscoveredVia_DISCOVERED_VIA_LIST {
		p.Pinned = proto.Bool(false)
	}
	if c := it.Get("CategoryType"); c.Exists() {
		p.Category = proto.Int32(int32(c.Int()))
	}
	if s := it.Get("itemCommentStatus"); s.Exists() {
		p.CommentsDisabled = proto.Bool(s.Int() != 0)
	}
	p.Labels = labels(it)
	p.Hashtags, p.TextRanges = textExtra(it, o, dropped)
	st, st2 := it.Get("stats"), it.Get("statsV2")
	if !st.Exists() && !st2.Exists() {
		rep.AddMissing("item.stats")
	}
	p.Counts = &ttv1.PostCounts{
		Plays: count(st2.Get("playCount"), st.Get("playCount")), Likes: count(st2.Get("diggCount"), st.Get("diggCount")),
		Comments: count(st2.Get("commentCount"), st.Get("commentCount")), Shares: count(st2.Get("shareCount"), st.Get("shareCount")),
		Saves: count(st2.Get("collectCount"), st.Get("collectCount")), Reposts: count(st2.Get("repostCount"), gjson.Result{}),
	}
	b := PostBundle{Post: p, Metrics: &ttv1.PostMetrics{Obs: o.obs(), PostId: id, UserId: userID, PostCreatedAt: created, Counts: p.Counts}}
	md := mediaCtx{o: o, userID: userID, postID: id, created: created}
	v := it.Get("video")
	p.DurationMs = v.Get("duration").Int() * 1000
	p.Width, p.Height = int32(v.Get("width").Int()), int32(v.Get("height").Int())
	if ip := it.Get("imagePost"); ip.Exists() {
		p.Kind = ttv1.PostKind_POST_KIND_PHOTO
		if c := ip.Get("cover"); c.Exists() {
			if m := md.image(c, ttv1.MediaKind_MEDIA_KIND_COVER, 0); m != nil {
				p.CoverMediaId = m.MediaId
				b.Media = append(b.Media, m)
			}
		}
		for i, img := range ip.Get("images").Array() {
			if m := md.image(img, ttv1.MediaKind_MEDIA_KIND_PHOTO, int32(i)); m != nil {
				p.Photos = append(p.Photos, &ttv1.Photo{MediaId: m.MediaId, Position: int32(i), Width: m.Width, Height: m.Height})
				b.Media = append(b.Media, m)
			}
		}
	} else {
		if m := md.link(v.Get("cover").String(), ttv1.MediaKind_MEDIA_KIND_COVER, "original"); m != nil {
			m.Width, m.Height = p.Width, p.Height
			p.CoverMediaId = m.MediaId
			b.Media = append(b.Media, m)
		}
		for _, k := range []struct {
			path string
			kind ttv1.MediaKind
		}{{"originCover", ttv1.MediaKind_MEDIA_KIND_ORIGIN_COVER}, {"dynamicCover", ttv1.MediaKind_MEDIA_KIND_DYNAMIC_COVER}} {
			if m := md.link(v.Get(k.path).String(), k.kind, "original"); m != nil {
				b.Media = append(b.Media, m)
			}
		}
		b.Media = append(b.Media, md.videos(v)...)
	}
	if mu := it.Get("music"); mu.Exists() {
		b.Music = music(mu, o, md, &b, dropped)
		if b.Music != nil {
			p.MusicId = proto.Int64(b.Music.MusicId)
		}
	}
	return b, true
}

func labels(it gjson.Result) *ttv1.PostLabels {
	l := &ttv1.PostLabels{AiGenerated: optBool(it.Get("ShowAIGC"))}
	if t := it.Get("aigcLabelType"); t.Exists() {
		l.AigcLabelType = proto.Int32(int32(t.Int()))
	}
	for _, w := range it.Get("warnInfo").Array() {
		if t := w.Get("text").String(); t != "" {
			l.Warnings = append(l.Warnings, t)
		}
	}
	if a := it.Get("anchors"); a.IsArray() && len(a.Array()) > 0 {
		l.AnchorsJson = a.Raw
	}
	return l
}

// textExtra reads a caption's hashtags and mentions. challenges carry the
// hashtags' ids; textExtra their places (UTF-16 offsets, start to end).
func textExtra(it gjson.Result, o Options, dropped Dropped) ([]*ttv1.Hashtag, []*ttv1.TextRange) {
	ids := map[string]int64{}
	var tags []*ttv1.Hashtag
	seen := map[string]bool{}
	add := func(name string, id int64) {
		k := strings.ToLower(name)
		if k == "" || seen[k] {
			return
		}
		seen[k] = true
		tags = append(tags, &ttv1.Hashtag{Id: id, Name: name})
	}
	for _, c := range it.Get("challenges").Array() {
		id, _, _ := ID(c.Get("id"))
		ids[strings.ToLower(c.Get("title").String())] = id
	}
	var ranges []*ttv1.TextRange
	for _, te := range it.Get("textExtra").Array() {
		start, end := te.Get("start").Int(), te.Get("end").Int()
		r := &ttv1.TextRange{Offset: int32(start), Length: int32(end - start)}
		if h := te.Get("hashtagName").String(); h != "" {
			r.Type, r.Name = "hashtag", h
			add(h, ids[strings.ToLower(h)])
		} else if te.Get("userId").Exists() || te.Get("userUniqueId").Exists() {
			uid, ok, _ := ID(te.Get("userId"))
			if !ok {
				dropped.add(DropNoUserID)
				continue
			}
			r.Type = "mention"
			r.Person = &ttv1.Person{AuthorKey: o.key(uid), UserId: proto.Int64(uid), SecUid: te.Get("secUid").String(),
				Username: te.Get("userUniqueId").String()}
		} else {
			continue
		}
		ranges = append(ranges, r)
	}
	for _, c := range it.Get("challenges").Array() { // hashtags the text did not mark
		id, _, _ := ID(c.Get("id"))
		add(c.Get("title").String(), id)
	}
	return tags, ranges
}

func music(mu gjson.Result, o Options, md mediaCtx, b *PostBundle, dropped Dropped) *ttv1.Music {
	id, ok, inRange := ID(mu.Get("id"))
	if !ok {
		if !inRange {
			dropped.add(DropIDRange)
		}
		return nil
	}
	m := &ttv1.Music{Obs: o.obs(), MusicId: id, Title: mu.Get("title").String(), AuthorName: mu.Get("authorName").String(),
		Original: optBool(mu.Get("original")), DurationMs: mu.Get("duration").Int() * 1000, Album: mu.Get("album").String(),
		Copyrighted: optBool(mu.Get("isCopyrighted")), Commerce: optBool(mu.Get("is_commerce_music"))}
	if c := firstOf(mu, "coverLarge", "coverMedium", "coverThumb").String(); c != "" {
		cm := md.link(c, ttv1.MediaKind_MEDIA_KIND_MUSIC_COVER, "original")
		cm.MusicId, cm.MediaId = proto.Int64(id), MediaID(id, ttv1.MediaKind_MEDIA_KIND_MUSIC_COVER, "original")
		cm.PostId = nil
		m.CoverMediaId = cm.MediaId
		b.Media = append(b.Media, cm)
	}
	return m
}

// mediaCtx builds a post's media records.
type mediaCtx struct {
	o       Options
	userID  int64
	postID  int64
	created *timestamppb.Timestamp
}

func (c mediaCtx) link(u string, kind ttv1.MediaKind, variant string) *ttv1.Media {
	if u == "" {
		return nil
	}
	return &ttv1.Media{Obs: c.o.obs(), MediaId: MediaID(c.postID, kind, variant), Kind: kind, Variant: variant,
		UserId: c.userID, PostId: proto.Int64(c.postID), SourceUrl: u, SourceExpiresAt: URLExpiry(u), PostCreatedAt: c.created}
}

// image reads one of a photo post's images ({imageURL.urlList, imageWidth,
// imageHeight}).
func (c mediaCtx) image(img gjson.Result, kind ttv1.MediaKind, pos int32) *ttv1.Media {
	m := c.link(img.Get("imageURL.urlList.0").String(), kind, "original")
	if m == nil {
		return nil
	}
	if kind == ttv1.MediaKind_MEDIA_KIND_PHOTO { // one id per photo
		m.MediaId = MediaID(c.postID, kind, "original-"+itoa(int64(pos)))
	}
	m.Position, m.Width, m.Height = pos, int32(img.Get("imageWidth").Int()), int32(img.Get("imageHeight").Int())
	return m
}

// videos maps the renditions to sd (the lowest bitrate) and hd (the
// highest) (ADR 0007 §7); without bitrateInfo, playAddr is sd.
func (c mediaCtx) videos(v gjson.Result) []*ttv1.Media {
	var lo, hi gjson.Result
	for _, b := range v.Get("bitrateInfo").Array() {
		if b.Get("PlayAddr.UrlList.0").String() == "" {
			continue
		}
		if !lo.Exists() || b.Get("Bitrate").Int() < lo.Get("Bitrate").Int() {
			lo = b
		}
		if !hi.Exists() || b.Get("Bitrate").Int() > hi.Get("Bitrate").Int() {
			hi = b
		}
	}
	dur := v.Get("duration").Int() * 1000
	if !lo.Exists() {
		m := c.link(v.Get("playAddr").String(), ttv1.MediaKind_MEDIA_KIND_VIDEO, "sd")
		if m == nil {
			return nil
		}
		m.Width, m.Height, m.DurationMs, m.Mime = int32(v.Get("width").Int()), int32(v.Get("height").Int()), dur, "video/mp4"
		return []*ttv1.Media{m}
	}
	r := func(b gjson.Result, variant string) *ttv1.Media {
		pa := b.Get("PlayAddr")
		m := c.link(pa.Get("UrlList.0").String(), ttv1.MediaKind_MEDIA_KIND_VIDEO, variant)
		m.Width, m.Height, m.SizeBytes = int32(pa.Get("Width").Int()), int32(pa.Get("Height").Int()), pa.Get("DataSize").Int()
		m.Bitrate, m.DurationMs, m.Mime = b.Get("Bitrate").Int(), dur, "video/mp4"
		return m
	}
	out := []*ttv1.Media{r(lo, "sd")}
	if hi.Get("Bitrate").Int() != lo.Get("Bitrate").Int() {
		out = append(out, r(hi, "hd"))
	}
	return out
}
