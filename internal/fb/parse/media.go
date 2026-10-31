package parse

import (
	"github.com/tidwall/gjson"

	fbv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/walk"
)

// Media typenames collected by the parsers.
var mediaTypenames = []string{"Photo", "Video", "Sticker", "GenericAttachmentMedia", "Gif"}

func isMedia(tn string) bool {
	for _, t := range mediaTypenames {
		if t == tn {
			return true
		}
	}
	return false
}

// Owner identifies who a media item belongs to.
type Owner struct {
	Type      string // post | comment | page
	ID        int64
	PostID    int64
	PageID    int64
	CommentID int64
	Comment   bool // comment attachment kinds
	Reel      bool
}

// MediaFromNode converts a media node into one Media per downloadable
// variant (original photo; sd/hd/thumbnail for video).
func MediaFromNode(n gjson.Result, typename string, o Owner) []*fbv1.Media {
	fbID := n.Get("id").String()
	alt := walk.First(n, "accessibility_caption", "alt_text", "accessibility_label").String()
	mk := func(variant, url string, kind fbv1.MediaKind, w, h int64) *fbv1.Media {
		if url == "" {
			return nil
		}
		id := fbID
		if id == "" {
			id = url
		}
		m := &fbv1.Media{
			MediaId: MediaID(id, variant), FbMediaId: id, Kind: kind, Variant: variant,
			OwnerType: o.Type, OwnerId: o.ID, PostId: o.PostID, PageId: o.PageID,
			SourceUrl: url, SourceExpiresAt: walk.URLExpiry(url), Width: int32(w), Height: int32(h), AltText: alt,
		}
		if o.CommentID != 0 {
			cid := o.CommentID
			m.CommentId = &cid
		}
		return m
	}
	var out []*fbv1.Media
	add := func(m *fbv1.Media) {
		if m != nil {
			out = append(out, m)
		}
	}
	switch typename {
	case "Photo":
		img := walk.First(n, "photo_image", "image", "viewer_image", "large_share_image", "preview_image", "blurred_image")
		kind := fbv1.MediaKind_MEDIA_KIND_PHOTO
		if o.Comment {
			kind = fbv1.MediaKind_MEDIA_KIND_COMMENT_PHOTO
		}
		add(mk("original", img.Get("uri").String(), kind, img.Get("width").Int(), img.Get("height").Int()))
	case "Video":
		kind := fbv1.MediaKind_MEDIA_KIND_VIDEO
		if o.Reel || n.Get("is_reel").Bool() {
			kind = fbv1.MediaKind_MEDIA_KIND_REEL
		}
		if o.Comment {
			kind = fbv1.MediaKind_MEDIA_KIND_COMMENT_VIDEO
		}
		w, h := walk.First(n, "width", "original_width").Int(), walk.First(n, "height", "original_height").Int()
		durMS := walk.First(n, "playable_duration_in_ms").Int()
		if durMS == 0 {
			durMS = int64(walk.First(n, "length_in_second", "playable_duration").Float() * 1000)
		}
		dash := walk.First(n, "dash_manifest_url", "dash_manifest_urls.0.manifest_url").String()
		thumb := walk.First(n, "preferred_thumbnail.image.uri", "thumbnailImage.uri", "thumbnail_image.uri", "image.uri").String()
		for _, v := range []struct {
			variant string
			url     string
		}{
			{"sd", walk.First(n, "browser_native_sd_url", "playable_url", "videoDeliveryLegacyFields.browser_native_sd_url").String()},
			{"hd", walk.First(n, "browser_native_hd_url", "playable_url_quality_hd", "videoDeliveryLegacyFields.browser_native_hd_url").String()},
		} {
			if m := mk(v.variant, v.url, kind, w, h); m != nil {
				m.DurationMs = durMS
				m.DashManifestUrl = dash
				m.ThumbnailUrl = thumb
				m.Mime = "video/mp4"
				add(m)
			}
		}
		add(mk("thumbnail", thumb, fbv1.MediaKind_MEDIA_KIND_THUMBNAIL, 0, 0))
	case "Sticker":
		img := walk.First(n, "image", "sticker_image", "preview_image")
		add(mk("original", img.Get("uri").String(), fbv1.MediaKind_MEDIA_KIND_STICKER, img.Get("width").Int(), img.Get("height").Int()))
	case "GenericAttachmentMedia", "Gif":
		if anim := walk.First(n, "animated_image", "animated_gif"); anim.Exists() {
			add(mk("original", anim.Get("uri").String(), fbv1.MediaKind_MEDIA_KIND_COMMENT_GIF, anim.Get("width").Int(), anim.Get("height").Int()))
			break
		}
		img := walk.First(n, "image", "large_share_image")
		kind := fbv1.MediaKind_MEDIA_KIND_THUMBNAIL
		if o.Comment {
			kind = fbv1.MediaKind_MEDIA_KIND_COMMENT_PHOTO
		}
		add(mk("link_preview", img.Get("uri").String(), kind, img.Get("width").Int(), img.Get("height").Int()))
	}
	return out
}
