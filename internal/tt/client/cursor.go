package client

import (
	"time"

	"github.com/tidwall/gjson"
)

// TikTokEpoch is before TikTok's first post (2016-09-01): a creator-list
// cursor older than this is past every account's history.
var TikTokEpoch = time.Date(2016, 9, 1, 0, 0, 0, 0, time.UTC)

// lookbackStep is how far back an empty creator-list page moves the cursor.
// The list answers only a window of time before its cursor (yt-dlp reports
// about a week), so an empty page does not mean the account has no older
// posts.
const lookbackStep = 7 * 24 * time.Hour

// NextCreatorCursor returns the cursor for the page of /api/creator/item_list/
// after page, which was read with cursor prev (both in ms). The answer has
// no cursor of its own (the C2 run, 2026-10-31): the cursor is a time, and
// the next page starts at the page's oldest createTime. The cursor keeps
// the rest of that second, so a post sharing it is not skipped; the oldest
// post then comes back first on the next page, and callers drop it by id.
// An empty page steps back a week. more is false once hasMorePrevious is
// false or the cursor is older than TikTok.
func NextCreatorCursor(page gjson.Result, prev int64) (next int64, more bool) {
	oldest := int64(0)
	for _, it := range page.Get("itemList").Array() {
		if t := it.Get("createTime").Int(); t > 0 && (oldest == 0 || t < oldest) {
			oldest = t
		}
	}
	switch {
	case oldest == 0:
		next = prev - lookbackStep.Milliseconds()
	case oldest*1000+999 < prev:
		next = oldest*1000 + 999
	default:
		// Every post on the page shares one second with the cursor: leave
		// that second behind.
		next = oldest*1000 - 1
	}
	more = page.Get("hasMorePrevious").Bool() && next > TikTokEpoch.UnixMilli()
	return next, more
}
