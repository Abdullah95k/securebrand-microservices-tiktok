package client

import (
	"testing"

	"github.com/tidwall/gjson"
)

func TestNextCreatorCursor(t *testing.T) {
	week := int64(7 * 24 * 3600 * 1000)
	for _, c := range []struct {
		name string
		page string
		prev int64
		next int64
		more bool
	}{
		{"oldest post on the page, keeping its second", `{"itemList":[{"createTime":1793500000},{"createTime":1793400000}],"hasMorePrevious":true}`, 1793600000000, 1793400000999, true},
		{"the list says it is done", `{"itemList":[{"createTime":1793400000}],"hasMorePrevious":false}`, 1793600000000, 1793400000999, false},
		{"an empty page steps back a week", `{"itemList":[],"hasMorePrevious":true}`, 1793600000000, 1793600000000 - week, true},
		{"a page all in the cursor's second moves past it", `{"itemList":[{"createTime":1793400000},{"createTime":1793400000}],"hasMorePrevious":true}`, 1793400000999, 1793399999999, true},
		{"older than TikTok", `{"itemList":[],"hasMorePrevious":true}`, 1472706000000, 1472706000000 - week, false},
	} {
		next, more := NextCreatorCursor(gjson.Parse(c.page), c.prev)
		if next != c.next || more != c.more {
			t.Errorf("%s: got %d %v, want %d %v", c.name, next, more, c.next, c.more)
		}
	}
}
