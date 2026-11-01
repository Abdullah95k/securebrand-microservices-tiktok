// Package probe answers the open questions about TikTok that need real
// traffic (docs/tiktok/open-questions.md), and measures what each request
// costs, for a short list of public accounts and then the whole list
// (docs/tiktok/probe-spec.md, checkpoints C2 and C3). The owner runs it on
// the Phase 0 droplet through the ISP proxy tiers.
//
// Beside the answers, the report holds everything the probe read (owner's
// decision): each account's details and avatar, its posts with their counts
// and covers, comments and replies, and sampled video files. That is
// personal data: the command writes it 0600 under phase0/, which git
// ignores, and it is never committed.
package probe

import "time"

// Step is one request's outcome.
type Step struct {
	Class     string `json:"class"`
	Status    int    `json:"status,omitempty"`
	Reason    string `json:"reason,omitempty"`
	Ms        int64  `json:"ms"`
	WireBytes int64  `json:"wire_bytes"`
	Items     int    `json:"items,omitempty"`
	HasMore   bool   `json:"has_more,omitempty"`
	// Empty marks an HTTP 200 with no body: TikTok's answer to a refused
	// request (T4).
	Empty bool `json:"empty,omitempty"`
}

// AccountData is what TikTok served about an account.
type AccountData struct {
	Source     string `json:"source"` // "page" or "user_detail"
	ID         string `json:"id"`
	SecUID     string `json:"sec_uid"`
	Username   string `json:"username"`
	Nickname   string `json:"nickname"`
	Bio        string `json:"bio"`
	BioLink    string `json:"bio_link,omitempty"`
	Verified   bool   `json:"verified"`
	Private    bool   `json:"private"`
	Followers  int64  `json:"followers"`
	Following  int64  `json:"following"`
	Likes      int64  `json:"likes"`
	Videos     int64  `json:"videos"`
	LikesGiven int64  `json:"likes_given"`
	CreatedAt  int64  `json:"created_at,omitempty"`
	// RenamedAt is uniqueIdModifyTime: when the username last changed.
	RenamedAt  int64  `json:"renamed_at,omitempty"`
	AvatarURL  string `json:"avatar_url,omitempty"`
	AvatarFile string `json:"avatar_file,omitempty"`
}

// Post is one video or photo post read.
type Post struct {
	ID       string   `json:"id"`
	Link     string   `json:"link"`
	Kind     string   `json:"kind"` // video or photo
	Caption  string   `json:"caption"`
	Hashtags []string `json:"hashtags,omitempty"`
	Mentions []string `json:"mentions,omitempty"`
	PostedAt int64    `json:"posted_at"`
	// IDTime is the time the id's top 32 bits encode (T8).
	IDTime      int64  `json:"id_time"`
	Duration    int64  `json:"duration,omitempty"`
	CoverURL    string `json:"cover_url,omitempty"`
	CoverFile   string `json:"cover_file,omitempty"`
	VideoURL    string `json:"video_url,omitempty"`
	MusicID     string `json:"music_id,omitempty"`
	MusicTitle  string `json:"music_title,omitempty"`
	MusicAuthor string `json:"music_author,omitempty"`
	MusicOrig   bool   `json:"music_original,omitempty"`
	Pinned      bool   `json:"pinned,omitempty"`
	Ad          bool   `json:"ad,omitempty"`
	AILabel     string `json:"ai_label,omitempty"`
	Plays       int64  `json:"plays"`
	Likes       int64  `json:"likes"`
	Comments    int64  `json:"comments"`
	Shares      int64  `json:"shares"`
	Saves       int64  `json:"saves"`
	Reposts     int64  `json:"reposts"`
	HasStats    bool   `json:"has_stats"`
}

// Comment is one comment or reply read.
type Comment struct {
	PostID         string `json:"post_id"`
	ID             string `json:"id"`
	ParentID       string `json:"parent_id,omitempty"`
	AuthorID       string `json:"author_id"`
	AuthorUsername string `json:"author_username"`
	AuthorNickname string `json:"author_nickname"`
	Text           string `json:"text"`
	CreatedAt      int64  `json:"created_at"`
	Likes          int64  `json:"likes"`
	Replies        int64  `json:"replies"`
	Page           int    `json:"page"`
}

// Walk is the post list read back in time.
type Walk struct {
	Variant string `json:"variant"` // creator_list or post_list
	Pages   int    `json:"pages"`
	Posts   int    `json:"posts"` // unique posts
	// Repeats counts posts a page served again (the creator list's cursor
	// keeps the oldest post's second, so its oldest post comes back once
	// per page when TikTok compares in milliseconds).
	Repeats int `json:"repeats"`
	// EmptyPages counts empty pages the walk stepped back a week from.
	EmptyPages int `json:"empty_pages,omitempty"`
	// Stop is "end", "cap", "age", "stuck" (a page brought nothing new),
	// "stopped" or "wall:<class>".
	Stop   string    `json:"stop"`
	Oldest time.Time `json:"oldest,omitzero"`
	// Filtered counts pages that said filter_reasons (T30).
	Filtered int   `json:"filtered,omitempty"`
	Failed   *Step `json:"failed,omitempty"`
}

// CommentRead is one post's comments read.
type CommentRead struct {
	PostID string `json:"post_id"`
	// Via is "plain" (unsigned HTTP) or "browser" (fetched from the browser
	// page, which signs it).
	Via   string `json:"via"`
	Pages int    `json:"pages"`
	Read  int    `json:"read"` // unique top-level comments
	Total int64  `json:"total"`
	Ended bool   `json:"ended"`
	// Order is "newest_first" or "not_time_ordered" (T13).
	Order string `json:"order,omitempty"`
	// ReplySum is Σ(1 + reply count) over the comments read; with Ended,
	// ReplySum ÷ Total is T15's ratio.
	ReplySum int64 `json:"reply_sum"`
	Failed   *Step `json:"failed,omitempty"`
}

// Depth is one creator-list page read with a cursor this far back (T11):
// how far back the list answers, without walking to it.
type Depth struct {
	Days   int       `json:"days"`
	Step   Step      `json:"step"`
	Newest time.Time `json:"newest,omitzero"`
	Oldest time.Time `json:"oldest,omitzero"`
}

// PageCall is one API call a browser page made on its own (the fallback,
// methods.md §3): what TikTok answered the page's own signed requests.
type PageCall struct {
	Path   string `json:"path"`
	Status int    `json:"status"`
	Bytes  int    `json:"bytes"`
	Items  int    `json:"items"`
	Empty  bool   `json:"empty,omitempty"`
	Signed bool   `json:"signed"` // carried X-Gnarly
}

// SignedCall is one call made from inside a browser page (T7, T27).
type SignedCall struct {
	// Class is how the answer reads, as the client would class it (an
	// empty body or account is a refusal, an error statusCode an error).
	Class  string `json:"class"`
	Reason string `json:"reason,omitempty"`
	Status int    `json:"status"`
	Items  int    `json:"items"`
	Empty  bool   `json:"empty,omitempty"`
	// Added are the parameters the page's script added to the URL.
	Added []string `json:"added,omitempty"`
	Ms    int64    `json:"ms"`
	// Replay is the signed URL sent again by plain HTTP from the probe's
	// identity (T27). For the first comment call, also: ReplayOther from a
	// fresh identity with no cookies (another exit when the tier has
	// several), ReplayModified with the cursor changed and the signatures
	// kept (does the signature cover the query?), and ReplayLater once more
	// after the browser's comment reads (its Reason says how much later).
	Replay         *Step `json:"replay,omitempty"`
	ReplayOther    *Step `json:"replay_other,omitempty"`
	ReplayModified *Step `json:"replay_modified,omitempty"`
	ReplayLater    *Step `json:"replay_later,omitempty"`

	sentURL string // the URL the page sent, signatures included
}

// Signed is the browser signer's part of an account.
type Signed struct {
	Page Step `json:"page"`
	// Env is what the page's script sees of the browser (navigator.webdriver,
	// time zone, screen) and BotType the page's verdict on it.
	Env     map[string]any `json:"env,omitempty"`
	BotType string         `json:"bot_type,omitempty"`
	// PageCalls are the profile page's own API calls (after one scroll when
	// the load made no list call); VideoPageCalls the post page's, reached
	// by following the post's link in the same page.
	PageCalls      []PageCall `json:"page_calls,omitempty"`
	VideoPage      *Step      `json:"video_page,omitempty"`
	VideoPageCalls []PageCall `json:"video_page_calls,omitempty"`
	// ParamsMissing are query parameters the page's own post-list call sent
	// that tks's builder does not; ParamsExtra the reverse.
	ParamsMissing []string `json:"params_missing,omitempty"`
	ParamsExtra   []string `json:"params_extra,omitempty"`
	// Calls are the calls the probe made from the page: tks's own URLs
	// (user_detail, creator_list, post_list, comments) and the next page of
	// the page's own list and comment calls (*_from_page).
	Calls map[string]*SignedCall `json:"calls,omitempty"`
	Error string                 `json:"error,omitempty"`
}

// Account is everything read for one account.
type Account struct {
	Index    int    `json:"index"`
	Username string `json:"username"`
	URL      string `json:"url"`
	// Page is the profile page load; PageForm is "ssr" (account
	// embedded), "shell" (no account), "challenge" or "none" (T1, T28).
	Page        Step            `json:"page"`
	PageForm    string          `json:"page_form,omitempty"`
	Cookies     []string        `json:"cookies,omitempty"`
	Tokens      []string        `json:"tokens,omitempty"`
	BotType     string          `json:"bot_type,omitempty"`
	AltPage     *Step           `json:"alt_fingerprint_page,omitempty"`
	Data        *AccountData    `json:"data,omitempty"`
	UserDetail  *Step           `json:"user_detail,omitempty"`
	CreatorList *Step           `json:"creator_list,omitempty"`
	PostList    *Step           `json:"post_list,omitempty"`
	CountChecks map[string]Step `json:"count_checks,omitempty"`
	// PageStatus is the profile page's user-detail statusCode when the page
	// carried no account (a shell): 10202 no such user, 10221 banned, ...
	PageStatus int     `json:"page_status,omitempty"`
	Depth      []Depth `json:"depth,omitempty"`
	// AppListPinned counts pinned posts on the first page of the app's own
	// list (unsigned or, failing that, signed in the browser): T10.
	AppListPinned int   `json:"app_list_pinned"`
	Walk          *Walk `json:"walk,omitempty"`
	// CursorPortable: a list cursor replayed from a second identity (T22).
	CursorPortable *bool           `json:"cursor_portable,omitempty"`
	Needs          map[string]Step `json:"needs,omitempty"` // no_mstoken, no_cookies, no_device_id (T2)
	PostPage       *Step           `json:"post_page,omitempty"`
	PostPageStats  bool            `json:"post_page_has_stats,omitempty"`
	PhotoPage      *Step           `json:"photo_page,omitempty"`
	CommentReads   []CommentRead   `json:"comment_reads,omitempty"`
	ReplyReads     []CommentRead   `json:"reply_reads,omitempty"`
	ReplyCount20   *Step           `json:"replies_count_20,omitempty"`
	Media          map[string]Step `json:"media,omitempty"`
	// VideoCookies are the cookie names the identity sends to the video
	// host (T18: the links carry tk=tt_chain_token).
	VideoCookies []string  `json:"video_cookies,omitempty"`
	Signed       *Signed   `json:"signed,omitempty"`
	OEmbed       *Step     `json:"oembed,omitempty"`
	Embed        *Step     `json:"creator_embed,omitempty"`
	Posts        []Post    `json:"posts,omitempty"`
	Comments     []Comment `json:"comments,omitempty"`
	Stopped      bool      `json:"stopped,omitempty"`
	Error        string    `json:"error,omitempty"`

	pageVideoURL string          // the post page's own video link (T18)
	commentSeen  map[string]bool // comment ids already kept (addComment)
}

// Report is a run's result.
type Report struct {
	Version     string    `json:"version"`
	Tier        string    `json:"tier"`
	Fingerprint string    `json:"fingerprint"`
	Signer      string    `json:"signer"`
	StartedAt   time.Time `json:"started_at"`
	FinishedAt  time.Time `json:"finished_at"`
	Accounts    []Account `json:"accounts"`
	Summary     Summary   `json:"summary"`
}

// Summary answers the questions over all accounts.
type Summary struct {
	Accounts     int            `json:"accounts"`
	StoppedEarly string         `json:"stopped_early,omitempty"` // "", "operator" or "captchas"
	Bootstrapped int            `json:"bootstrapped"`
	PageForms    map[string]int `json:"page_forms"`
	Cookies      map[string]int `json:"cookies"`
	Tokens       map[string]int `json:"tokens"`
	BotTypes     map[string]int `json:"bot_types"`
	// Calls: per call kind, the classes seen ("creator_list": {"ok": 5}).
	Calls      map[string]map[string]int `json:"calls"`
	Captchas   int                       `json:"captchas"`
	Challenges int                       `json:"challenges"`
	// T8: posts whose id time is within 60 s of createTime, and the
	// largest gap seen in seconds.
	IDTimeMatches    int   `json:"id_time_matches"`
	IDTimeMismatches int   `json:"id_time_mismatches"`
	IDTimeMaxGap     int64 `json:"id_time_max_gap_s"`
	// IDTimeScheduled counts posts published more than 60 s after their id
	// was made: scheduled posts, whose createTime is the publish time and
	// whose id is the upload time (the C2 run).
	IDTimeScheduled int `json:"id_time_scheduled"`
	// T9, T10, T16, T19.
	Posts          int `json:"posts"`
	PostsWithStats int `json:"posts_with_stats"`
	PinnedSeen     int `json:"pinned_seen"`
	AdSeen         int `json:"ad_seen"`
	PhotoSeen      int `json:"photo_seen"`
	// T11.
	WalkStops     map[string]int `json:"walk_stops"`
	OldestReached time.Time      `json:"oldest_reached,omitzero"`
	FilteredPages int            `json:"filtered_pages"`
	WalkRepeats   int            `json:"walk_repeats"`
	// DepthServed: per days back, how many accounts' lists answered posts
	// from that far back.
	DepthServed map[string]int `json:"depth_served,omitempty"`
	// T12-T15.
	CommentReads   int `json:"comment_reads"`
	CommentsServed int `json:"comments_served"`
	// CommentsByVia splits CommentsServed by how they were asked for.
	CommentsByVia  map[string]int `json:"comments_by_via,omitempty"`
	OrderVerdicts  map[string]int `json:"order_verdicts"`
	RepliesServed  int            `json:"replies_served"`
	CompleteRatios []float64      `json:"complete_ratios,omitempty"`
	// T22.
	CursorPortable    int `json:"cursor_portable"`
	CursorNotPortable int `json:"cursor_not_portable"`
	// Cost: per request kind, count, mean latency and mean wire bytes.
	Requests map[string]Cost `json:"requests"`
}

// Cost aggregates one request kind.
type Cost struct {
	N         int   `json:"n"`
	MeanMs    int64 `json:"mean_ms"`
	MeanBytes int64 `json:"mean_wire_bytes"`
	sumMs     int64
	sumBytes  int64
}
