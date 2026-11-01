package parse

import (
	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/proto"

	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/htmlx"
)

// Page status codes seen on profile pages that hold no account (C2, C3;
// T1): 10221 on 11 of the full pass's 13 shells, 209002 on 2. Their exact
// meaning is TikTok's: tks reads 10221 as banned and the others as
// unavailable, and records the code.
const (
	CodeBanned           = 10221
	CodeNotShown         = 209002
	CodeUserNotFoundHint = 10202 // public sources' "user does not exist"
)

// AccountResult is one parsed account.
type AccountResult struct {
	// Account is nil when the answer holds no account; Status then says
	// why, from StatusCode.
	Account    *ttv1.Account
	Media      []*ttv1.Media // the avatar
	Status     ttv1.AccountStatus
	StatusCode int64
	Dropped    Dropped
}

// StatusFromCode maps a page's or call's statusCode to an account status.
func StatusFromCode(code int64) ttv1.AccountStatus {
	switch code {
	case 0:
		return ttv1.AccountStatus_ACCOUNT_STATUS_ACTIVE
	case CodeBanned:
		return ttv1.AccountStatus_ACCOUNT_STATUS_BANNED
	default:
		return ttv1.AccountStatus_ACCOUNT_STATUS_UNAVAILABLE
	}
}

// ProfilePage parses a profile page (/@<username>): the rehydration
// document's webapp.user-detail. A page without the document (a challenge,
// a client-rendered shell) is drift and is reported missing; a page whose
// user-detail carries an error statusCode and no user is an unavailable
// account, not drift.
func ProfilePage(html []byte, o Options) (AccountResult, Report) {
	var rep Report
	res := AccountResult{Dropped: Dropped{}}
	ud, ok := htmlx.Scope(html, htmlx.ScopeUserDetail)
	if !ok {
		rep.AddMissing(htmlx.ScopeUserDetail)
		return res, rep
	}
	return userInfo(ud, ttv1.AccountSource_ACCOUNT_SOURCE_PROFILE_PAGE, o, res, rep)
}

// UserDetail parses the account call (/api/user/detail/). Unsigned, the
// call is refused with an empty userInfo and no status code (C2 run 1,
// T24): that reads as no account, with userInfo.user.id missing. The
// client's decoder classes it as a refusal before a parser sees it.
func UserDetail(body []byte, o Options) (AccountResult, Report) {
	res := AccountResult{Dropped: Dropped{}}
	return userInfo(gjson.ParseBytes(body), ttv1.AccountSource_ACCOUNT_SOURCE_USER_DETAIL, o, res, Report{})
}

func userInfo(doc gjson.Result, src ttv1.AccountSource, o Options, res AccountResult, rep Report) (AccountResult, Report) {
	res.StatusCode, _ = statusCode(doc)
	ui := doc.Get("userInfo")
	if u := ui.Get("user"); !u.Get("id").Exists() {
		if res.StatusCode != 0 {
			res.Status = StatusFromCode(res.StatusCode)
			return res, rep
		}
		rep.AddMissing("userInfo.user.id")
		return res, rep
	}
	acc, media := account(ui.Get("user"), ui.Get("stats"), ui.Get("statsV2"), src, o, &rep, res.Dropped)
	if acc != nil {
		res.Account, res.Media, res.Status = acc, media, acc.Status
	}
	return res, rep
}

// account builds an Account from a user object and its two count
// objects: the page's userInfo (user, stats, statsV2) or a list item's
// (author, authorStats, authorStatsV2).
func account(u, stats, statsV2 gjson.Result, src ttv1.AccountSource, o Options, rep *Report, dropped Dropped) (*ttv1.Account, []*ttv1.Media) {
	id, ok, inRange := ID(u.Get("id"))
	if !ok {
		if !inRange {
			dropped.add(DropIDRange)
		} else {
			rep.AddMissing("user.id")
		}
		return nil, nil
	}
	a := &ttv1.Account{Obs: o.obs(), UserId: id, SecUid: u.Get("secUid").String(), Username: u.Get("uniqueId").String(),
		Nickname: u.Get("nickname").String(), Bio: u.Get("signature").String(), Verified: optBool(u.Get("verified")),
		Private: optBool(u.Get("privateAccount")), CreatedAt: unix(u.Get("createTime")),
		UsernameModifiedAt: unix(u.Get("uniqueIdModifyTime")), NicknameModifiedAt: unix(u.Get("nickNameModifyTime")),
		Language: u.Get("language").String(), Source: src, AuthorKey: o.key(id)}
	for _, f := range []string{"secUid", "uniqueId"} {
		if !u.Get(f).Exists() {
			rep.AddMissing("user." + f)
		}
	}
	if org := u.Get("isOrganization"); org.Exists() {
		a.Organization = proto.Bool(org.Bool())
	}
	if bl := u.Get("bioLink"); bl.Exists() {
		if l := bl.Get("link").String(); l != "" {
			a.BioLink = proto.String(l)
		}
		if r := bl.Get("risk"); r.Exists() {
			a.BioLinkRisk = proto.Int32(int32(r.Int()))
		}
	}
	if stats.Exists() || statsV2.Exists() {
		a.Counts = &ttv1.AccountCounts{
			Followers:  count(statsV2.Get("followerCount"), stats.Get("followerCount")),
			Following:  count(statsV2.Get("followingCount"), stats.Get("followingCount")),
			Hearts:     count(firstOf(statsV2, "heartCount", "heart"), firstOf(stats, "heartCount", "heart")),
			Videos:     count(statsV2.Get("videoCount"), stats.Get("videoCount")),
			LikesGiven: count(statsV2.Get("diggCount"), stats.Get("diggCount")),
			Friends:    count(statsV2.Get("friendCount"), stats.Get("friendCount")),
			// The page's statsV2 is exact; a list item's authorStatsV2 is
			// rounded like stats (every follower count in the recordings and
			// the probe's lists ended in 000, M4).
			Exact: statsV2.Exists() && src != ttv1.AccountSource_ACCOUNT_SOURCE_POST_LIST,
		}
	} else {
		rep.AddOptional("stats")
	}
	a.Status = ttv1.AccountStatus_ACCOUNT_STATUS_ACTIVE
	if a.GetPrivate() {
		a.Status = ttv1.AccountStatus_ACCOUNT_STATUS_PRIVATE
	}
	var media []*ttv1.Media
	if av := firstOf(u, "avatarLarger", "avatarMedium", "avatarThumb").String(); av != "" {
		m := &ttv1.Media{Obs: o.obs(), MediaId: MediaID(id, ttv1.MediaKind_MEDIA_KIND_AVATAR, "original"),
			Kind: ttv1.MediaKind_MEDIA_KIND_AVATAR, Variant: "original", UserId: id, SourceUrl: av,
			SourceExpiresAt: URLExpiry(av), AuthorKey: a.AuthorKey}
		a.AvatarUrl, a.AvatarMediaId = av, m.MediaId
		media = append(media, m)
	}
	return a, media
}

// firstOf returns the first of r's keys that exists.
func firstOf(r gjson.Result, keys ...string) gjson.Result {
	for _, k := range keys {
		if v := r.Get(k); v.Exists() && v.Type != gjson.Null {
			return v
		}
	}
	return gjson.Result{}
}
