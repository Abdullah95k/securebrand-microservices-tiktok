package content

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"time"

	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"
	discoveryv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/discovery/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// CandidateCursor is the candidate publisher's row in content_cursors.
const CandidateCursor = TopicCandidate

// Candidates publishes discovery's found accounts on
// discovery.candidate.v1: when found, and again whenever the outcome
// changes (checked, added, not relevant, dormant, removed by the owner).
// Every write to a candidate sets its updated_at, wherever it is made (the
// entry check, the dormant list, the review page, a client's removal), so
// the publisher reads the table by updated_at from its cursor instead of
// every writer publishing; the change gate ("candidate/<ref>") sends a
// candidate only when its outcome, relevance or sources changed. It runs
// on the scheduler's leader.
type Candidates struct {
	E *Emitter
	// Batch is how many candidates a round reads at a time (default 500).
	Batch int
	// Overlap is how far before its cursor a round reads again, for rows
	// whose transaction committed after a later one (default 10 minutes).
	Overlap time.Duration
}

// Tick publishes what changed since the cursor; it returns how many
// candidates it read.
func (c *Candidates) Tick(ctx context.Context) (int, error) {
	batch := c.Batch
	if batch <= 0 {
		batch = 500
	}
	overlap := c.Overlap
	if overlap <= 0 {
		overlap = 10 * time.Minute
	}
	var cursor time.Time
	err := c.E.DB.QueryRow(ctx, `SELECT coalesce((SELECT at FROM content_cursors WHERE name = $1), 'epoch')`, CandidateCursor).
		Scan(&cursor)
	if err != nil {
		return 0, err
	}
	from, ref := cursor.Add(-overlap), ""
	read := 0
	for {
		cs, err := c.load(ctx, from, ref, batch)
		if err != nil {
			return read, err
		}
		if len(cs) == 0 {
			return read, nil
		}
		evs := make([]*Event, 0, len(cs))
		for _, cd := range cs {
			if e := cd.event(); e != nil {
				evs = append(evs, e)
			}
		}
		if err := c.E.Send(ctx, evs); err != nil {
			return read, err
		}
		read += len(cs)
		last := cs[len(cs)-1]
		from, ref = last.updated, last.ref
		if from.After(cursor) {
			cursor = from
			if _, err := c.E.DB.Exec(ctx, `INSERT INTO content_cursors (name, at) VALUES ($1, $2)
				ON CONFLICT (name) DO UPDATE SET at = greatest(content_cursors.at, EXCLUDED.at), updated_at = now()`,
				CandidateCursor, cursor); err != nil {
				return read, err
			}
		}
		if len(cs) < batch {
			return read, nil
		}
	}
}

// storedCandidate is a discovery_candidates row with its sources.
type storedCandidate struct {
	ref, state       string
	userID           *int64
	username, reason *string
	score            *float64
	evidence         []byte
	found, updated   time.Time
	checked, added   *time.Time
	sources          []storedSource
}

type storedSource struct {
	how, via string
	first    time.Time
	seen     int64
	evidence []byte
}

func (c *Candidates) load(ctx context.Context, from time.Time, after string, n int) ([]*storedCandidate, error) {
	rows, err := c.E.DB.Query(ctx, `SELECT account_ref, state, user_id, username, reason, score, evidence, first_seen_at,
			updated_at, checked_at, added_at
		FROM discovery_candidates WHERE (updated_at, account_ref) > ($1, $2) ORDER BY updated_at, account_ref LIMIT $3`, from, after, n)
	if err != nil {
		return nil, err
	}
	var out []*storedCandidate
	by := map[string]*storedCandidate{}
	var users []int64
	for rows.Next() {
		cd := &storedCandidate{}
		if err := rows.Scan(&cd.ref, &cd.state, &cd.userID, &cd.username, &cd.reason, &cd.score, &cd.evidence, &cd.found,
			&cd.updated, &cd.checked, &cd.added); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, cd)
		by[cd.ref] = cd
		if cd.userID != nil {
			users = append(users, *cd.userID)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(out) == 0 {
		return out, err
	}
	refs := make([]string, len(out))
	for i, cd := range out {
		refs[i] = cd.ref
	}
	// A candidate's sources are recorded under its reference, or (an
	// account found by another reference first) under its user id.
	rows, err = c.E.DB.Query(ctx, `SELECT s.account_ref, coalesce(s.user_id, 0), s.how, s.via, s.first_at, s.seen, s.evidence
		FROM account_sources s WHERE s.account_ref = ANY($1) OR s.user_id = ANY($2) ORDER BY s.first_at, s.how, s.via`, refs, users)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	byUser := map[int64]*storedCandidate{}
	for _, cd := range out {
		if cd.userID != nil {
			byUser[*cd.userID] = cd
		}
	}
	for rows.Next() {
		var ref string
		var uid int64
		var s storedSource
		if err := rows.Scan(&ref, &uid, &s.how, &s.via, &s.first, &s.seen, &s.evidence); err != nil {
			return nil, err
		}
		cd := by[ref]
		if cd == nil {
			cd = byUser[uid]
		}
		if cd != nil && !slices.ContainsFunc(cd.sources, func(x storedSource) bool { return x.how == s.how && x.via == s.via }) {
			cd.sources = append(cd.sources, s)
		}
	}
	return out, rows.Err()
}

// candidateSource maps a source's how (account_sources) to the contract's.
func candidateSource(how string) discoveryv1.Source {
	switch how {
	case "api":
		return discoveryv1.Source_SOURCE_CLIENT_REQUEST
	case "import":
		return discoveryv1.Source_SOURCE_IMPORT
	case "mention":
		return discoveryv1.Source_SOURCE_MENTION
	case "commenter", "reply":
		return discoveryv1.Source_SOURCE_COMMENTER
	case "search":
		return discoveryv1.Source_SOURCE_KEYWORD_SEARCH
	case "hashtag":
		return discoveryv1.Source_SOURCE_HASHTAG_PAGE
	case "sound":
		return discoveryv1.Source_SOURCE_AUDIO_PAGE
	case "related", "feed":
		// The For You feed is TikTok's own recommendation, as related
		// posts are; via says which.
		return discoveryv1.Source_SOURCE_RELATED
	case "following":
		return discoveryv1.Source_SOURCE_FOLLOWING
	case "follower":
		return discoveryv1.Source_SOURCE_FOLLOWERS
	}
	return discoveryv1.Source_SOURCE_UNSPECIFIED
}

// outcome maps a candidate's state and dormant reason to the contract's
// outcome and outcome_reason.
func outcome(state, reason string) (discoveryv1.Outcome, string) {
	switch state {
	case "added":
		return discoveryv1.Outcome_OUTCOME_ADDED, ""
	case "not_iraqi":
		return discoveryv1.Outcome_OUTCOME_NOT_RELEVANT, "not_relevant"
	case "dormant":
		if reason == "unreadable" {
			return discoveryv1.Outcome_OUTCOME_UNAVAILABLE, reason
		}
		return discoveryv1.Outcome_OUTCOME_DORMANT, reason
	case "blocked":
		return discoveryv1.Outcome_OUTCOME_EXCLUDED, ""
	}
	// pending, checking, qualified (passed, waiting for its turn to be
	// registered)
	return discoveryv1.Outcome_OUTCOME_PENDING, ""
}

func (cd *storedCandidate) event() *Event {
	reason := ""
	if cd.reason != nil {
		reason = *cd.reason
	}
	acct := &contentv1.AccountRef{Platform: Platform}
	if cd.userID != nil {
		acct.NativeId = strconv.FormatInt(*cd.userID, 10)
		acct.Id = ID("account", acct.NativeId)
	}
	if sec, ok := strings.CutPrefix(cd.ref, "sec:"); ok {
		acct.AltIds = map[string]string{"sec_uid": sec}
	}
	if cd.username != nil && *cd.username != "" {
		acct.Handle, acct.Url = *cd.username, AccountURL(*cd.username)
	} else if u, ok := strings.CutPrefix(cd.ref, "u:"); ok {
		acct.Handle, acct.Url = u, AccountURL(u)
	}
	out, why := outcome(cd.state, reason)
	m := &discoveryv1.Candidate{
		TargetPlatform: Platform, Account: acct, FoundAt: timestamppb.New(cd.found), FoundBy: Collector,
		Outcome: out, OutcomeReason: why,
		Provenance: &contentv1.Provenance{Collector: Collector, CollectorVersion: collectorVersion,
			Acquisition: contentv1.Acquisition_ACQUISITION_SCRAPED_LOGGED_OUT},
	}
	if len(cd.sources) > 0 {
		m.Source = candidateSource(cd.sources[0].how)
	}
	for _, s := range cd.sources {
		ev := &discoveryv1.Evidence{Via: s.how, Sightings: s.seen}
		if s.via != "" {
			ev.Via = s.how + ":" + s.via
		}
		var x struct {
			PostID    string `json:"post_id"`
			CommentID string `json:"comment_id"`
		}
		if json.Unmarshal(s.evidence, &x) == nil {
			switch {
			case x.CommentID != "":
				ev.ContentId = ID("comment", x.CommentID)
			case x.PostID != "":
				ev.ContentId = ID("post", x.PostID)
			}
		}
		m.Evidence = append(m.Evidence, ev)
	}
	if reason == "private" {
		m.IsPrivate = proto.Bool(true)
	}
	if cd.score != nil || out == discoveryv1.Outcome_OUTCOME_NOT_RELEVANT || out == discoveryv1.Outcome_OUTCOME_ADDED {
		r := &contentv1.Relevance{Score: cd.score}
		switch cd.state {
		case "added", "qualified":
			r.Relevant = proto.Bool(true)
		case "not_iraqi":
			r.Relevant = proto.Bool(false)
		}
		var signals []struct {
			Signal string `json:"signal"`
		}
		if json.Unmarshal(cd.evidence, &signals) == nil {
			for _, s := range signals {
				if s.Signal != "" && !slices.Contains(r.MatchedRules, s.Signal) {
					r.MatchedRules = append(r.MatchedRules, s.Signal)
				}
			}
			slices.Sort(r.MatchedRules)
		}
		m.Relevance = r
	}
	switch out {
	case discoveryv1.Outcome_OUTCOME_ADDED:
		m.Scope = contentv1.CollectionScope_COLLECTION_SCOPE_FULL
		if cd.added != nil {
			m.DecidedAt = timestamppb.New(*cd.added)
		}
	case discoveryv1.Outcome_OUTCOME_EXCLUDED:
		m.DecidedAt = timestamppb.New(cd.updated)
	case discoveryv1.Outcome_OUTCOME_PENDING:
	default:
		if cd.checked != nil {
			m.DecidedAt = timestamppb.New(*cd.checked)
		}
	}
	key := Platform + "/" + cd.ref
	e := &Event{Gate: "candidate/" + key, Topic: TopicCandidate, Key: key, Observed: cd.updated.UTC(), Msg: m}
	// Sightings and the provenance's build do not change the candidate.
	e.Hash = hashOf(m, func(x proto.Message) {
		c := x.(*discoveryv1.Candidate)
		c.Provenance = nil
		for _, ev := range c.Evidence {
			ev.Sightings = 0
		}
	})
	e.seal = func(hash string) {
		sum := sha256.Sum256([]byte(key + "|" + strconv.FormatInt(e.Observed.UnixMicro(), 10) + "|" + hash))
		m.EventId = hex.EncodeToString(sum[:])
	}
	return e
}
