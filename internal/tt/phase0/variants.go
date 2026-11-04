package phase0

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/parse"
)

// VariantTry is one operation variant tried on one account.
type VariantTry struct {
	Query   string `json:"query"`
	Variant string `json:"variant"`
	Signed  bool   `json:"signed"`
	Account int    `json:"account"`
	Class   string `json:"class"`
	Items   int    `json:"items"`
	OK      bool   `json:"ok"`
}

// VariantsResult is what the variants step found: which variant of each
// operation the crawler uses answers today.
type VariantsResult struct {
	Tries []VariantTry `json:"tries"`
	// Chosen is the working variant of each operation, first in the
	// registry the measuring steps use.
	Chosen map[string]string `json:"chosen"`
	// Missing operations had no working variant; Untested ones had nothing
	// to try (no post with comments, no comment with replies).
	Missing  []string `json:"missing,omitempty"`
	Untested []string `json:"untested,omitempty"`
}

// Candidates are the variants of an operation to try, in order: the
// registry's own (by priority), then each one's signed or unsigned twin.
// TikTok changes which calls it accepts unsigned (T4, T25), so the kit
// tries both ways rather than trusting the seed.
func Candidates(seed registry.Seed, name string) []registry.Query {
	var own []registry.Query
	for _, q := range seed.Queries {
		if q.Name == name {
			own = append(own, q)
		}
	}
	slices.SortStableFunc(own, func(a, b registry.Query) int { return b.Priority - a.Priority })
	out := slices.Clone(own)
	have := map[string]bool{}
	for _, q := range own {
		have[q.DocID] = true
	}
	for _, q := range own {
		twin := ttclient.Twin(q)
		if !have[twin.DocID] {
			have[twin.DocID] = true
			out = append(out, twin)
		}
	}
	return out
}

// Choose writes the chosen variants first: each gets a priority above its
// operation's others, which stay as fallbacks.
func Choose(seed registry.Seed, chosen map[string]string) registry.Seed {
	out := registry.Seed{ClientParams: seed.ClientParams}
	added := map[string]bool{}
	top := map[string]int{}
	for _, q := range seed.Queries {
		top[q.Name] = max(top[q.Name], q.Priority)
	}
	for _, q := range seed.Queries {
		if chosen[q.Name] == q.DocID {
			q.Priority = top[q.Name] + 10
			added[q.Name] = true
		}
		out.Queries = append(out.Queries, q)
	}
	// A chosen twin the seed does not have.
	for _, name := range sortedKeys(chosen) {
		if added[name] {
			continue
		}
		for _, c := range Candidates(seed, name) {
			if c.DocID == chosen[name] {
				c.Priority = top[name] + 10
				out.Queries = append(out.Queries, c)
				break
			}
		}
	}
	return out
}

// variants tries each candidate of the crawler's operations on the first
// accounts, in order, and keeps the first that answers with data.
func (k *Kit) variants(ctx context.Context) error {
	tier := k.Cfg.Tiers[0]
	f, err := os.OpenFile(k.path(FileVariantsLog), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	rec := NewRecorder(f, StepVariants, tier)
	res := VariantsResult{Chosen: map[string]string{}}
	try := func(acct int, q registry.Query, do func(*graphql.Client) (*graphql.Response, Record), ok func(*graphql.Response) (int, bool)) (*graphql.Response, bool) {
		client := k.client(registry.NewStatic(registry.Seed{ClientParams: k.Cfg.Seed.ClientParams, Queries: []registry.Query{q}}))
		resp, r := do(client)
		r.Op, r.Variant = OpVariant, q.DocID
		t := VariantTry{Query: q.Name, Variant: q.DocID, Signed: ttclient.Signed(q), Account: acct, Class: r.Class}
		if resp != nil && resp.Class == detect.OK {
			t.Items, t.OK = ok(resp)
		}
		r.Items = t.Items
		rec.Add(r)
		res.Tries = append(res.Tries, t)
		k.logf("%-13s %-22s account %d: %s, %d items", q.Name, q.DocID, acct, verdict(t), t.Items)
		return resp, t.OK
	}
	w := k.newWork(tier, rec, Limits{})
	accounts := k.Cfg.Accounts[:min(k.Cfg.VariantAccounts, len(k.Cfg.Accounts))]
	for i, name := range accounts {
		acct := k.line(i)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		id, err := k.Stack.Pool.Acquire(ctx, tier)
		if err != nil {
			return &StepError{Step: StepVariants, Err: err}
		}
		exit := k.exits.Label(id)
		sec, why := w.page(ctx, id, exit, acct, name)
		if sec == "" {
			k.logf("account %d: no list to try (%s)", acct, why)
			k.Stack.Pool.Release(id)
			continue
		}
		// The list.
		var list *graphql.Response
		for _, q := range Candidates(k.Cfg.Seed, queryList) {
			resp, ok := try(acct, q, func(c *graphql.Client) (*graphql.Response, Record) {
				return w.callWith(ctx, c, id, exit, acct, OpVariant, queryList,
					map[string]any{"sec_uid": sec, "count": listPageSize, "cursor": (k.now().Unix() + 1) * 1000}, profileURL(id, name))
			}, func(r *graphql.Response) (int, bool) {
				p, rep := parse.PostList(r.Body, parse.Options{Query: queryList})
				return len(p.Posts), rep.OK()
			})
			if ok {
				res.Chosen[queryList] = first(res.Chosen[queryList], q.DocID)
				list = resp
				break
			}
		}
		if list == nil {
			k.Stack.Pool.Release(id)
			continue
		}
		posts, _ := parse.PostList(list.Body, parse.Options{Query: queryList})
		// Comments, on the newest post that has some.
		var thread *parse.ThreadRef
		for _, b := range posts.Posts {
			if b.Post.GetCounts().GetComments() == 0 || b.Post.GetCommentsDisabled() {
				continue
			}
			p := b.Post
			cc := parse.CommentContext{PostID: p.GetPostId(), UserID: p.GetUserId(), PostCreatedAt: p.GetCreatedAt()}
			var page parse.CommentsResult
			for _, q := range Candidates(k.Cfg.Seed, queryComments) {
				_, ok := try(acct, q, func(c *graphql.Client) (*graphql.Response, Record) {
					return w.callWith(ctx, c, id, exit, acct, OpVariant, queryComments,
						map[string]any{"item_id": p.GetPostId(), "count": commentPageSize, "cursor": 0}, postURL(id, name, p))
				}, func(r *graphql.Response) (int, bool) {
					var rep parse.Report
					page, rep = parse.Comments(r.Body, cc, parse.Options{Query: queryComments})
					return len(page.Comments), rep.OK() && len(page.Comments) > 0
				})
				if ok {
					res.Chosen[queryComments] = first(res.Chosen[queryComments], q.DocID)
					break
				}
			}
			for _, t := range page.Threads {
				if t.ReplyCount > 0 {
					thread = &t
					break
				}
			}
			if thread != nil {
				rc := cc
				rc.Replies, rc.ParentCommentID = true, thread.CommentID
				for _, q := range Candidates(k.Cfg.Seed, queryReplies) {
					_, ok := try(acct, q, func(c *graphql.Client) (*graphql.Response, Record) {
						return w.callWith(ctx, c, id, exit, acct, OpVariant, queryReplies,
							map[string]any{"item_id": p.GetPostId(), "comment_id": thread.CommentID, "count": replyPageSize, "cursor": 0}, postURL(id, name, p))
					}, func(r *graphql.Response) (int, bool) {
						pg, rep := parse.Comments(r.Body, rc, parse.Options{Query: queryReplies})
						return len(pg.Comments), rep.OK() && len(pg.Comments) > 0
					})
					if ok {
						res.Chosen[queryReplies] = first(res.Chosen[queryReplies], q.DocID)
						break
					}
				}
			}
			break
		}
		k.Stack.Pool.Release(id)
		if res.Chosen[queryList] != "" && res.Chosen[queryComments] != "" && res.Chosen[queryReplies] != "" {
			break
		}
	}
	for _, name := range []string{queryList, queryComments, queryReplies} {
		if res.Chosen[name] != "" {
			continue
		}
		tried := false
		for _, t := range res.Tries {
			tried = tried || t.Query == name
		}
		if tried {
			res.Missing = append(res.Missing, name)
		} else {
			res.Untested = append(res.Untested, name)
		}
	}
	if err := writeJSON(k.path(FileVariants), res); err != nil {
		return err
	}
	if err := rec.Err(); err != nil {
		return err
	}
	switch {
	case slices.Contains(res.Missing, queryList) || res.Chosen[queryList] == "":
		return &StepError{Step: StepVariants, NeedsAction: true, Err: fmt.Errorf("no variant of the post list answered (see %s); send %s", k.path(FileVariants), k.path(FileBundle))}
	case len(res.Missing) > 0:
		return &StepError{Step: StepVariants, NeedsAction: true, Err: fmt.Errorf("no variant of %s answered (see %s); send %s",
			strings.Join(res.Missing, ", "), k.path(FileVariants), k.path(FileBundle))}
	}
	for _, name := range res.Untested {
		k.logf("%s: untested (no post with comments, or no comment with replies, among the first %d accounts); the seed's variant is kept", name, len(accounts))
	}
	chosen := Choose(k.Cfg.Seed, res.Chosen)
	b, err := registry.MarshalSeed(chosen)
	if err != nil {
		return err
	}
	head := "# Written by tks-spike phase0 (variants step): the working variant of each operation first.\n"
	if err := os.WriteFile(k.path(FileRegistry), append([]byte(head), b...), 0o600); err != nil {
		return err
	}
	k.registrySeed = &chosen
	k.logf("wrote %s", k.path(FileRegistry))
	return nil
}

func verdict(t VariantTry) string {
	if t.OK {
		return "answers"
	}
	return "refused (" + t.Class + ")"
}

func first(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
