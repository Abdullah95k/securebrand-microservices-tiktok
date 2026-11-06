package stage

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/emit"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/parse"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
)

// EntryStage runs discovery's entry check of an account a source found
// (docs/tiktok/discovery-plan.md §4): one read of the first page of its
// post list (the unsigned creator list, its newest posts with their
// regions), and its profile page only when the list's key (secUid) is not
// known or the identity needs a page's values. Everything read is stored
// as a monitored account's would be (the account, its posts with their
// counts, music and media metadata: discovery-plan §10), but nothing else
// is read: no comments, no media files, no list watermark (the account's
// first read, once it is added, is a full backfill). Whether the account is
// added, kept as not Iraqi (yet) or put on the dormant list is decided by
// the job's finalizer once the persister has stored what this read
// (discovery.Discoverer.Decide); the counter entry_read tells it the list
// answered.
type EntryStage struct {
	A *AccountStage
}

func (s *EntryStage) Run(ctx context.Context, t *tkstaskv1.Task) Outcome {
	d := s.A.D
	sc := t.GetAccount()
	c := map[string]int64{}
	if sc == nil {
		return Outcome{Result: Fail, Reason: "entry task without account scope"}
	}
	id, err := d.identity(ctx, "list", t.Stalls)
	if err != nil {
		return noIdentity(t, err, c)
	}
	defer d.Pool.Release(id)

	// The profile page only when it is needed: an account with no known
	// secUid, or an identity without a page's values. It is never "due".
	now := timestamppb.New(d.now())
	if sc.AccountSeenAt == nil {
		sc.AccountSeenAt = now
	}
	if sc.PageSeenAt == nil {
		sc.PageSeenAt = now
	}
	if out, stop := s.A.profile(ctx, t, sc, id, c); stop {
		return out
	}
	if sc.SecUid == "" {
		return Outcome{Result: Fail, Reason: "account unresolved: no secUid", Counters: c}
	}

	q, _ := d.Client.Registry.Get(listQuery)
	resp, err := d.Client.Do(ctx, id, graphql.Request{Query: listQuery,
		Vars:   map[string]any{"sec_uid": sc.SecUid, "count": d.Cfg.TT.ListPageSize, "cursor": d.now().Add(time.Second).UnixMilli()},
		PageID: sc.UserId, Lane: topics.LaneName(t.Lane), Referer: profileURL(id, sc.Username)})
	c["requests"]++
	if err != nil {
		c["transport_errors"]++
		return retry(t, "", "30s", err.Error(), c)
	}
	c["wire_bytes"] += resp.BytesIn + resp.BytesOut
	switch resp.Class {
	case detect.OK:
	case detect.ContentUnavailable:
		return s.A.unavailable(ctx, t, sc, statusOf(resp.Errors), c)
	default:
		return classOutcome(t, resp.Class, resp.Reason, "", c)
	}
	res, rep := parse.PostList(resp.Body, parse.Options{AuthorKey: d.Emit.Key, Query: listQuery, Variant: q.DocID})
	d.Client.Parsed(resp, rep.OK())
	if !rep.OK() {
		metrics.ParseTotal.WithLabelValues(listQuery, strconv.Itoa(parse.Version), "drift").Inc()
		return classOutcome(t, detect.SchemaDrift, fmt.Sprint(rep.Missing), "", c)
	}
	metrics.ParseTotal.WithLabelValues(listQuery, strconv.Itoa(parse.Version), "ok").Inc()

	b := d.Producer.NewBatch()
	f := fetcherOf(id)
	if res.Account != nil {
		if out, stop := s.A.listAccount(ctx, b, t, sc, res, f, c); stop {
			return out
		}
	}
	src := emit.Source{JobID: t.JobId, Query: listQuery, Variant: q.DocID, ParserVersion: parse.Version, Mode: t.Mode, Region: regionOf(id)}
	for _, pb := range res.Posts {
		p := pb.Post
		if p.UserId != 0 && sc.UserId != 0 && p.UserId != sc.UserId {
			c["posts_foreign"]++ // never under this account
			continue
		}
		if err := d.Emit.Post(ctx, b, src, p); err != nil {
			return retry(t, "", "30s", err.Error(), c)
		}
		if err := d.Emit.Music(ctx, b, src, pb.Music); err != nil {
			return retry(t, "", "30s", err.Error(), c)
		}
		// Metadata only: an entry task's limits download nothing.
		if _, err := d.media(ctx, b, t, src, pb.Media, false, f); err != nil {
			return retry(t, "", "30s", err.Error(), c)
		}
		c["posts"]++
	}
	if err := b.Wait(ctx); err != nil {
		return retry(t, "", "30s", "produce: "+err.Error(), c)
	}
	c["entry_read"] = 1
	return done(c)
}
