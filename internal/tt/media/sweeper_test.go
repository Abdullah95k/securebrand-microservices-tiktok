package media

import (
	"bytes"
	"context"
	"testing"
	"time"

	mediav1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/media/v1"
	"github.com/minio/minio-go/v7"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/content"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
)

// The sweeper (owner, 2026-11-10: tks deletes its own files) deletes a
// ready file once every required consumer acknowledged it (processed,
// failed for good, or skipped), or at its deadline; not one a consumer has
// yet to acknowledge, nor one whose retryable failure is pending, before
// the deadline. Each deletion removes the object, goes out on tks's topic
// and the contract's, and marks the row deleted; a second sweep deletes
// nothing more.
func TestSweeper(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	ids := map[string]string{
		"acked":    "11111111-1111-5111-8111-000000000001", // both consumers processed
		"partly":   "11111111-1111-5111-8111-000000000002", // one of two
		"expired":  "11111111-1111-5111-8111-000000000003", // past its deadline, no ack
		"retrying": "11111111-1111-5111-8111-000000000004", // a retryable failure, not yet due
		"stored":   "11111111-1111-5111-8111-000000000005", // not handed over yet
		"failed":   "11111111-1111-5111-8111-000000000006", // processed and failed for good
	}
	for name, id := range ids {
		st, stored := "ready", now.Add(-time.Hour)
		var deleteAfter *time.Time
		switch name {
		case "stored":
			st = "stored"
		case "expired":
			d := now.Add(-time.Minute)
			deleteAfter = &d
		}
		key := "tks/media/" + id
		if _, err := e.s3.PutObject(ctx, "tks-media", key, bytes.NewReader([]byte("x")), 1, minio.PutObjectOptions{}); err != nil {
			t.Fatal(err)
		}
		if _, err := e.db.Exec(ctx, `INSERT INTO media (media_id, source_media_id, kind, variant, user_id, post_id, status, s3_bucket, s3_key,
			size_bytes, stored_at, delete_after) VALUES ($1::uuid, $1::text, 'cover', 'original', 7, 8, $2, 'tks-media', $3, 1, $4, $5)`,
			id, st, key, stored, deleteAfter); err != nil {
			t.Fatal(err)
		}
	}
	ack := func(id, consumer, status string) {
		if _, err := e.db.Exec(ctx, `INSERT INTO media_acks (media_id, consumer, status, acked_at) VALUES ($1, $2, $3, $4)`,
			id, consumer, status, now); err != nil {
			t.Fatal(err)
		}
	}
	ack(ids["acked"], "sb-analysis", "processed")
	ack(ids["acked"], "other", "skipped")
	ack(ids["partly"], "sb-analysis", "processed")
	ack(ids["retrying"], "sb-analysis", "failed_retryable")
	ack(ids["retrying"], "other", "processed")
	ack(ids["failed"], "sb-analysis", "failed_permanent")
	ack(ids["failed"], "other", "processed")

	s := &Sweeper{DB: e.db, S3: e.s3, Producer: e.prod, RequiredConsumers: []string{"sb-analysis", "other"}, DeleteAfter: 72 * time.Hour,
		Contract: true, Batch: 2, Now: func() time.Time { return now }}
	n, err := s.Sweep(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("%d files deleted, want 3 (acked, expired, failed)", n)
	}
	gone := map[string]bool{"acked": true, "expired": true, "failed": true}
	for name, id := range ids {
		st, _ := status(t, e.db, id)
		_, err := e.s3.StatObject(ctx, "tks-media", "tks/media/"+id, minio.StatObjectOptions{})
		if gone[name] != (st == "deleted") || gone[name] != (err != nil) {
			t.Errorf("%s: status %q, object err %v", name, st, err)
		}
	}
	// The events: tks's and the contract's, with the reason.
	reasons := map[string]string{}
	cl, err := kgo.NewClient(kgo.SeedBrokers(e.brokers...), kgo.ConsumeTopics(e.prefix+topics.MediaDeleted, e.prefix+content.TopicMediaDeleted),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	pctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for len(reasons) < 6 && pctx.Err() == nil {
		cl.PollFetches(pctx).EachRecord(func(r *kgo.Record) {
			if r.Topic == e.prefix+topics.MediaDeleted {
				var ev commonv1.MediaDeleted
				if err := proto.Unmarshal(r.Value, &ev); err == nil {
					reasons["tks:"+ev.MediaId] = ev.Reason.String()
				}
				return
			}
			var ev mediav1.MediaDeleted
			if err := proto.Unmarshal(r.Value, &ev); err == nil {
				reasons["contract:"+content.MediaID(ev.MediaId)] = ev.Reason.String()
			}
		})
	}
	want := map[string]string{
		"tks:" + ids["acked"]: "DELETE_REASON_ACKED", "tks:" + ids["failed"]: "DELETE_REASON_ACKED", "tks:" + ids["expired"]: "DELETE_REASON_TTL_EXPIRED",
		"contract:" + ids["acked"]: "DELETE_REASON_PROCESSED", "contract:" + ids["failed"]: "DELETE_REASON_PROCESSED",
		"contract:" + ids["expired"]: "DELETE_REASON_EXPIRED",
	}
	for k, v := range want {
		if reasons[k] != v {
			t.Errorf("%s: reason %q, want %q (all %v)", k, reasons[k], v, reasons)
		}
	}
	if n, err := s.Sweep(ctx); err != nil || n != 0 {
		t.Fatalf("second sweep: %d deleted, %v", n, err)
	}
	// The ledger's own handling of the deletion is idempotent: the row stays
	// deleted.
	if err := (&Ledger{DB: e.db}).deleted(ctx, deletion{mediaID: ids["acked"], reason: "acked"}); err != nil {
		t.Fatal(err)
	}
	if st, _ := status(t, e.db, ids["acked"]); st != "deleted" {
		t.Fatalf("acked file %q after the ledger", st)
	}
}
