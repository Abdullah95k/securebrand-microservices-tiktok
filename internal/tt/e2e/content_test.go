package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-contracts/conformance"
	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/content"
)

// published is what the persister sent on content.*, by topic.
type published map[string][]proto.Message

// readContent reads every content.* topic from the start to its current
// end.
func readContent(t *testing.T, s *Stack) published {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	prefix := s.Cfg.Kafka.TopicPrefix
	names := make([]string, len(content.Topics))
	for i, tp := range content.Topics {
		names[i] = prefix + tp
	}
	ends, err := kafka.EndOffsets(ctx, s.Producer.Client(), names...)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]map[int32]int64{}
	total := int64(0)
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			if want[o.Topic] == nil {
				want[o.Topic] = map[int32]int64{}
			}
			want[o.Topic][o.Partition] = o.Offset
			total += o.Offset
		}
	})
	cl, err := kgo.NewClient(kgo.SeedBrokers(s.Cfg.Kafka.Brokers...), kgo.ConsumeTopics(names...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	out := published{}
	var read int64
	for read < total {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("read %d of %d content records", read, total)
		}
		fs.EachRecord(func(r *kgo.Record) {
			if r.Offset >= want[r.Topic][r.Partition] {
				return
			}
			read++
			name := kafka.HeaderValue(r, content.HeaderType)
			mt, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(name))
			if err != nil {
				t.Errorf("%s: sb-type %q: %v", r.Topic, name, err)
				return
			}
			m := mt.New().Interface()
			if err := proto.Unmarshal(r.Value, m); err != nil {
				t.Errorf("%s: %v", r.Topic, err)
				return
			}
			if kafka.HeaderValue(r, content.HeaderContract) == "" {
				t.Errorf("%s: no sb-contract header", r.Topic)
			}
			tp := r.Topic[len(prefix):]
			out[tp] = append(out[tp], m)
		})
	}
	return out
}

// checkContent checks the contract's events against the store: every one
// conforms (production checks), and every stored account, post and
// comment has its event, with the stored post's latest caption.
func checkContent(t *testing.T, s *Stack) {
	t.Helper()
	pub := readContent(t, s)
	opt := conformance.Options{Production: true}
	for tp, ms := range pub {
		for _, m := range ms {
			if err := content.Check(m, opt); err != nil {
				t.Errorf("%s: %v", tp, err)
			}
		}
	}
	for _, tp := range content.Topics {
		if len(pub[tp]) == 0 {
			t.Errorf("nothing published on %s", tp)
		}
	}
	captions := map[string]string{}
	for _, m := range pub["content.post.v1"] {
		p := m.(*contentv1.Post)
		if !p.Envelope.Partial {
			captions[p.Envelope.Id] = p.GetText().GetText()
		}
	}
	rows, err := s.DB.Query(context.Background(), `SELECT 'tt:post:' || post_id, coalesce(caption, '') FROM posts WHERE status = 'visible'`)
	if err != nil {
		t.Fatal(err)
	}
	stored := 0
	for rows.Next() {
		var id, caption string
		if err := rows.Scan(&id, &caption); err != nil {
			t.Fatal(err)
		}
		stored++
		if got, ok := captions[id]; !ok {
			t.Errorf("stored post %s has no content.post event", id)
		} else if got != caption {
			t.Errorf("post %s: last event's caption differs from the store's", id)
		}
	}
	rows.Close()
	ids := func(tp string) map[string]bool {
		out := map[string]bool{}
		for _, m := range pub[tp] {
			out[m.ProtoReflect().Get(m.ProtoReflect().Descriptor().Fields().ByName("envelope")).Message().Interface().(*contentv1.Envelope).Id] = true
		}
		return out
	}
	comments, accounts := ids("content.comment.v1"), ids("content.account.v1")
	if n := count(t, s, `SELECT count(*) FROM comments`); int64(len(comments)) < n {
		t.Errorf("%d comments in content.comment.v1, %d stored", len(comments), n)
	}
	if n := count(t, s, `SELECT count(*) FROM accounts WHERE page_read_at IS NOT NULL`); int64(len(accounts)) < n {
		t.Errorf("%d accounts in content.account.v1, %d read from their page", len(accounts), n)
	}
	checkCaptions(t, s, pub)
	t.Logf("content events: %d posts (%d stored), %d comments, %d accounts, %d metrics", len(pub["content.post.v1"]), stored,
		len(pub["content.comment.v1"]), len(pub["content.account.v1"]), len(pub["content.metrics.v1"]))
}

// checkCaptions checks M17's fields against the store: the latest captions
// a post's events carry (full or partial) are its stored subtitle files'
// text, speech-to-text first; a photo post's title and a post's AI text are
// on its latest full event.
func checkCaptions(t *testing.T, s *Stack, pub published) {
	t.Helper()
	captions := map[string][]*contentv1.CaptionTrack{}
	full := map[string]*contentv1.Post{}
	for _, m := range pub["content.post.v1"] {
		p := m.(*contentv1.Post)
		if len(p.Captions) > 0 {
			captions[p.Envelope.Id] = p.Captions
		}
		if !p.Envelope.Partial {
			full[p.Envelope.Id] = p
		}
	}
	rows, err := s.DB.Query(context.Background(), `SELECT 'tt:post:' || post_id, language, body FROM post_subtitles
		WHERE body IS NOT NULL ORDER BY post_id, source, language`)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{}
	for rows.Next() {
		var id, lang, body string
		if err := rows.Scan(&id, &lang, &body); err != nil {
			t.Fatal(err)
		}
		want[id] = append(want[id], content.SubtitleLanguage(lang)+"|"+content.VTTText(body))
	}
	rows.Close()
	if len(want) == 0 {
		t.Error("no subtitles stored")
	}
	for id, w := range want {
		var got []string
		for _, c := range captions[id] {
			got = append(got, c.Language+"|"+c.Text)
		}
		if strings.Join(got, "\n") != strings.Join(w, "\n") {
			t.Errorf("post %s: captions %q, want %q", id, got, w)
		}
	}
	rows, err = s.DB.Query(context.Background(), `SELECT 'tt:post:' || post_id, coalesce(title, ''), coalesce(labels->>'ai_description', '')
		FROM posts WHERE status = 'visible'`)
	if err != nil {
		t.Fatal(err)
	}
	titles := 0
	for rows.Next() {
		var id, title, ai string
		if err := rows.Scan(&id, &title, &ai); err != nil {
			t.Fatal(err)
		}
		if title != "" {
			titles++
		}
		if p := full[id]; p.GetTitle().GetText() != title || p.GetLabels().GetAiLabelText() != ai {
			t.Errorf("post %s: event's title %q and AI text %q, stored %q and %q", id, p.GetTitle().GetText(),
				p.GetLabels().GetAiLabelText(), title, ai)
		}
	}
	rows.Close()
	if titles == 0 {
		t.Error("no photo post's title stored")
	}
}
