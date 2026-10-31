package spike

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	fbv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/parse"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakefb"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/fbtest"
)

func runner(t *testing.T, faults fakefb.Faults) (*Runner, *fbtest.Env, *bytes.Buffer) {
	env := fbtest.New(t, fbtest.Options{Faults: faults})
	var out bytes.Buffer
	return &Runner{Pool: env.Pool, Loader: env.Loader, GQL: env.GQL, HTTP: http.DefaultClient, Out: &out,
		Opt: Options{Tier: "direct", MaxTimelinePages: 50, PostsPerPage: 3, MaxCommentPages: 50, ThreadsPerPost: 500,
			Orderings: []string{"comments", "comments_newest", "comments_relevant"}, MediaSamples: 2, Concurrency: 2,
			PageTimeout: time.Minute}}, env, &out
}

func TestSpikeAgainstFakeFacebook(t *testing.T) {
	r, env, out := runner(t, fakefb.Faults{})
	var pages []PageRef
	for _, p := range env.World.Pages() {
		pages = append(pages, PageRef{Ref: p.Vanity})
	}
	r.Run(context.Background(), pages)
	s, err := Report(bytes.NewReader(out.Bytes()), Thresholds{MinMedianCompleteness: 0.7, MinBootstrapSuccess: 0.9, MaxBlockRate: 0.05, MinMedianDays: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("\n" + Format(s))
	if !s.Go || s.BootstrapSuccess != 1 || s.Portability != 1 || s.MediaSuccess != 1 {
		t.Fatalf("unexpected summary: %+v", s)
	}
	// The unfiltered ordering is complete; "most relevant" hides comments.
	if s.Completeness["comments"].Median != 1 || s.Completeness["comments_relevant"].Median >= 1 {
		t.Fatalf("ordering comparison wrong: %+v", s.Completeness)
	}
}

func TestSpikeReportsLoginWalls(t *testing.T) {
	r, env, out := runner(t, fakefb.Faults{LoginWallAfter: 3})
	r.Run(context.Background(), []PageRef{{Ref: env.World.Pages()[0].Vanity}})
	s, _ := Report(bytes.NewReader(out.Bytes()), DefaultThresholds)
	if s.Go || s.BlockRate == 0 {
		t.Fatalf("login walls must fail the verdict: %+v", s)
	}
}

func TestSamplePosts(t *testing.T) {
	mk := func(counts ...int64) []*parse.PostBundle {
		var out []*parse.PostBundle
		for i, c := range counts {
			out = append(out, &parse.PostBundle{Post: &fbv1.Post{PostId: int64(i), CommentCount: &c}})
		}
		return out
	}
	ids := func(bs []*parse.PostBundle) (out []int64) {
		for _, b := range bs {
			out = append(out, b.Post.GetCommentCount())
		}
		return out
	}
	posts := mk(0, 5000, 120, 300, 80, 900, 301)
	if got := ids(SamplePosts(posts, 3, 300)); fmt.Sprint(got) != "[300 120 80]" {
		t.Fatal(got)
	}
	if got := ids(SamplePosts(posts, 5, 300)); fmt.Sprint(got) != "[300 120 80 301 900]" {
		t.Fatal(got)
	}
	if got := ids(SamplePosts(posts, 2, 0)); fmt.Sprint(got) != "[5000 900]" {
		t.Fatal(got)
	}
}
