package client_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/tidwall/gjson"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/igtest"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
)

func bootstrap(t *testing.T, env *igtest.Env, username string) *session.Identity {
	t.Helper()
	id, err := env.Pool.Acquire(context.Background(), "direct")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { env.Pool.Release(id) })
	if _, err := env.Loader.Bootstrap(context.Background(), id, env.HTTP.URL+"/"+username); err != nil {
		t.Fatal(err)
	}
	return id
}

func do(t *testing.T, env *igtest.Env, id *session.Identity, q string, vars map[string]any) *graphql.Response {
	t.Helper()
	resp, err := env.GQL.Do(context.Background(), id, graphql.Request{Query: q, Vars: vars})
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// Over both transports: a page load bootstraps an identity (tokens and the
// csrftoken cookie), and the five operations read a profile, its whole
// grid, a post's top-level comments and a highlight.
func TestClientAgainstFakeig(t *testing.T) {
	for _, useTLS := range []bool{false, true} {
		name := map[bool]string{false: "std", true: "tls"}[useTLS]
		t.Run(name, func(t *testing.T) {
			env := igtest.New(t, igtest.Options{UseTLS: useTLS})
			p := env.World.Profiles()[0]
			id := bootstrap(t, env, p.Username)
			if !id.Bootstrapped() || id.Tokens()["csrf_token"] == "" || id.Doer.Cookie(id.Base, "csrftoken") == "" {
				t.Fatalf("bootstrap: tokens %v", id.Tokens())
			}

			prof := do(t, env, id, "profile", map[string]any{"username": p.Username})
			u := prof.Parts[0].Get("data.xig_user_by_username")
			if prof.Class != detect.OK || u.Get("username").String() != p.Username || u.Get("pk").Int() != p.PK {
				t.Fatalf("profile: %s %s", prof.Class, prof.Body)
			}

			first := do(t, env, id, "grid_first", map[string]any{"username": p.Username})
			conn := first.Parts[0].Get("data.xig_user_by_username.polaris_ordered_timeline_connection")
			seen := len(conn.Get("edges").Array())
			cursor := conn.Get("page_info.end_cursor").String()
			for conn.Get("page_info.has_next_page").Bool() {
				next := do(t, env, id, "grid", map[string]any{"user_id": u.Get("id").String(), "cursor": cursor})
				if next.Class != detect.OK {
					t.Fatalf("grid page after %d: %s %s", seen, next.Class, next.Reason)
				}
				conn = next.Parts[0].Get("data.node.polaris_ordered_timeline_connection")
				seen += len(conn.Get("edges").Array())
				cursor = conn.Get("page_info.end_cursor").String()
			}
			if seen != len(p.Posts) {
				t.Fatalf("grid: %d posts, want %d", seen, len(p.Posts))
			}

			var post *fakeig.Post
			for _, x := range p.Posts {
				if len(x.Comments) > fakeig.CommentPageSize {
					post = x
					break
				}
			}
			if post == nil {
				t.Fatal("no post with more than one comment page")
			}
			// By pk the grid's connection query finds no user, as on
			// Instagram: it takes the user's id.
			if r := do(t, env, id, "grid", map[string]any{"user_id": u.Get("pk").String()}); r.Parts[0].Get("data.node").Type != gjson.Null {
				t.Fatalf("grid by pk: %s", r.Body)
			}
			comments, cursor := 0, ""
			for {
				vars := map[string]any{"media_pk": strconv.FormatInt(post.PK, 10)}
				if cursor != "" {
					vars["cursor"] = cursor
				}
				resp := do(t, env, id, "comments", vars)
				cc := resp.Parts[0].Get("data.xig_polaris_media.comments_connection")
				if resp.Class != detect.OK {
					t.Fatalf("comments: %s %s", resp.Class, resp.Reason)
				}
				comments += len(cc.Get("edges").Array())
				if !cc.Get("page_info.has_next_page").Bool() {
					break
				}
				cursor = cc.Get("page_info.end_cursor").String()
			}
			if comments != len(post.Comments) {
				t.Fatalf("comments: %d, want %d", comments, len(post.Comments))
			}

			h := u.Get("lox_highlights_connection.edges.0.node.id").String()
			hl := do(t, env, id, "highlights", map[string]any{"reel_ids": []string{h}, "initial_reel_id": h})
			if hl.Class != detect.OK || len(hl.Parts[0].Get("data.xdt_api__v1__feed__reels_media__connection.edges.0.node.items").Array()) == 0 {
				t.Fatalf("highlights: %s %s", hl.Class, hl.Body)
			}
		})
	}
}

// Walls and faults classify as the crawler needs them to.
func TestFakeigFaults(t *testing.T) {
	ctx := context.Background()
	t.Run("login wall after N queries", func(t *testing.T) {
		env := igtest.New(t, igtest.Options{Faults: fakeig.Faults{LoginWallAfter: 2}})
		p := env.World.Profiles()[0]
		id := bootstrap(t, env, p.Username)
		vars := map[string]any{"username": p.Username}
		for i := range 2 {
			if r := do(t, env, id, "profile", vars); r.Class != detect.OK {
				t.Fatalf("query %d: %s", i+1, r.Class)
			}
		}
		if r := do(t, env, id, "profile", vars); r.Class != detect.LoginWall {
			t.Fatalf("query 3: %s %s", r.Class, r.Reason)
		}
		// The walled identity's next page load is redirected to the login
		// page, which the loader classifies instead of following.
		pl, err := env.Loader.Load(ctx, id, env.HTTP.URL+"/"+p.Username+"/")
		if err != nil || pl.Class != detect.LoginWall {
			t.Fatalf("page load: %v %+v", err, pl)
		}
	})
	t.Run("challenge redirect", func(t *testing.T) {
		env := igtest.New(t, igtest.Options{Faults: fakeig.Faults{Challenge: true}})
		id, _ := env.Pool.Acquire(ctx, "direct")
		defer env.Pool.Release(id)
		pl, err := env.Loader.Bootstrap(ctx, id, env.HTTP.URL+"/"+env.World.Profiles()[0].Username)
		if err == nil || pl == nil || pl.Class != detect.Checkpoint {
			t.Fatalf("challenge: %v %+v", err, pl)
		}
	})
	t.Run("rotated doc_ids", func(t *testing.T) {
		env := igtest.New(t, igtest.Options{RotateDocIDs: true})
		p := env.World.Profiles()[0]
		id := bootstrap(t, env, p.Username)
		if r := do(t, env, id, "profile", map[string]any{"username": p.Username}); r.Class != detect.RegistryStale {
			t.Fatalf("stale doc_id: %s %s", r.Class, r.Reason)
		}
	})
	t.Run("rate limit", func(t *testing.T) {
		env := igtest.New(t, igtest.Options{Faults: fakeig.Faults{RateLimitEvery: 1}})
		p := env.World.Profiles()[0]
		id := bootstrap(t, env, p.Username)
		if r := do(t, env, id, "profile", map[string]any{"username": p.Username}); r.Class != detect.RateLimited {
			t.Fatalf("rate limit: %s", r.Class)
		}
	})
	t.Run("client state required", func(t *testing.T) {
		env := igtest.New(t, igtest.Options{Faults: fakeig.Faults{RequireClientState: true}})
		p := env.World.Profiles()[0]
		id := bootstrap(t, env, p.Username)
		if r := do(t, env, id, "profile", map[string]any{"username": p.Username}); r.Class == detect.OK {
			t.Fatal("a query without __dyn succeeded")
		}
	})
}
