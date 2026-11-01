package scrub_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"unicode"
	"unicode/utf16"

	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/parse"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/scrub"
)

// TestRealAnswers scrubs real answers and proves nothing personal is
// left: every username, nickname, secUid, id, text and signed link the
// parsers read from the original is searched for in the scrubbed copy. It
// also parses both and checks the scrubbed copy reads the same (records,
// id times, text lengths, counts). Real answers never enter git, so this
// runs only where they are: TKS_SCRUB_REAL_DIR=<dir of HAR bodies or probe
// raw/ files> go test ./internal/tt/scrub -run Real -v
func TestRealAnswers(t *testing.T) {
	dir := os.Getenv("TKS_SCRUB_REAL_DIR")
	if dir == "" {
		t.Skip("TKS_SCRUB_REAL_DIR not set: no real answers here (they never enter git)")
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*"))
	n := 0
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil || len(b) == 0 {
			continue
		}
		k, err := scrub.Detect(b)
		if err != nil {
			continue
		}
		n++
		name := "file" + strconv.Itoa(n) + "-" + string(k) // never the file's name: it may hold a username
		t.Run(name, func(t *testing.T) {
			out, err := scrub.Scrub(b, k, scrub.Options{})
			if err != nil {
				t.Fatal(err)
			}
			orig, scr := readAll(t, b, k), readAll(t, out, k)
			for _, s := range orig.secrets {
				if bytes.Contains(out, []byte(s)) || bytes.Contains(out, []byte(jsonEscaped(s))) {
					t.Errorf("a %s survived the scrub (%d bytes long) at %v", orig.what[s], len(s), where(out, s))
				}
			}
			for _, marker := range []string{"x-signature=", "signature=", "policy=", "tk=tt_chain_token"} {
				if bytes.Contains(out, []byte(marker)) {
					t.Errorf("a signed link survived (%s) at %v", marker, where(out, marker))
				}
			}
			if len(orig.shape) != len(scr.shape) {
				t.Fatalf("scrubbed copy reads %d records, the original %d", len(scr.shape), len(orig.shape))
			}
			for i := range orig.shape {
				if orig.shape[i] != scr.shape[i] {
					t.Errorf("record %d reads differently: %s, scrubbed %s", i, orig.shape[i], scr.shape[i])
				}
			}
		})
	}
	if n == 0 {
		t.Fatalf("no TikTok answers in %s (the path is read from the package directory)", dir)
	}
	t.Logf("%d real answers scrubbed", n)
}

type reading struct {
	secrets []string
	what    map[string]string
	shape   []string // per record: what must survive a scrub
}

func (r *reading) secret(what, s string, minLen int) {
	if len([]rune(s)) < minLen || r.what[s] != "" {
		return
	}
	if r.what == nil {
		r.what = map[string]string{}
	}
	r.what[s] = what
	r.secrets = append(r.secrets, s)
}

func (r *reading) id(what string, id int64) {
	if id != 0 {
		r.secret(what+" id", strconv.FormatInt(id, 10), 6)
	}
}

func (r *reading) person(what string, p *ttv1.Person) {
	if p == nil {
		return
	}
	r.id(what, p.GetUserId())
	r.secret(what+" secUid", p.SecUid, 10)
	r.secret(what+" username", p.Username, 3)
	r.name(what+" nickname", p.Nickname)
	r.link(what+" avatar", p.AvatarUrl)
}

// link records a media link's path (it identifies the file) and its
// signature.
func (r *reading) link(what, u string) {
	if i := strings.Index(u, "?"); i > 0 {
		r.secret(what+" link", u[:i], 20)
	}
}

func (r *reading) text(what, s string) {
	// Short texts ("ok", an emoji) are blanked too, but can occur by chance.
	if hasLetter(s) {
		r.secret(what, s, 8)
	}
}

// name records a nickname. One without letters (digits, emoji) is still
// replaced, but a short one can occur by chance in a pseudonymous id or
// in text TikTok wrote, so only longer ones are searched for.
func (r *reading) name(what, s string) {
	if hasLetter(s) {
		r.secret(what, s, 3)
	} else {
		r.secret(what, s, 6)
	}
}

func hasLetter(s string) bool { return strings.IndexFunc(s, unicode.IsLetter) >= 0 }

func u16(s string) int { return len(utf16.Encode([]rune(s))) }

func readAll(t *testing.T, b []byte, k scrub.Kind) *reading {
	t.Helper()
	r := &reading{}
	acc := func(a *ttv1.Account) {
		if a == nil {
			return
		}
		r.id("user", a.UserId)
		r.secret("secUid", a.SecUid, 10)
		r.secret("username", a.Username, 3)
		r.name("nickname", a.Nickname)
		r.text("bio", a.Bio)
		r.secret("bio link", a.GetBioLink(), 8)
		r.link("avatar", a.AvatarUrl)
		r.shape = append(r.shape, "account "+strconv.Itoa(u16(a.Bio))+" "+a.GetCounts().String()+" "+a.Status.String())
	}
	post := func(b parse.PostBundle) {
		p := b.Post
		r.id("post", p.PostId)
		r.id("user", p.UserId)
		r.text("caption", p.Caption)
		for _, tr := range p.TextRanges {
			r.person("mention", tr.Person)
		}
		if b.Music != nil {
			r.id("music", b.Music.MusicId)
			r.text("music title", b.Music.Title)
			r.secret("music author", b.Music.AuthorName, 4)
		}
		for _, m := range b.Media {
			r.link("media", m.SourceUrl)
		}
		r.shape = append(r.shape, "post "+strconv.FormatInt(p.PostId>>32, 10)+" "+strconv.Itoa(u16(p.Caption))+" "+
			p.Kind.String()+" "+strconv.Itoa(len(p.TextRanges))+" "+strconv.Itoa(len(b.Media))+" "+p.Counts.String())
	}
	switch k {
	case scrub.ProfilePage, scrub.UserDetail:
		var res parse.AccountResult
		if k == scrub.ProfilePage {
			res, _ = parse.ProfilePage(b, parse.Options{})
		} else {
			res, _ = parse.UserDetail(b, parse.Options{})
		}
		acc(res.Account)
		r.shape = append(r.shape, "status "+res.Status.String())
	case scrub.PostPage:
		res, rep := parse.PostPage(b, parse.Options{})
		if res.Post != nil {
			post(*res.Post)
		}
		acc(res.Account)
		r.shape = append(r.shape, "missing "+strings.Join(rep.Missing, ","))
	case scrub.PostList, scrub.CreatorList:
		res, rep := parse.PostList(b, parse.Options{})
		for _, p := range res.Posts {
			post(p)
		}
		acc(res.Account)
		r.shape = append(r.shape, "missing "+strings.Join(rep.Missing, ",")+" more "+strconv.FormatBool(res.HasMore))
	case scrub.Comments, scrub.Replies:
		res, rep := parse.Comments(b, parse.CommentContext{Replies: k == scrub.Replies}, parse.Options{})
		for _, c := range res.Comments {
			cm := c.Comment
			r.id("comment", cm.CommentId)
			r.id("post", cm.PostId)
			r.person("commenter", cm.Author)
			r.text("comment text", cm.Text)
			for _, tr := range cm.TextRanges {
				r.person("mention", tr.Person)
			}
			for _, m := range c.Media {
				r.link("comment image", m.SourceUrl)
			}
			r.shape = append(r.shape, "comment "+strconv.FormatInt(cm.CommentId>>32, 10)+" "+strconv.Itoa(u16(cm.Text))+" "+
				strconv.Itoa(int(cm.Depth))+" "+strconv.FormatInt(cm.GetLikeCount(), 10)+" "+strconv.Itoa(len(cm.TextRanges)))
		}
		r.shape = append(r.shape, "missing "+strings.Join(rep.Missing, ",")+" total "+strconv.FormatInt(res.Total, 10))
	}
	return r
}

// jsonEscaped is s as encoding/json writes it inside a string (HTML-safe).
func jsonEscaped(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "<", `<`, ">", `>`, "&", `&`, "/", `\/`)
	return r.Replace(s)
}

var script = regexp.MustCompile(`(?s)<script[^>]*>(.*?)</script>`)

// where lists the key paths (never the values) of the strings in a
// scrubbed answer that hold s, for a failure message.
func where(out []byte, s string) []string {
	if m := script.FindSubmatch(out); m != nil {
		out = m[1]
	}
	var v any
	if json.Unmarshal(out, &v) != nil {
		return nil
	}
	set := map[string]bool{}
	var walk func(v any, p string)
	walk = func(v any, p string) {
		switch v := v.(type) {
		case map[string]any:
			for k, x := range v {
				walk(x, p+"."+k)
			}
		case []any:
			for _, x := range v {
				walk(x, p+"[]")
			}
		case string:
			if strings.Contains(v, s) {
				set[p] = true
			}
		}
	}
	walk(v, "")
	var ps []string
	for p := range set {
		ps = append(ps, p)
	}
	sort.Strings(ps)
	return ps
}
