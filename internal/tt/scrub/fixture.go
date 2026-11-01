package scrub

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Write scrubs an answer of kind k and stores it as a golden fixture,
// root/<kind>/<name>/<kind's file> with a meta.json. The raw answer never
// reaches the disk: a failed scrub writes nothing.
func Write(root string, k Kind, name string, body []byte, meta map[string]any, o Options) (string, error) {
	if name == "" || strings.ContainsAny(name, `/\`) {
		return "", fmt.Errorf("scrub: bad fixture name %q", name)
	}
	out, err := Scrub(body, k, o)
	if err != nil {
		return "", err
	}
	dir := filepath.Join(root, string(k), name)
	if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:gosec // fixtures are meant to be committed
		return "", err
	}
	if err := os.WriteFile(filepath.Join(dir, k.File()), out, 0o644); err != nil { //nolint:gosec // fixtures are meant to be committed
		return "", err
	}
	if meta == nil {
		meta = map[string]any{}
	}
	meta["kind"] = string(k)
	meta["captured_at"] = time.Now().UTC().Format(time.RFC3339)
	return dir, writeMeta(dir, meta)
}

// Meta is what a fixture's meta.json says about it.
type Meta struct {
	Kind     Kind   `json:"kind"`
	Source   string `json:"source"` // har | probe | fakett | handcrafted
	Scrubbed bool   `json:"__scrubbed"`
	Scrubber int    `json:"__scrubber"`
}

// ReadMeta reads dir/meta.json.
func ReadMeta(dir string) (Meta, error) {
	var m Meta
	b, err := os.ReadFile(filepath.Join(dir, "meta.json")) //nolint:gosec // a fixture directory the operator names
	if err != nil {
		return m, err
	}
	return m, json.Unmarshal(b, &m)
}

// Synthetic reports fixtures with no real data in them (written by fakett
// or by hand), which are never scrubbed.
func (m Meta) Synthetic() bool { return m.Source == "fakett" || m.Source == "handcrafted" }

// ScrubDir scrubs a fixture directory again when an older scrubber wrote
// it. Its pseudonyms are pseudonymised again, which keeps them consistent
// within the fixture.
func ScrubDir(dir string) error {
	m, err := ReadMeta(dir)
	if err != nil {
		return err
	}
	if m.Synthetic() || (m.Scrubbed && m.Scrubber >= Version) {
		return nil
	}
	p := filepath.Join(dir, m.Kind.File())
	b, err := os.ReadFile(p) //nolint:gosec // a fixture directory the operator names
	if err != nil {
		return err
	}
	out, err := Scrub(b, m.Kind, Options{})
	if err != nil {
		return err
	}
	if err := os.WriteFile(p, out, 0o644); err != nil { //nolint:gosec // fixtures are meant to be committed
		return err
	}
	raw := map[string]any{}
	if b, err := os.ReadFile(filepath.Join(dir, "meta.json")); err == nil { //nolint:gosec // as above
		_ = json.Unmarshal(b, &raw)
	}
	return writeMeta(dir, raw)
}

func writeMeta(dir string, meta map[string]any) error {
	meta["__scrubbed"] = true
	meta["__scrubber"] = Version
	b, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "meta.json"), append(b, '\n'), 0o644) //nolint:gosec // fixtures are meant to be committed
}

// matches reports whether a request URL returned an answer of kind k.
func (k Kind) matches(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || !strings.HasSuffix(u.Hostname(), "tiktok.com") {
		return false
	}
	p := u.Path
	switch k {
	case ProfilePage:
		return strings.HasPrefix(p, "/@") && !strings.Contains(p, "/video/") && !strings.Contains(p, "/photo/")
	case PostPage:
		return strings.HasPrefix(p, "/@") && strings.Contains(p, "/video/")
	case PostList:
		return p == "/api/post/item_list/"
	case CreatorList:
		return p == "/api/creator/item_list/"
	case UserDetail:
		return p == "/api/user/detail/"
	case Comments:
		return p == "/api/comment/list/"
	case Replies:
		return p == "/api/comment/list/reply/"
	}
	return false
}

// FromHAR returns the body of the nth (from 1) answer of kind k in a HAR
// file, skipping answers without a body.
func FromHAR(b []byte, k Kind, n int) ([]byte, error) {
	var h struct {
		Log struct {
			Entries []struct {
				Request struct {
					URL string `json:"url"`
				} `json:"request"`
				Response struct {
					Content struct {
						Text     string `json:"text"`
						Encoding string `json:"encoding"`
					} `json:"content"`
				} `json:"response"`
			} `json:"entries"`
		} `json:"log"`
	}
	if err := json.Unmarshal(b, &h); err != nil {
		return nil, fmt.Errorf("scrub: not a HAR file: %w", err)
	}
	seen := 0
	for _, e := range h.Log.Entries {
		c := e.Response.Content
		if !k.matches(e.Request.URL) || c.Text == "" {
			continue
		}
		if seen++; seen < n {
			continue
		}
		if c.Encoding == "base64" {
			return base64.StdEncoding.DecodeString(c.Text)
		}
		return []byte(c.Text), nil
	}
	return nil, fmt.Errorf("scrub: the HAR has %d %s answers with a body, not %d", seen, k, n)
}

// Detect guesses an answer's kind from its content.
func Detect(b []byte) (Kind, error) {
	t := bytes.TrimSpace(b)
	if len(t) > 0 && t[0] == '<' {
		switch {
		case bytes.Contains(b, []byte(`"webapp.video-detail"`)):
			return PostPage, nil
		case bytes.Contains(b, []byte(`"webapp.user-detail"`)):
			return ProfilePage, nil
		}
		return "", errors.New("scrub: a page with no account or post scope")
	}
	v, err := decode(t)
	if err != nil {
		return "", fmt.Errorf("scrub: not JSON: %w", err)
	}
	m, _ := v.(map[string]any)
	switch {
	case m == nil:
	case m["itemList"] != nil && m["hasMorePrevious"] != nil:
		return CreatorList, nil
	case m["itemList"] != nil || m["hasMore"] != nil:
		return PostList, nil
	case m["userInfo"] != nil:
		return UserDetail, nil
	case m["comments"] != nil:
		for _, c := range asList(m["comments"]) {
			if cm, ok := c.(map[string]any); ok && nonEmpty(cm["reply_id"]) {
				return Replies, nil
			}
		}
		return Comments, nil
	}
	return "", errors.New("scrub: unknown answer")
}

func asList(v any) []any { xs, _ := v.([]any); return xs }
