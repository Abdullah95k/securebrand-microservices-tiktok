package probe

import (
	"archive/tar"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Write puts the report in dir: report.json, and accounts.csv, posts.csv
// and comments.csv for a spreadsheet. Every file is 0600: the report holds
// personal data. Avatars, covers and videos are already in dir.
func Write(dir string, rep Report) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(rep, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, "report.json"), append(b, '\n'), 0o600); err != nil {
		return err
	}
	accounts := [][]string{{"line", "username", "url", "id", "sec_uid", "nickname", "bio", "bio_link", "verified", "private",
		"followers", "following", "likes", "videos", "created_at", "renamed_at", "avatar_file", "page", "page_form",
		"list", "list_pages", "posts_read", "list_stop", "oldest_post_read", "comments_read", "error"}}
	posts := [][]string{{"username", "id", "link", "kind", "posted_at", "id_time", "caption", "hashtags", "mentions", "duration",
		"pinned", "ad", "ai_label", "plays", "likes", "comments", "shares", "saves", "reposts",
		"music_id", "music_title", "music_author", "music_original", "cover_file", "cover_url"}}
	comments := [][]string{{"username", "post_id", "comment_id", "parent_id", "author_id", "author_username", "author_nickname",
		"created_at", "likes", "replies", "page", "text"}}
	for _, a := range rep.Accounts {
		row := []string{strconv.Itoa(a.Index), a.Username, a.URL}
		if d := a.Data; d != nil {
			row = append(row, d.ID, d.SecUID, d.Nickname, d.Bio, d.BioLink, strconv.FormatBool(d.Verified), strconv.FormatBool(d.Private),
				i64(d.Followers), i64(d.Following), i64(d.Likes), i64(d.Videos), unix(d.CreatedAt), unix(d.RenamedAt), d.AvatarFile)
		} else {
			row = append(row, make([]string, 14)...)
		}
		list, pages, stop, oldest := "", "", "", ""
		if w := a.Walk; w != nil {
			list, pages, stop = w.Variant, strconv.Itoa(w.Pages+1), w.Stop
			if !w.Oldest.IsZero() {
				oldest = w.Oldest.Format(time.RFC3339)
			}
		}
		row = append(row, a.Page.Class, a.PageForm, list, pages, strconv.Itoa(len(a.Posts)), stop, oldest, strconv.Itoa(len(a.Comments)), a.Error)
		accounts = append(accounts, row)
		for _, p := range a.Posts {
			posts = append(posts, []string{a.Username, p.ID, p.Link, p.Kind, unix(p.PostedAt), unix(p.IDTime), p.Caption,
				strings.Join(p.Hashtags, " "), strings.Join(p.Mentions, " "), i64(p.Duration), strconv.FormatBool(p.Pinned),
				strconv.FormatBool(p.Ad), p.AILabel, i64(p.Plays), i64(p.Likes), i64(p.Comments), i64(p.Shares), i64(p.Saves),
				i64(p.Reposts), p.MusicID, p.MusicTitle, p.MusicAuthor, strconv.FormatBool(p.MusicOrig), p.CoverFile, p.CoverURL})
		}
		for _, c := range a.Comments {
			comments = append(comments, []string{a.Username, c.PostID, c.ID, c.ParentID, c.AuthorID, c.AuthorUsername, c.AuthorNickname,
				unix(c.CreatedAt), i64(c.Likes), i64(c.Replies), strconv.Itoa(c.Page), c.Text})
		}
	}
	for name, rows := range map[string][][]string{"accounts.csv": accounts, "posts.csv": posts, "comments.csv": comments} {
		if err := writeCSV(filepath.Join(dir, name), rows); err != nil {
			return err
		}
	}
	return nil
}

func writeCSV(path string, rows [][]string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600) //nolint:gosec // the operator's output dir
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	if err := w.WriteAll(rows); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// Bundle packs dir into dir.tar.gz (0600) for downloading, and returns its
// path.
func Bundle(dir string) (string, error) {
	out := strings.TrimRight(dir, "/") + ".tar.gz"
	f, err := os.OpenFile(out, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600) //nolint:gosec // the operator's output dir
	if err != nil {
		return "", err
	}
	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)
	root := filepath.Base(dir)
	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(dir, path)
		if err := tw.WriteHeader(&tar.Header{Name: filepath.ToSlash(filepath.Join(root, rel)), Mode: 0o600,
			Size: info.Size(), ModTime: info.ModTime()}); err != nil {
			return err
		}
		src, err := os.Open(path) //nolint:gosec // walking our own output dir
		if err != nil {
			return err
		}
		defer func() { _ = src.Close() }()
		_, err = io.Copy(tw, src)
		return err
	})
	for _, c := range []io.Closer{tw, gz, f} {
		if cerr := c.Close(); err == nil {
			err = cerr
		}
	}
	if err != nil {
		return "", fmt.Errorf("probe: bundle: %w", err)
	}
	return out, nil
}

func i64(v int64) string { return strconv.FormatInt(v, 10) }
