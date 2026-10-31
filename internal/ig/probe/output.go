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

// Write puts the report in dir: report.json, and profiles.csv, posts.csv
// and comments.csv for a spreadsheet. Every file is 0600: the report holds
// personal data. The profile pictures are already in dir/images.
func Write(dir string, rep Report) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(rep, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, "report.json"), append(b, '\n'), 0o600); err != nil {
		return err
	}
	profiles := [][]string{{"line", "username", "url", "full_name", "followers", "following", "verified", "private",
		"biography", "bio_links", "linked_facebook_page", "highlights", "profile_pic_file", "profile_pic_url",
		"page_load", "posts_read", "grid_stop", "oldest_post_read", "comments_read"}}
	posts := [][]string{{"username", "shortcode", "url", "posted_at", "media_type", "product_type", "caption", "alt_text", "thumbnail_url"}}
	comments := [][]string{{"username", "post_shortcode", "commenter", "commenter_verified", "created_at", "likes", "text"}}
	for _, p := range rep.Profiles {
		row := []string{strconv.Itoa(p.Index), p.Username, p.URL}
		if d := p.Data; d != nil {
			var links, hl []string
			for _, l := range d.BioLinks {
				links = append(links, l.URL)
			}
			for _, h := range d.Highlights {
				hl = append(hl, h.Title)
			}
			row = append(row, d.FullName, i64(d.Followers), i64(d.Following), strconv.FormatBool(d.Verified),
				strconv.FormatBool(d.Private), d.Biography, strings.Join(links, " "), d.LinkedFacebookPage,
				strings.Join(hl, " | "), d.ProfilePicFile, d.ProfilePicURL)
		} else {
			row = append(row, make([]string, 11)...)
		}
		stop, oldest := "", ""
		if p.Grid != nil {
			stop = p.Grid.Stop
			if !p.Grid.Oldest.IsZero() {
				oldest = p.Grid.Oldest.Format(time.RFC3339)
			}
		}
		row = append(row, p.Bootstrap.Class, strconv.Itoa(len(p.Posts)), stop, oldest, strconv.Itoa(len(p.Comments)))
		profiles = append(profiles, row)
		for _, x := range p.Posts {
			posted := ""
			if !x.PostedAt.IsZero() {
				posted = x.PostedAt.Format(time.RFC3339)
			}
			posts = append(posts, []string{p.Username, x.Shortcode, x.URL, posted, x.MediaType, x.ProductType, x.Caption, x.AltText, x.ThumbnailURL})
		}
		for _, c := range p.Comments {
			comments = append(comments, []string{p.Username, c.PostShortcode, c.Username, strconv.FormatBool(c.Verified),
				c.CreatedAt.Format(time.RFC3339), i64(c.Likes), c.Text})
		}
	}
	for name, rows := range map[string][][]string{"profiles.csv": profiles, "posts.csv": posts, "comments.csv": comments} {
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
