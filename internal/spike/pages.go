package spike

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/task"
)

// PageRef is one page to measure. Labels (size, category, region, ...) come
// from the pages file and let the report break results down by stratum.
type PageRef struct {
	Ref    string
	Labels map[string]string
}

// ReadPages parses a pages file. Two formats are accepted:
//
//   - one page URL, vanity or id per line (extra comma-separated fields are
//     ignored);
//   - CSV whose header starts with "page", e.g. page,size,category,region,language.
//
// Blank lines and lines starting with # are skipped.
func ReadPages(r io.Reader) ([]PageRef, error) {
	cr := csv.NewReader(r)
	cr.Comment = '#'
	cr.FieldsPerRecord = -1
	cr.TrimLeadingSpace = true
	var header []string
	var out []PageRef
	for {
		rec, err := cr.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("spike: pages: %w", err)
		}
		if len(rec) == 0 || strings.TrimSpace(rec[0]) == "" {
			continue
		}
		if header == nil && len(out) == 0 && strings.EqualFold(strings.TrimSpace(rec[0]), "page") {
			for _, h := range rec {
				header = append(header, strings.ToLower(strings.TrimSpace(h)))
			}
			continue
		}
		p := PageRef{Ref: strings.TrimSpace(rec[0])}
		for i := 1; i < len(rec) && i < len(header); i++ {
			if v := strings.TrimSpace(rec[i]); v != "" && header[i] != "" {
				if p.Labels == nil {
					p.Labels = map[string]string{}
				}
				p.Labels[header[i]] = strings.ToLower(v)
			}
		}
		out = append(out, p)
	}
	if len(out) == 0 {
		return nil, ErrNoPages
	}
	return out, nil
}

// ReadPagesFile reads a pages file from disk.
func ReadPagesFile(path string) ([]PageRef, error) {
	if path == "" {
		return nil, ErrNoPages
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return ReadPages(f)
}

// Done returns the normalised refs of pages that finished in a results
// file (a page_done record), so an interrupted run can resume.
func Done(r io.Reader) (map[string]bool, error) {
	done := map[string]bool{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	for sc.Scan() {
		var rec struct {
			Type string `json:"type"`
			Page string `json:"page"`
		}
		if json.Unmarshal(sc.Bytes(), &rec) == nil && rec.Type == "page_done" {
			done[rec.Page] = true
		}
	}
	return done, sc.Err()
}

// DoneFile is Done for a file; a missing file means nothing is done.
func DoneFile(path string) (map[string]bool, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]bool{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return Done(f)
}

// Remaining drops the pages in done.
func Remaining(pages []PageRef, done map[string]bool) []PageRef {
	var out []PageRef
	for _, p := range pages {
		ref, err := task.NormalizeRef(p.Ref)
		if err == nil && done[ref] {
			continue
		}
		out = append(out, p)
	}
	return out
}

// userinfo matches credentials embedded in URLs (proxy URLs in particular).
var userinfo = regexp.MustCompile(`([A-Za-z][A-Za-z0-9+.\-]*://)[^/@\s"']+@`)

// Redact removes credentials embedded in URLs from s. Transport errors can
// echo the proxy URL, user name and password included.
func Redact(s string) string { return userinfo.ReplaceAllString(s, "${1}***@") }
