// Package schemareg registers protobuf schemas with a Confluent-compatible
// Schema Registry (Redpanda's included) and gates changes on its
// compatibility check. fbs keeps plain protobuf on the wire (the message
// type is in the igs-type record header); the registry documents the
// contract for consumers and rejects changes that would break them.
package schemareg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Compatibility levels.
const (
	BackwardTransitive = "BACKWARD_TRANSITIVE"
)

// Reference is an import resolved to another subject.
type Reference struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

// Schema is a protobuf schema with its references.
type Schema struct {
	Schema     string      `json:"schema"`
	SchemaType string      `json:"schemaType"`
	References []Reference `json:"references,omitempty"`
}

// Client talks to the registry's REST API.
type Client struct {
	URL      string
	Username string
	Password string
	HTTP     *http.Client
}

// Error is a registry error response.
type Error struct {
	Status  int
	Code    int    `json:"error_code"`
	Message string `json:"message"`
}

func (e *Error) Error() string {
	return fmt.Sprintf("schema registry: %d %d: %s", e.Status, e.Code, e.Message)
}

// IsNotFound reports a missing subject or version.
func IsNotFound(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.Status == http.StatusNotFound
}

// ErrIncompatible is returned by Apply when a schema fails its check.
var ErrIncompatible = errors.New("schemareg: incompatible schema")

// IsIncompatible reports a schema the subject's compatibility level rejects.
func IsIncompatible(err error) bool {
	var e *Error
	return errors.Is(err, ErrIncompatible) || (errors.As(err, &e) && e.Status == http.StatusConflict)
}

// do sends one request. Every call this client makes is idempotent, so a
// request on a connection the registry closed meanwhile (Redpanda drops
// kept-alive connections after some error responses, and idle ones) is
// sent once more. The Idempotency-Key header (see send) lets net/http
// replay a POST on a reused connection itself; the retry here covers a
// connection dropped mid-request and the transport's own error.
func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	var body []byte
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = b
	}
	err := c.send(ctx, method, path, body, out)
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET) ||
		(err != nil && strings.Contains(err.Error(), "server closed idle connection")) {
		err = c.send(ctx, method, path, body, out)
	}
	return err
}

func (c *Client) send(ctx context.Context, method, path string, body []byte, out any) error {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.URL, "/")+path, r)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json, application/json")
	// Marks the request replayable: net/http then retries a POST, like a
	// GET, when the registry closed the kept-alive connection under it.
	req.Header.Set("Idempotency-Key", strconv.FormatInt(time.Now().UnixNano(), 36))
	if c.Username != "" {
		req.SetBasicAuth(c.Username, c.Password)
	}
	hc := c.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: 30 * time.Second}
	}
	resp, err := hc.Do(req)
	if err != nil {
		return fmt.Errorf("schema registry: %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode >= 300 {
		e := &Error{Status: resp.StatusCode}
		if json.Unmarshal(b, e) != nil || e.Message == "" {
			e.Message = strings.TrimSpace(string(b))
		}
		return e
	}
	if out != nil {
		return json.Unmarshal(b, out)
	}
	return nil
}

func subjectPath(subject string) string { return "/subjects/" + url.PathEscape(subject) }

// SetCompatibility sets the subject's compatibility level.
func (c *Client) SetCompatibility(ctx context.Context, subject, level string) error {
	return c.do(ctx, http.MethodPut, "/config/"+url.PathEscape(subject), map[string]string{"compatibility": level}, nil)
}

// Compatibility returns the subject's own level ("" when it has none).
func (c *Client) Compatibility(ctx context.Context, subject string) (string, error) {
	var out struct {
		Level string `json:"compatibilityLevel"`
	}
	err := c.do(ctx, http.MethodGet, "/config/"+url.PathEscape(subject), nil, &out)
	if IsNotFound(err) {
		return "", nil
	}
	return out.Level, err
}

// Check reports whether s is compatible with the subject's latest version
// (a subject that does not exist yet accepts anything).
func (c *Client) Check(ctx context.Context, subject string, s Schema) (bool, []string, error) {
	var out struct {
		Compatible bool     `json:"is_compatible"`
		Messages   []string `json:"messages"`
	}
	err := c.do(ctx, http.MethodPost, "/compatibility"+subjectPath(subject)+"/versions/latest?verbose=true", s, &out)
	if IsNotFound(err) {
		return true, nil, nil
	}
	if err != nil {
		return false, nil, err
	}
	return out.Compatible, out.Messages, nil
}

// Register adds s to subject (idempotent: an identical schema returns its
// existing version) and returns its id and version.
func (c *Client) Register(ctx context.Context, subject string, s Schema) (id, version int, err error) {
	var reg struct {
		ID int `json:"id"`
	}
	if err := c.do(ctx, http.MethodPost, subjectPath(subject)+"/versions", s, &reg); err != nil {
		return 0, 0, err
	}
	v, err := c.Lookup(ctx, subject, s)
	return reg.ID, v, err
}

// Subjects lists the registered subjects.
func (c *Client) Subjects(ctx context.Context) ([]string, error) {
	var out []string
	err := c.do(ctx, http.MethodGet, "/subjects", nil, &out)
	return out, err
}

// Lookup returns the version under which subject holds exactly s, 0 when
// it does not.
func (c *Client) Lookup(ctx context.Context, subject string, s Schema) (int, error) {
	var out struct {
		Version int `json:"version"`
	}
	err := c.do(ctx, http.MethodPost, subjectPath(subject), s, &out)
	if IsNotFound(err) {
		return 0, nil
	}
	return out.Version, err
}

// DeleteSubject deletes subject and all its versions, permanently.
func (c *Client) DeleteSubject(ctx context.Context, subject string) error {
	if err := c.do(ctx, http.MethodDelete, subjectPath(subject), nil, nil); err != nil && !IsNotFound(err) {
		return err
	}
	err := c.do(ctx, http.MethodDelete, subjectPath(subject)+"?permanent=true", nil, nil)
	if IsNotFound(err) {
		return nil
	}
	return err
}

// Result describes one subject applied (or checked).
type Result struct {
	Subject string `json:"subject"`
	File    string `json:"file"`
	ID      int    `json:"id,omitempty"`
	// Version is the registered version (0 when a check found a change).
	Version    int      `json:"version,omitempty"`
	Unchanged  bool     `json:"unchanged,omitempty"`
	Compatible bool     `json:"compatible"`
	Messages   []string `json:"messages,omitempty"`
}

var importRe = regexp.MustCompile(`(?m)^\s*import\s+(?:public\s+|weak\s+)?"([^"]+)"\s*;`)

// imports returns the non-well-known imports of a proto file.
func imports(src string) []string {
	var out []string
	for _, m := range importRe.FindAllStringSubmatch(src, -1) {
		if !strings.HasPrefix(m[1], "google/protobuf/") {
			out = append(out, m[1])
		}
	}
	return out
}

// node is one subject to apply.
type node struct {
	file, subject string
	source        string
	imports       []string
	isImport      bool
}

// plan orders the subjects dependencies first: every file a topic's file
// imports (transitively) as <prefix><import path>, then <prefix><topic>-value.
func plan(files fs.FS, prefix string, topicFiles map[string]string) ([]node, error) {
	var out []node
	seen := map[string]bool{}
	var visit func(file, subject string, isImport bool, stack []string) error
	visit = func(file, subject string, isImport bool, stack []string) error {
		for _, f := range stack {
			if f == file {
				return fmt.Errorf("schemareg: import cycle through %s", file)
			}
		}
		if seen[subject] {
			return nil
		}
		b, err := fs.ReadFile(files, file)
		if err != nil {
			return fmt.Errorf("schemareg: %w", err)
		}
		n := node{file: file, subject: subject, source: string(b), imports: imports(string(b)), isImport: isImport}
		for _, imp := range n.imports {
			if err := visit(imp, prefix+imp, true, append(stack, file)); err != nil {
				return err
			}
		}
		seen[subject] = true
		out = append(out, n)
		return nil
	}
	topics := make([]string, 0, len(topicFiles))
	for t := range topicFiles {
		topics = append(topics, t)
	}
	sort.Strings(topics)
	for _, t := range topics {
		if err := visit(topicFiles[t], prefix+t+"-value", false, nil); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (n node) schema(refs map[string]Reference) Schema {
	s := Schema{Schema: n.source, SchemaType: "PROTOBUF"}
	for _, imp := range n.imports {
		s.References = append(s.References, refs[imp])
	}
	return s
}

// Apply registers, for each topic in topicFiles, the proto file holding its
// value message as subject <prefix><topic>-value, and every file it imports
// as subject <prefix><import path>, which the topic subjects reference.
// Well-known google/protobuf imports are left to the registry.
//
// It first checks every changed schema against its subject's latest
// version, and registers nothing unless all pass. A changed import is
// staged under a temporary subject during the check, so the files
// importing it are checked against its new content; the temporary
// subjects are deleted afterwards. With dryRun it stops after the check.
// A subject without a compatibility level of its own gets
// BACKWARD_TRANSITIVE; a level an operator set is kept. (The registry
// enforces the subject's level, transitive included, on registration.)
func Apply(ctx context.Context, c *Client, files fs.FS, prefix string, topicFiles map[string]string, dryRun bool) ([]Result, error) {
	nodes, err := plan(files, prefix, topicFiles)
	if err != nil {
		return nil, err
	}
	results := make([]Result, len(nodes))
	staged := map[string]Reference{}
	var temps []string
	defer func() {
		cctx := context.WithoutCancel(ctx)
		for i := len(temps) - 1; i >= 0; i-- {
			_ = c.DeleteSubject(cctx, temps[i])
		}
	}()
	tmpPrefix := fmt.Sprintf("%sigs-check-%d.", prefix, time.Now().UnixNano())
	var problems []string
	for i, n := range nodes {
		s := n.schema(staged)
		r := Result{Subject: n.subject, File: n.file}
		v, err := c.Lookup(ctx, n.subject, s)
		if err != nil {
			return nil, err
		}
		if v > 0 {
			r.Version, r.Unchanged, r.Compatible = v, true, true
			results[i] = r
			if n.isImport {
				staged[n.file] = Reference{Name: n.file, Subject: n.subject, Version: v}
			}
			continue
		}
		ok, msgs, err := c.Check(ctx, n.subject, s)
		if err != nil {
			return nil, fmt.Errorf("schemareg: check %s: %w", n.subject, err)
		}
		r.Compatible, r.Messages = ok, msgs
		results[i] = r
		if !ok {
			problems = append(problems, fmt.Sprintf("%s (%s): %s", n.subject, n.file, strings.Join(msgs, "; ")))
		}
		if n.isImport {
			tmp := tmpPrefix + n.file
			temps = append(temps, tmp)
			_, tv, err := c.Register(ctx, tmp, s)
			if err != nil {
				return nil, fmt.Errorf("schemareg: stage %s: %w", n.file, err)
			}
			staged[n.file] = Reference{Name: n.file, Subject: tmp, Version: tv}
		}
	}
	if len(problems) > 0 {
		return results, fmt.Errorf("%w: %s", ErrIncompatible, strings.Join(problems, " | "))
	}
	if dryRun {
		return results, nil
	}
	registered := map[string]Reference{}
	for i, n := range nodes {
		s := n.schema(registered)
		lvl, err := c.Compatibility(ctx, n.subject)
		if err != nil {
			return results, err
		}
		if lvl == "" {
			if err := c.SetCompatibility(ctx, n.subject, BackwardTransitive); err != nil {
				return results, err
			}
		}
		id, v, err := c.Register(ctx, n.subject, s)
		if err != nil {
			return results, fmt.Errorf("schemareg: register %s: %w", n.subject, err)
		}
		results[i].ID, results[i].Version, results[i].Compatible = id, v, true
		if n.isImport {
			registered[n.file] = Reference{Name: n.file, Subject: n.subject, Version: v}
		}
	}
	return results, nil
}
