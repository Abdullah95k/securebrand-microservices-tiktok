package api

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/emit"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/erasure"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

// listOperations lists TikTok's operations in the registry (ADR 0006 §3):
// each call's variants, method, endpoint and check state.
func (s *Server) listOperations(w http.ResponseWriter, r *http.Request) {
	qs, err := registry.ListFrom(r.Context(), s.ReadDB, ttstore.Registry)
	if err != nil {
		serverError(w, err)
		return
	}
	if qs == nil {
		qs = []registry.Query{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"operations": qs})
}

// putOperation sets a registry entry; crawlers pick it up on their next
// reload.
func (s *Server) putOperation(w http.ResponseWriter, r *http.Request) {
	var q registry.Query
	if err := decode(r, &q); err != nil {
		problem(w, http.StatusBadRequest, "invalid body", err.Error())
		return
	}
	q.Name = chi.URLParam(r, "name")
	if q.DocID == "" || q.FriendlyName == "" || q.Parser == "" || len(q.Variables) == 0 {
		problem(w, http.StatusBadRequest, "invalid operation", "doc_id (the variant label), friendly_name, parser and variables are required")
		return
	}
	if q.ParserVersion == 0 {
		q.ParserVersion = 1
	}
	q.Source = "manual"
	if err := registry.UpsertTo(r.Context(), s.DB, ttstore.Registry, q); err != nil {
		problem(w, http.StatusBadRequest, "invalid operation", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, q)
}

// canaryRuns lists the latest canary runs (M8), newest first.
func (s *Server) canaryRuns(w http.ResponseWriter, r *http.Request) {
	limit := 20
	if v, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && v > 0 && v <= 200 {
		limit = v
	}
	rows, err := s.ReadDB.Query(r.Context(), `SELECT run_id, started_at, finished_at, status, report
		FROM canary_runs ORDER BY started_at DESC LIMIT $1`, limit)
	if err != nil {
		serverError(w, err)
		return
	}
	type run struct {
		ID         int64           `json:"id"`
		StartedAt  time.Time       `json:"started_at"`
		FinishedAt time.Time       `json:"finished_at"`
		Status     string          `json:"status"`
		Report     json.RawMessage `json:"report"`
	}
	runs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (run, error) {
		var x run
		return x, row.Scan(&x.ID, &x.StartedAt, &x.FinishedAt, &x.Status, &x.Report)
	})
	if err != nil {
		serverError(w, err)
		return
	}
	if runs == nil {
		runs = []run{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": runs})
}

// Erasure is the API view of an erasure request. It never shows who was
// erased.
type Erasure struct {
	ID          uuid.UUID       `json:"id"`
	SubjectType string          `json:"subject_type"`
	Status      string          `json:"status"`
	Stats       json.RawMessage `json:"stats,omitempty"`
	Error       *string         `json:"error,omitempty"`
	Attempts    int             `json:"attempts"`
	CreatedAt   time.Time       `json:"created_at"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
}

// insertErasure queues a request; the scheduler's eraser carries it out
// (internal/tt/erasure). input keeps what the client named the subject by
// until it is done.
func (s *Server) insertErasure(ctx context.Context, c *Client, subject string, authorKey []byte, userID int64, input map[string]any) (*Erasure, error) {
	id, _ := uuid.NewV7()
	in, _ := json.Marshal(input)
	e := &Erasure{ID: id, SubjectType: subject, Status: "pending", CreatedAt: time.Now().UTC()}
	_, err := s.DB.Exec(ctx, `INSERT INTO erasure_requests (id, api_client_id, subject_type, author_key, user_id, input, status, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,'pending',$7)`, id, c.ID, subject, authorKey, nilIfZero(userID), in, e.CreatedAt)
	return e, err
}

// erasureReq names the subject one way: an author by author key, user id,
// secUid or username; an account by user id, secUid or username. Names are
// resolved from what tks stores; no TikTok request is made.
type erasureReq struct {
	SubjectType string `json:"subject_type"` // author | account
	AuthorKey   string `json:"author_key,omitempty"`
	// UserID is a string: TikTok's ids do not fit a JavaScript number.
	UserID   string `json:"user_id,omitempty"`
	SecUID   string `json:"sec_uid,omitempty"`
	Username string `json:"username,omitempty"`
}

var errUnknownSubject = errors.New("unknown subject")

// userIDOf resolves a secUid or a username (current or former) to a user
// id: an account first, then a person (a commenter or a mentioned user).
func (s *Server) userIDOf(ctx context.Context, sec, username string) (int64, error) {
	var id int64
	var err error
	switch {
	case sec != "":
		err = s.DB.QueryRow(ctx, `SELECT user_id FROM accounts WHERE sec_uid = $1
			UNION ALL SELECT user_id FROM people WHERE sec_uid = $1 AND user_id IS NOT NULL LIMIT 1`, sec).Scan(&id)
	case username != "":
		u := strings.ToLower(strings.TrimPrefix(strings.TrimSpace(username), "@"))
		err = s.DB.QueryRow(ctx, `SELECT user_id FROM (
				SELECT user_id, last_seen_at FROM account_usernames WHERE lower(username) = $1
				UNION ALL SELECT user_id, last_seen_at FROM people WHERE lower(username) = $1 AND user_id IS NOT NULL
			) x ORDER BY last_seen_at DESC LIMIT 1`, u).Scan(&id)
	default:
		return 0, errUnknownSubject
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, errUnknownSubject
	}
	return id, err
}

// subjectUserID reads the request's user id, or resolves its secUid or
// username.
func (s *Server) subjectUserID(ctx context.Context, req erasureReq) (int64, error) {
	if id := strings.TrimSpace(req.UserID); id != "" {
		v, err := strconv.ParseInt(id, 10, 64)
		if err != nil || v <= 0 || strconv.FormatInt(v, 10) != id {
			return 0, errors.New("user_id is TikTok's numeric user id, as a string")
		}
		return v, nil
	}
	return s.userIDOf(ctx, strings.TrimSpace(req.SecUID), req.Username)
}

// createErasure queues an erasure. An author key alone works for a person
// tks stores pseudonymized; a user id works even for one tks never stored
// (it is suppressed from then on).
func (s *Server) createErasure(w http.ResponseWriter, r *http.Request) {
	var req erasureReq
	if err := decode(r, &req); err != nil {
		problem(w, http.StatusBadRequest, "invalid body", err.Error())
		return
	}
	ctx := r.Context()
	c := clientFrom(ctx)
	named := 0
	for _, v := range []string{req.AuthorKey, req.UserID, req.SecUID, req.Username} {
		if strings.TrimSpace(v) != "" {
			named++
		}
	}
	if named != 1 {
		problem(w, http.StatusBadRequest, "invalid subject", "name the subject one way: author_key, user_id, sec_uid or username")
		return
	}
	input := map[string]any{}
	for k, v := range map[string]string{"user_id": req.UserID, "sec_uid": req.SecUID, "username": req.Username} {
		if v != "" {
			input[k] = v
		}
	}
	var e *Erasure
	var err error
	switch req.SubjectType {
	case erasure.SubjectAuthor:
		var key []byte
		if req.AuthorKey != "" {
			if key, err = hex.DecodeString(strings.TrimSpace(req.AuthorKey)); err != nil || len(key) != 32 {
				problem(w, http.StatusBadRequest, "invalid author_key", "author_key is the 64 hex characters the API shows")
				return
			}
		} else {
			uid, err := s.subjectUserID(ctx, req)
			if errors.Is(err, errUnknownSubject) {
				problem(w, http.StatusBadRequest, "unknown author", "no stored person has this secUid or username; give the user_id")
				return
			}
			if err != nil {
				problem(w, http.StatusBadRequest, "invalid user_id", err.Error())
				return
			}
			key = emit.AuthorKey([]byte(s.Cfg.Privacy.AuthorHMACKey), uid)
		}
		e, err = s.insertErasure(ctx, c, erasure.SubjectAuthor, key, 0, input)
	case erasure.SubjectAccount:
		if req.AuthorKey != "" {
			problem(w, http.StatusBadRequest, "invalid subject", "an account is named by user_id, sec_uid or username")
			return
		}
		uid, err := s.subjectUserID(ctx, req)
		if errors.Is(err, errUnknownSubject) {
			problem(w, http.StatusBadRequest, "unknown account", "no stored account has this secUid or username; give the user_id")
			return
		}
		if err != nil {
			problem(w, http.StatusBadRequest, "invalid user_id", err.Error())
			return
		}
		e, err = s.insertErasure(ctx, c, erasure.SubjectAccount, nil, uid, input)
		if err != nil {
			serverError(w, err)
			return
		}
	default:
		problem(w, http.StatusBadRequest, "invalid subject_type", "subject_type must be author or account")
		return
	}
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, e)
}

func (s *Server) getErasure(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid id", err.Error())
		return
	}
	var e Erasure
	err = s.DB.QueryRow(r.Context(), `SELECT id, subject_type, status, stats, error, attempts, created_at, completed_at
		FROM erasure_requests WHERE id = $1`, id).Scan(&e.ID, &e.SubjectType, &e.Status, &e.Stats, &e.Error, &e.Attempts,
		&e.CreatedAt, &e.CompletedAt)
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, e)
}
