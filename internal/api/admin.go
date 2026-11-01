package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/gdpr"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
)

func (s *Server) listQueries(w http.ResponseWriter, r *http.Request) {
	qs, err := registry.List(r.Context(), s.ReadDB)
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"queries": qs})
}

// registrySyncRuns lists the latest fbs-registry-sync runs, newest first.
func (s *Server) registrySyncRuns(w http.ResponseWriter, r *http.Request) {
	limit := 20
	if v, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && v > 0 && v <= 200 {
		limit = v
	}
	rows, err := s.ReadDB.Query(r.Context(), `SELECT run_id, started_at, finished_at, status, report
		FROM registry_sync_runs ORDER BY started_at DESC LIMIT $1`, limit)
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
		err := row.Scan(&x.ID, &x.StartedAt, &x.FinishedAt, &x.Status, &x.Report)
		return x, err
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

// putQuery sets a registry entry; crawlers pick it up on their next reload.
func (s *Server) putQuery(w http.ResponseWriter, r *http.Request) {
	var q registry.Query
	if err := decode(r, &q); err != nil {
		problem(w, http.StatusBadRequest, "invalid body", err.Error())
		return
	}
	q.Name = chi.URLParam(r, "name")
	if q.DocID == "" || q.FriendlyName == "" || q.Parser == "" || len(q.Variables) == 0 {
		problem(w, http.StatusBadRequest, "invalid query", "doc_id, friendly_name, parser and variables are required")
		return
	}
	if q.ParserVersion == 0 {
		q.ParserVersion = 1
	}
	q.Source = "manual"
	if err := registry.Upsert(r.Context(), s.DB, q); err != nil {
		problem(w, http.StatusBadRequest, "invalid query", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, q)
}

// Erasure is the API view of an erasure request.
type Erasure struct {
	ID          uuid.UUID       `json:"id"`
	SubjectType string          `json:"subject_type"`
	Status      string          `json:"status"`
	Stats       json.RawMessage `json:"stats,omitempty"`
	Error       *string         `json:"error,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
}

func (s *Server) insertErasure(ctx context.Context, c *Client, subject string, authorKey []byte, pageID int64, input map[string]any) (*Erasure, error) {
	id, _ := uuid.NewV7()
	in, _ := json.Marshal(input)
	var pid *int64
	if pageID != 0 {
		pid = &pageID
	}
	e := &Erasure{ID: id, SubjectType: subject, Status: "pending", CreatedAt: time.Now().UTC()}
	_, err := s.DB.Exec(ctx, `INSERT INTO erasure_requests (id, api_client_id, subject_type, author_key, page_id, input, status, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,'pending',$7)`, id, c.ID, subject, authorKey, pid, in, e.CreatedAt)
	return e, err
}

type erasureReq struct {
	SubjectType string `json:"subject_type"` // author | page
	AuthorID    string `json:"author_id,omitempty"`
	ProfileURL  string `json:"profile_url,omitempty"`
	PageID      int64  `json:"page_id,omitempty"`
}

// createErasure queues a GDPR erasure. Authors are identified by Facebook id
// or profile URL and resolved locally (no Facebook request is made).
func (s *Server) createErasure(w http.ResponseWriter, r *http.Request) {
	var req erasureReq
	if err := decode(r, &req); err != nil {
		problem(w, http.StatusBadRequest, "invalid body", err.Error())
		return
	}
	c := clientFrom(r.Context())
	switch req.SubjectType {
	case "author":
		// The key is the HMAC of the id exactly as the crawlers see it, a
		// number: anything else (a profile link, stray spaces) would match
		// nothing, and the request would end done with nothing erased.
		id, profile := strings.TrimSpace(req.AuthorID), strings.TrimSpace(req.ProfileURL)
		if v, err := strconv.ParseUint(id, 10, 63); id != "" && (err != nil || strconv.FormatUint(v, 10) != id) {
			// Not a number, or not as Facebook writes it (leading zeros).
			problem(w, http.StatusBadRequest, "invalid author_id", "author_id is the numeric Facebook id; for a profile link, use profile_url")
			return
		}
		req.ProfileURL = profile
		if id == "" && req.ProfileURL != "" {
			_ = s.DB.QueryRow(r.Context(), `SELECT author_id FROM comment_authors WHERE profile_url = $1 AND author_id IS NOT NULL LIMIT 1`,
				req.ProfileURL).Scan(&id)
		}
		var key []byte
		if id != "" {
			key = gdpr.AuthorKey([]byte(s.Cfg.Privacy.AuthorHMACKey), id)
		} else if req.ProfileURL != "" {
			_ = s.DB.QueryRow(r.Context(), `SELECT author_key FROM comment_authors WHERE profile_url = $1 LIMIT 1`, req.ProfileURL).Scan(&key)
		}
		if len(key) == 0 {
			problem(w, http.StatusBadRequest, "unknown author", "provide author_id, or a profile_url we have stored")
			return
		}
		// The Facebook id (resolved from the profile URL if need be) lets the
		// erasure find the author's mentions; it is dropped once done.
		e, err := s.insertErasure(r.Context(), c, "author", key, 0, map[string]any{"author_id": id, "profile_url": req.ProfileURL})
		if err != nil {
			serverError(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, e)
	case "page":
		if req.PageID == 0 {
			problem(w, http.StatusBadRequest, "invalid page", "page_id is required")
			return
		}
		e, err := s.insertErasure(r.Context(), c, "page", nil, req.PageID, map[string]any{"page_id": req.PageID})
		if err != nil {
			serverError(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, e)
	default:
		problem(w, http.StatusBadRequest, "invalid subject_type", "subject_type must be author or page")
	}
}

func (s *Server) getErasure(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid id", err.Error())
		return
	}
	var e Erasure
	err = s.DB.QueryRow(r.Context(), `SELECT id, subject_type, status, stats, error, created_at, completed_at
		FROM erasure_requests WHERE id = $1`, id).Scan(&e.ID, &e.SubjectType, &e.Status, &e.Stats, &e.Error, &e.CreatedAt, &e.CompletedAt)
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, e)
}
