package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
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
