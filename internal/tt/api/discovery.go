package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/ratelimit"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/accounts"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/discovery"
)

// sourceView is a way an account came to tks (account_sources).
type sourceView struct {
	How      string          `json:"how"`
	Via      string          `json:"via,omitempty"`
	FirstAt  time.Time       `json:"first_at"`
	LastAt   time.Time       `json:"last_at"`
	Seen     int64           `json:"seen"`
	Evidence json.RawMessage `json:"evidence,omitempty"`
	Score    *float64        `json:"score,omitempty"`
}

// sources lists where an account came from: the rows of its schedule
// reference or of its user id, oldest first.
func (s *Server) sources(ctx context.Context, ref string, userID int64) ([]sourceView, error) {
	rows, err := s.ReadDB.Query(ctx, `SELECT how, via, first_at, last_at, seen, evidence, score FROM account_sources
		WHERE account_ref = $1 OR ($2::bigint IS NOT NULL AND user_id = $2) ORDER BY first_at, how, via`, ref, nilIfZero(userID))
	if err != nil {
		return nil, err
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (sourceView, error) {
		var v sourceView
		return v, r.Scan(&v.How, &v.Via, &v.FirstAt, &v.LastAt, &v.Seen, &v.Evidence, &v.Score)
	})
	if out == nil {
		out = []sourceView{}
	}
	return out, err
}

// labels lists a client's labels on a registration.
func (s *Server) labels(ctx context.Context, client uuid.UUID, ref string) ([]string, error) {
	rows, err := s.ReadDB.Query(ctx, `SELECT label FROM account_labels WHERE api_client_id = $1 AND account_ref = $2 ORDER BY label`, client, ref)
	if err != nil {
		return nil, err
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if out == nil {
		out = []string{}
	}
	return out, err
}

type importReq struct {
	// Lines are accounts, one each; Text is a whole file (one account a
	// line, or a CSV with an account column). One of the two, at most
	// accounts.MaxAPILines lines a call.
	Lines          []string `json:"lines,omitempty"`
	Text           string   `json:"text,omitempty"`
	Labels         []string `json:"labels,omitempty"`
	DryRun         bool     `json:"dry_run,omitempty"`
	DisableMissing bool     `json:"disable_missing,omitempty"`
	Force          bool     `json:"force,omitempty"`
}

// importAccounts registers a list of accounts for the caller
// (docs/tiktok/discovery-plan.md §6): every line is cleaned, checked and
// registered or reported, and the batch is kept for its later report.
func (s *Server) importAccounts(w http.ResponseWriter, r *http.Request) {
	var req importReq
	if err := decode(r, &req); err != nil {
		problem(w, http.StatusBadRequest, "invalid body", err.Error())
		return
	}
	if (len(req.Lines) == 0) == (req.Text == "") {
		problem(w, http.StatusBadRequest, "invalid body", "give lines or text, not both")
		return
	}
	text := req.Text
	if len(req.Lines) > 0 {
		text = strings.Join(req.Lines, "\n")
	}
	lines, err := accounts.Parse(strings.NewReader(text))
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid list", err.Error())
		return
	}
	if len(lines) > accounts.MaxAPILines {
		problem(w, http.StatusRequestEntityTooLarge, "too many lines",
			"at most 1000 accounts a call: send the list in chunks (tksctl accounts import does)")
		return
	}
	if req.DisableMissing && !req.DryRun && len(lines) > 0 {
		// --disable-missing compares with the whole list, which one call of
		// 1000 lines may not hold: only tksctl runs it on a whole file.
		problem(w, http.StatusBadRequest, "disable_missing", "disable_missing needs the whole list: use tksctl accounts import --disable-missing")
		return
	}
	for _, l := range req.Labels {
		if err := accounts.CheckLabel(l); err != nil {
			problem(w, http.StatusBadRequest, "invalid label", err.Error())
			return
		}
	}
	ctx := r.Context()
	st := s.budget(ctx)
	rep, err := accounts.Import(ctx, s.DB, s.Cfg, lines, accounts.ImportOptions{ClientID: clientFrom(ctx).ID, Labels: req.Labels,
		DryRun: req.DryRun, DisableMissing: req.DisableMissing, Force: req.Force, Budget: st, Now: s.now()})
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

// importReport lists an import's accounts with what tks found since:
// ?problems=true keeps those that turned out private, inactive or gone.
func (s *Server) importReport(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "batch"))
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid batch", "batch must be an import's batch id")
		return
	}
	c := clientFrom(r.Context())
	out, err := accounts.BatchReport(r.Context(), s.ReadDB, c.ID, c.Has(ScopeAdmin), id, r.URL.Query().Get("problems") == "true", s.now())
	if errors.Is(err, pgx.ErrNoRows) {
		problem(w, http.StatusNotFound, "unknown batch", "no import of this client has this batch id")
		return
	}
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"batch": id, "accounts": out})
}

// admission shows the admission queue, its rate and estimated finish, and
// what draining it within a day would take.
func (s *Server) admission(w http.ResponseWriter, r *http.Request) {
	q, err := accounts.QueueStatus(r.Context(), s.ReadDB, s.Cfg, s.budget(r.Context()), s.now())
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, q)
}

// budget is the budget controller's last state, nil when tks-api has no
// Valkey or no request budget is set.
func (s *Server) budget(ctx context.Context) *ratelimit.BudgetState {
	if s.Redis == nil || !s.Cfg.Scheduler.Budget.Enabled || s.Cfg.Scheduler.Budget.RequestsPerDay <= 0 {
		return nil
	}
	st, err := ratelimit.LoadBudgetState(ctx, s.Redis, s.Cfg.Kafka.TopicPrefix)
	if err != nil {
		return nil
	}
	return &st
}

// listKeywords lists the discovery keywords (?language=, ?group=,
// ?enabled=, cursor-paged by id with ?after=).
func (s *Server) listKeywords(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := discovery.KeywordFilter{Language: q.Get("language"), Group: q.Get("group"), Limit: limitParam(r)}
	if v := q.Get("enabled"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			problem(w, http.StatusBadRequest, "invalid enabled", "enabled must be true or false")
			return
		}
		f.Enabled = &b
	}
	if v := q.Get("after"); v != "" {
		id, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			problem(w, http.StatusBadRequest, "invalid after", "after must be a keyword id")
			return
		}
		f.AfterID = id
	}
	out, err := discovery.ListKeywords(r.Context(), s.ReadDB, f)
	if err != nil {
		serverError(w, err)
		return
	}
	if out == nil {
		out = []discovery.Keyword{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"keywords": out})
}

type keywordReq struct {
	Term     string `json:"term"`
	Language string `json:"language"`
	Group    string `json:"group,omitempty"`
	Enabled  *bool  `json:"enabled,omitempty"`
}

// addKeyword adds a keyword; a spelling that folds to one already listed
// in the language returns that one (200) unchanged.
func (s *Server) addKeyword(w http.ResponseWriter, r *http.Request) {
	var req keywordReq
	if err := decode(r, &req); err != nil {
		problem(w, http.StatusBadRequest, "invalid body", err.Error())
		return
	}
	k, created, err := discovery.AddKeyword(r.Context(), s.DB, req.Term, req.Language, req.Group, req.Enabled == nil || *req.Enabled)
	if errors.Is(err, discovery.ErrInvalid) {
		problem(w, http.StatusBadRequest, "invalid keyword", err.Error())
		return
	}
	if err != nil {
		serverError(w, err)
		return
	}
	code := http.StatusOK
	if created {
		code = http.StatusCreated
	}
	writeJSON(w, code, map[string]any{"keyword": k, "created": created})
}

type keywordPatch struct {
	Term    *string `json:"term,omitempty"`
	Group   *string `json:"group,omitempty"`
	Enabled *bool   `json:"enabled,omitempty"`
}

// updateKeyword turns a keyword on or off, or corrects it.
func (s *Server) updateKeyword(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid keyword id", "id must be a number")
		return
	}
	var req keywordPatch
	if err := decode(r, &req); err != nil {
		problem(w, http.StatusBadRequest, "invalid body", err.Error())
		return
	}
	k, err := discovery.UpdateKeyword(r.Context(), s.DB, id, discovery.KeywordUpdate{Term: req.Term, Group: req.Group, Enabled: req.Enabled})
	switch {
	case errors.Is(err, discovery.ErrNotFound):
		problem(w, http.StatusNotFound, "unknown keyword", err.Error())
	case errors.Is(err, discovery.ErrInvalid):
		problem(w, http.StatusBadRequest, "invalid keyword", err.Error())
	case errors.Is(err, discovery.ErrDuplicate):
		problem(w, http.StatusConflict, "duplicate keyword", err.Error())
	case err != nil:
		serverError(w, err)
	default:
		writeJSON(w, http.StatusOK, k)
	}
}
