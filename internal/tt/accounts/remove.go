package accounts

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Removed is what Remove did.
type Removed struct {
	// Stopped are the accounts whose checks ended (no client is left on
	// them).
	Stopped int `json:"stopped"`
}

// Remove takes accounts out of a client's monitoring for good (the
// owner's choices of 2026-11-06, M18): the client's block keeps discovery
// from adding them back, as DELETE /v1/accounts and the review page's
// Remove do; the client's registrations and labels go, and so do
// discovery's (discoveryClient names it: tt.discovery.client), so an
// account discovery added stops too; the schedule stops once no client is
// left on it; a found account's candidate becomes blocked. The data stays
// (ADR 0007 §14). refs are schedule refs.
func Remove(ctx context.Context, tx pgx.Tx, client uuid.UUID, discoveryClient string, refs []string, now time.Time) (Removed, error) {
	var out Removed
	if len(refs) == 0 {
		return out, nil
	}
	if _, err := tx.Exec(ctx, `INSERT INTO account_blocks (api_client_id, account_ref, user_id, created_at)
		SELECT $1, r, s.user_id, $3 FROM unnest($2::text[]) r LEFT JOIN account_schedule s ON s.account_ref = r
		ON CONFLICT (api_client_id, account_ref) DO UPDATE SET user_id = coalesce(EXCLUDED.user_id, account_blocks.user_id)`,
		client, refs, now); err != nil {
		return out, err
	}
	// Discovery's own client: the one tks made, which no key signs in as.
	clients := []uuid.UUID{client}
	if discoveryClient != "" {
		var dc uuid.UUID
		err := tx.QueryRow(ctx, `SELECT id FROM api_clients WHERE name = $1 AND key_hash = '\x00'::bytea`, discoveryClient).Scan(&dc)
		if err == nil {
			clients = append(clients, dc)
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return out, err
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM account_registrations WHERE account_ref = ANY($1) AND api_client_id = ANY($2)`, refs, clients); err != nil {
		return out, err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM account_labels WHERE account_ref = ANY($1) AND api_client_id = ANY($2)`, refs, clients); err != nil {
		return out, err
	}
	if _, err := tx.Exec(ctx, `UPDATE discovery_candidates SET state = 'blocked', updated_at = now()
		WHERE schedule_ref = ANY($1) AND state <> 'blocked'`, refs); err != nil {
		return out, err
	}
	tag, err := tx.Exec(ctx, `UPDATE account_schedule s SET enabled = false WHERE s.account_ref = ANY($1) AND s.enabled
		AND NOT EXISTS (SELECT 1 FROM account_registrations g WHERE g.account_ref = s.account_ref)`, refs)
	if err != nil {
		return out, err
	}
	out.Stopped = int(tag.RowsAffected())
	return out, nil
}
