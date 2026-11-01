package app

import (
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	sharedpersist "github.com/abdullah95k/securebrand-microservices-tiktok/internal/persist"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/connect"
	ttpersist "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/persist"
)

// NewPersister builds the persister: tt.*.v1 into schema tt (db: a pool
// from ttstore.Open), in batches of up to 2,000 records.
func NewPersister(cfg *config.Config, db *pgxpool.Pool, linger time.Duration) (*sharedpersist.Consumer, error) {
	kcfg, err := connect.Kafka(cfg, "-persister")
	if err != nil {
		return nil, err
	}
	w := &ttpersist.Writer{DB: db, CommentsRetention: time.Duration(cfg.TT.Retention.CommentsDays) * 24 * time.Hour}
	return ttpersist.NewConsumer(kcfg, w, 2000, linger)
}
