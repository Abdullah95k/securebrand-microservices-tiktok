package app

import (
	"time"

	"github.com/abdullah95k/securebrand-contracts/conformance"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	sharedpersist "github.com/abdullah95k/securebrand-microservices-tiktok/internal/persist"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/connect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/content"
	ttpersist "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/persist"
)

// NewPersister builds the persister: tt.*.v1 into schema tt (db: a pool
// from ttstore.Open), in batches of up to 2,000 records. now is the clock
// of a new post's stored_at and delay (nil: time.Now). It records discovery
// part B's sightings for the sources tt.discovery switches on, and publishes
// the shared contract's content.* events when tt.content is on.
func NewPersister(cfg *config.Config, db *pgxpool.Pool, linger time.Duration, now func() time.Time) (*sharedpersist.Consumer, error) {
	kcfg, err := connect.Kafka(cfg, "-persister")
	if err != nil {
		return nil, err
	}
	w := &ttpersist.Writer{DB: db, CommentsRetention: time.Duration(cfg.TT.Retention.CommentsDays) * 24 * time.Hour, Now: now,
		Sightings: cfg.TT.Discovery.SourceOn}
	var ce ttpersist.ContentEmitter
	if cfg.TT.Content.Enabled {
		p, err := kafka.NewProducer(kcfg)
		if err != nil {
			return nil, err
		}
		ce = &content.Emitter{DB: db, Producer: p, Options: conformance.Options{Production: cfg.TT.Content.Production}}
	}
	return ttpersist.NewConsumer(kcfg, w, ce, 2000, linger)
}
