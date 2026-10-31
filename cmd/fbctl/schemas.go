package main

import (
	"context"
	"errors"

	protosrc "github.com/abdullah95k/securebrand-microservices-igs/api/proto"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/schemareg"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/topics"
)

// schemasApply registers the public topics' schemas, or with check only
// verifies they are compatible with what is registered.
func schemasApply(ctx context.Context, cfg *config.Config, check bool) error {
	sr := cfg.Kafka.SchemaRegistry
	if sr.URL == "" {
		return errors.New("kafka.schema_registry.url is not set")
	}
	c := &schemareg.Client{URL: sr.URL, Username: sr.Username, Password: sr.Password}
	res, err := schemareg.Apply(ctx, c, protosrc.FS, cfg.Kafka.TopicPrefix, topics.ValueSchemas, check)
	printJSON(res)
	return err
}
