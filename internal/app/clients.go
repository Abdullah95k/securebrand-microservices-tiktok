package app

import (
	"context"

	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/connect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
)

// KafkaClient is a component's Kafka client configuration: id is appended
// to kafka.client_id, and kafka.tls and kafka.sasl secure the connections
// (connect.Kafka).
func KafkaClient(cfg *config.Config, id string) (kafka.ClientConfig, error) {
	return connect.Kafka(cfg, id)
}

// OpenRedis connects to Valkey/Redis (redis.tls for TLS; connect.Redis).
func OpenRedis(ctx context.Context, cfg *config.Config) (*redis.Client, error) {
	return connect.Redis(ctx, cfg)
}
