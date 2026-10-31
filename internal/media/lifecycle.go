package media

import "github.com/minio/minio-go/v7/pkg/lifecycle"

func lifecycleConfig(days int) *lifecycle.Configuration {
	return &lifecycle.Configuration{Rules: []lifecycle.Rule{{
		ID:         "igs-media-ttl",
		Status:     "Enabled",
		RuleFilter: lifecycle.Filter{Prefix: "media/"},
		Expiration: lifecycle.Expiration{Days: lifecycle.ExpirationDays(days)},
	}}}
}
