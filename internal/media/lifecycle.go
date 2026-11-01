package media

import "github.com/minio/minio-go/v7/pkg/lifecycle"

// lifecycleConfig expires the objects under prefix ("" for the whole
// bucket) after days.
func lifecycleConfig(id, prefix string, days int) *lifecycle.Configuration {
	return &lifecycle.Configuration{Rules: []lifecycle.Rule{{
		ID:         id,
		Status:     "Enabled",
		RuleFilter: lifecycle.Filter{Prefix: prefix},
		Expiration: lifecycle.Expiration{Days: lifecycle.ExpirationDays(days)},
	}}}
}

// rawLifecycleConfig covers the whole raw bucket.
func rawLifecycleConfig(days int) *lifecycle.Configuration {
	return lifecycleConfig("tks-raw-retention", "", days)
}
