// Package metrics holds the Prometheus registry and the service metrics
// shared across binaries. Metric names are prefixed with igs_, never fbs_:
// fbs's alert rules select by metric name alone, so an igs pod exporting
// fbs_ series would feed fbs's alerts (ADR 0004 §5).
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Registry is the process registry exposed on /metrics.
var Registry = prometheus.NewRegistry()

func init() {
	Registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
}

func counterVec(name, help string, labels ...string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "igs", Name: name, Help: help}, labels)
	Registry.MustRegister(c)
	return c
}

func histVec(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "igs", Name: name, Help: help, Buckets: buckets}, labels)
	Registry.MustRegister(h)
	return h
}

func gauge(name, help string) prometheus.Gauge {
	g := prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "igs", Name: name, Help: help})
	Registry.MustRegister(g)
	return g
}

func gaugeVec(name, help string, labels ...string) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "igs", Name: name, Help: help}, labels)
	Registry.MustRegister(g)
	return g
}

var (
	// Requests to the scraped platform.
	FBRequests      = counterVec("ig_requests_total", "Platform requests by query and response class.", "query", "class", "tier")
	FBRequestSecs   = histVec("ig_request_seconds", "Platform request latency.", prometheus.ExponentialBuckets(0.05, 2, 10), "query", "tier")
	FBResponseBytes = histVec("ig_response_bytes", "Decoded platform response size.", prometheus.ExponentialBuckets(1024, 2, 12), "query")
	ProxyBytes      = counterVec("proxy_bytes_total", "Wire bytes through each proxy tier.", "tier", "dir")

	// Parsing.
	ParseTotal        = counterVec("parse_total", "Parser runs by outcome.", "query", "parser_version", "outcome")
	ParseMissingField = counterVec("parse_missing_field_total", "Optional or required fields missing from responses.", "query", "field")
	ParseShadow       = counterVec("parse_shadow_total", "Responses a candidate (shadow) parser also parsed, by outcome (same, differs, drift, error).", "query", "outcome")

	// Identities.
	IdentityActive  = gaugeVec("identity_active", "Live identities in this pod.", "tier")
	IdentityRetired = counterVec("identity_retired_total", "Retired identities by reason.", "tier", "reason")
	Bootstraps      = counterVec("bootstrap_total", "Identity bootstraps.", "method", "outcome")

	// Tasks.
	Tasks       = counterVec("tasks_total", "Crawl tasks by stage, lane and outcome.", "stage", "lane", "outcome")
	TaskSeconds = histVec("task_seconds", "Crawl task duration.", prometheus.ExponentialBuckets(0.1, 2, 12), "stage")
	TaskYields  = counterVec("task_yields_total", "Tasks that yielded a continuation.", "stage")
	DLQ         = counterVec("dlq_total", "Tasks sent to the dead letter queue.", "stage", "reason")
	Retries     = counterVec("retries_total", "Tasks sent to a retry topic.", "stage", "tier")

	// Coverage.
	CommentCompleteness = histVec("comment_completeness_ratio", "Collected / reported comments per post at job completion.", []float64{0.1, 0.25, 0.5, 0.7, 0.8, 0.9, 0.95, 0.99, 1.0, 1.01}, "lane")
	TimelineTruncated   = counterVec("timeline_truncated_total", "Timelines that hit a login wall before the end of the window.", "reason")

	// Media.
	MediaDiscoveryToStored = histVec("media_discovery_to_stored_seconds", "Time from media discovery to object stored.", prometheus.ExponentialBuckets(1, 2, 14), "kind")
	MediaOutcome           = counterVec("media_download_total", "Media downloads by outcome.", "kind", "outcome")
	MediaDeleted           = counterVec("media_deleted_total", "Media objects deleted by reason.", "reason")

	// Persistence and jobs.
	PersistRows          = counterVec("persist_rows_total", "Rows written by the persister.", "entity", "op")
	CommentChanges       = counterVec("comment_changes_total", "Stored comments edited, marked deleted or seen again after a deletion, by kind (edited, deleted, restored).", "kind")
	DeletionChecks       = counterVec("deletion_checks_total", "Full comment reads of a post checked for deleted comments, by outcome (checked, skipped_partial).", "outcome")
	PersistBatch         = histVec("persist_batch_seconds", "Persister batch flush duration.", prometheus.ExponentialBuckets(0.005, 2, 12), "entity")
	Jobs                 = counterVec("jobs_total", "Jobs reaching a terminal state.", "status", "lane")
	ScheduleLag          = gaugeVec("schedule_lag_seconds", "Age of the oldest due page in the schedule.", "tier")
	PageTier             = gaugeVec("scheduled_pages", "Scheduled pages per adaptive tier.", "tier")
	PostRefreshes        = counterVec("post_refreshes_total", "Post comment refreshes dispatched, by kind (refresh, recrawl).", "kind")
	BreakerState         = gaugeVec("breaker_state", "Circuit breaker state (0 closed, 1 half-open, 2 open).", "name")
	QueryShortCircuit    = counterVec("ig_query_short_circuit_total", "Requests not sent because the query's breaker is open.", "query")
	SchedulerLeader      = gauge("scheduler_leader", "1 on the scheduler pod that holds the leader lock.")
	AIMDCeiling          = gauge("aimd_ceiling_rps", "Fleet-wide Facebook request ceiling set by AIMD.")
	AIMDAdjustments      = counterVec("aimd_adjustments_total", "AIMD ceiling changes by direction.", "direction")
	FleetRPS             = gauge("fleet_rps", "Facebook GraphQL responses per second across the fleet (last AIMD window).")
	BudgetUsed           = gaugeVec("budget_used", "Facebook traffic used today (UTC), by kind (requests, bytes) and lane.", "kind", "lane")
	BudgetProjected      = gaugeVec("budget_projected", "Facebook traffic projected by the end of the day, by kind.", "kind")
	BudgetLimit          = gaugeVec("budget_limit", "Daily traffic budget by kind (0 = none).", "kind")
	BudgetBackfillPaused = gauge("budget_backfill_paused", "1 while the budget holds backfill (p2) back.")
	BudgetStretch        = gauge("budget_stretch", "Interval factor the budget applies to the slow tiers (1 = none).")
	FleetBlockRate       = gauge("fleet_block_rate", "Share of fleet GraphQL responses that were blocks (last AIMD window).")
	LaneSlots            = gaugeVec("lane_slots", "Shared crawl slots per stage and lane: used, or tasks waiting for one.", "stage", "lane", "state")
	APIRequests          = counterVec("api_requests_total", "API requests.", "route", "code")
	KafkaProduce         = counterVec("kafka_produce_total", "Produced records.", "topic", "outcome")
	KafkaCommitted       = counterVec("kafka_commits_total", "Offset commits.", "group", "outcome")
	ConsumerLag          = gaugeVec("kafka_consumer_lag", "Records behind the end of each topic, per fbs consumer group (leader scheduler).", "group", "topic")

	// Registry sync (fbs-registry-sync).
	RegistrySyncRuns        = counterVec("registry_sync_runs_total", "Registry sync runs by status (ok, degraded, error).", "status")
	RegistrySyncLastSuccess = gauge("registry_sync_last_success_timestamp_seconds", "When the last registry sync run that gave every query a verdict completed.")
	RegistryChecks          = counterVec("registry_checks_total", "Registry entry checks on canary pages by entry status and verdict.", "query", "status", "verdict")
	RegistryPromotions      = counterVec("registry_promotions_total", "doc_ids promoted to active.", "query")
	RegistryQueryHealthy    = gaugeVec("registry_query_healthy", "1 when the query's active doc_id parsed on a canary page (or a replacement was promoted), 0 when it failed with none to promote.", "query")
	RegistryDiscovered      = gauge("registry_discovered_operations", "Relay operations found in the canary pages' bundles in the last run.")
)
