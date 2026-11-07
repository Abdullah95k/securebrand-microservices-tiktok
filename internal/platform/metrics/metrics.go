// Package metrics holds the Prometheus registry and the service metrics
// shared across binaries. Metric names are prefixed with tks_, never fbs_:
// fbs's alert rules select by metric name alone, so a tks pod exporting
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
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "tks", Name: name, Help: help}, labels)
	Registry.MustRegister(c)
	return c
}

func counter(name, help string) prometheus.Counter {
	c := prometheus.NewCounter(prometheus.CounterOpts{Namespace: "tks", Name: name, Help: help})
	Registry.MustRegister(c)
	return c
}

func histVec(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "tks", Name: name, Help: help, Buckets: buckets}, labels)
	Registry.MustRegister(h)
	return h
}

func gauge(name, help string) prometheus.Gauge {
	g := prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "tks", Name: name, Help: help})
	Registry.MustRegister(g)
	return g
}

func gaugeVec(name, help string, labels ...string) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "tks", Name: name, Help: help}, labels)
	Registry.MustRegister(g)
	return g
}

var (
	// Requests to the scraped platform.
	FBRequests      = counterVec("tt_requests_total", "Platform requests by query and response class.", "query", "class", "tier")
	FBRequestSecs   = histVec("tt_request_seconds", "Platform request latency.", prometheus.ExponentialBuckets(0.05, 2, 10), "query", "tier")
	FBResponseBytes = histVec("tt_response_bytes", "Decoded platform response size.", prometheus.ExponentialBuckets(1024, 2, 12), "query")
	ProxyBytes      = counterVec("proxy_bytes_total", "Wire bytes through each proxy tier.", "tier", "dir")

	// Parsing.
	ParseTotal        = counterVec("parse_total", "Parser runs by outcome.", "query", "parser_version", "outcome")
	ParseMissingField = counterVec("parse_missing_field_total", "Optional or required fields missing from responses.", "query", "field")
	ParseShadow       = counterVec("parse_shadow_total", "Responses a candidate (shadow) parser also parsed, by outcome (same, differs, drift, error).", "query", "outcome")

	// Identities.
	IdentityActive  = gaugeVec("identity_active", "Live identities in this pod.", "tier")
	IdentityRetired = counterVec("identity_retired_total", "Retired identities by reason.", "tier", "reason")
	// PageLoadsDeferred counts page loads an exit put off: "paced" (the
	// minimum interval) or "backoff" (after a walled or failed page load).
	PageLoadsDeferred = counterVec("proxy_page_loads_deferred_total", "Page loads put off by an exit's page-load gate, by reason.", "tier", "reason")
	Bootstraps        = counterVec("bootstrap_total", "Identity bootstraps.", "method", "outcome")

	// Tasks.
	Tasks       = counterVec("tasks_total", "Crawl tasks by stage, lane and outcome.", "stage", "lane", "outcome")
	TaskSeconds = histVec("task_seconds", "Crawl task duration.", prometheus.ExponentialBuckets(0.1, 2, 12), "stage")
	TaskYields  = counterVec("task_yields_total", "Tasks that yielded a continuation.", "stage")
	DLQ         = counterVec("dlq_total", "Tasks sent to the dead letter queue.", "stage", "reason")
	Retries     = counterVec("retries_total", "Tasks sent to a retry topic.", "stage", "tier")

	// Coverage.
	CommentCompleteness = histVec("comment_completeness_ratio", "Collected / reported comments per post at job completion.", []float64{0.1, 0.25, 0.5, 0.7, 0.8, 0.9, 0.95, 0.99, 1.0, 1.01}, "lane")
	ListStops           = counterVec("list_stops_total", "tks account list walks by mode (full, incremental, refresh) and why they stopped (first_post, age, watermark, max_posts, stuck).", "mode", "reason")
	// PostVanish counts stored posts a full list page covering their date
	// did not return (docs/tiktok/discovery-plan.md §17), by what the
	// check made of them: unavailable (the post's page confirmed it),
	// visible (the page still shows it), account (the page says the
	// account, not the post, is unavailable: nothing marked),
	// unknown_code (a statusCode not known to mean a post that is gone:
	// nothing marked), reading_problem (too many on one page: nothing
	// marked), deferred (over the check's cap, or the page load paced) and
	// error.
	PostVanish = counterVec("post_vanish_total", "tks stored posts a list page covering their date did not return, by what the check made of them (unavailable, visible, account, unknown_code, reading_problem, deferred, error).", "result")
	// tks's browser signer (internal/tt/sign; build brief D11).
	SignerSigns     = counterVec("signer_signs_total", "Calls the browser signer signed, by query and result (ok, error, busy).", "query", "result")
	SignerSeconds   = histVec("signer_sign_seconds", "Time to sign one call in a signer page.", prometheus.ExponentialBuckets(0.01, 2, 12), "query")
	SignerPageOpens = counterVec("signer_page_opens_total", "Signer pages opened, by result (ok, error).", "result")
	SignerPages     = gauge("signer_pages", "Signer pages open in this pod.")
	// SignerPagesRefused counts pages replaced because TikTok refused their
	// signed calls tt.signer.max_refusals times in a row.
	SignerPagesRefused = counterVec("signer_pages_refused_total", "Signer pages replaced after TikTok refused their signed calls in a row.").WithLabelValues()
	// SignerPagesMax is tt.signer.pages, the pool's cap: pages at the cap
	// with busy signatures mean the pool is too small.
	SignerPagesMax   = gauge("signer_pages_max", "Signer pages this pod may keep open (tt.signer.pages).")
	SignerPageCloses = counterVec("signer_page_closes_total", "Signer pages closed, by reason (spent, idle, memory, refused, error, room, shutdown, stuck: did not close in time, killed: past kill_page_mb while signing).", "reason")
	// SignerRemoteCalls counts the crawlers' and the canary's calls to
	// tks-signer (tt.signer.mode remote), by call (sign, outcome) and
	// result (ok, busy, error, refused, unreachable, cancelled).
	SignerRemoteCalls = counterVec("signer_remote_calls_total", "Calls to tks-signer by call (sign, outcome) and result (ok, busy, error, refused, unreachable, cancelled).", "call", "result")
	// SignedCalls counts TikTok's answers to the calls a signer page signed:
	// answered, or refused (an empty answer).
	SignedCalls = counterVec("signed_calls_total", "Calls a signer page signed, by TikTok's answer (answered, refused).", "result")
	// SignRetries counts signed calls TikTok refused and the client signed
	// and sent once more, on another page.
	SignRetries = counterVec("sign_retries_total", "Signed calls refused and sent once more, signed on another page.").WithLabelValues()
	// tks's comment reads (internal/tt/stage; ADR 0007 §5).
	CommentReads      = counterVec("comment_reads_total", "tks comment reads by kind (post, thread) and how they ended (complete, partial, truncated).", "kind", "state")
	TimelineTruncated = counterVec("timeline_truncated_total", "Timelines that hit a login wall before the end of the window.", "reason")

	// Media.
	MediaDiscoveryToStored = histVec("media_discovery_to_stored_seconds", "Time from media discovery to object stored.", prometheus.ExponentialBuckets(1, 2, 14), "kind")
	MediaOutcome           = counterVec("media_download_total", "Media downloads by outcome.", "kind", "outcome")
	SubtitleFetches        = counterVec("subtitle_fetches_total", "Videos' subtitle files the crawlers fetched (M17), by result: fetched; failed (an error, a refusal or not WebVTT); expired (the link had expired); too_large (over tt.subtitles.max_bytes, recorded without its text).", "result")
	MediaDeleted           = counterVec("media_deleted_total", "Media objects deleted by reason.", "reason")

	// Persistence and jobs.
	PersistRows        = counterVec("persist_rows_total", "Rows written by the persister.", "entity", "op")
	ContentEvents      = counterVec("content_events_total", "The shared contract's content.* events by topic and result: sent; unchanged (the gate held it back); invalid (it broke the contract's conformance checks and was not sent).", "topic", "result")
	PersistQuarantined = counterVec("persist_quarantined_total", "Records the persister set aside in its quarantine topic: undecodable, or refused by Postgres.", "entity", "reason")
	CommentChanges     = counterVec("comment_changes_total", "Stored comments edited, marked deleted or seen again after a deletion, by kind (edited, deleted, restored).", "kind")
	PostStatusChanges  = counterVec("post_status_changes_total", "tks stored posts whose status changed, by new status (unavailable, visible, private).", "status")
	DeletionChecks     = counterVec("deletion_checks_total", "Full comment reads of a post checked for deleted comments, by outcome (checked, skipped_partial).", "outcome")
	PersistBatch       = histVec("persist_batch_seconds", "Persister batch flush duration.", prometheus.ExponentialBuckets(0.005, 2, 12), "entity")
	Jobs               = counterVec("jobs_total", "Jobs reaching a terminal state.", "status", "lane")
	ScheduleLag        = gaugeVec("schedule_lag_seconds", "Age of the oldest due page in the schedule.", "tier")
	PageTier           = gaugeVec("scheduled_pages", "Scheduled pages per adaptive tier.", "tier")
	// tks's scheduler (internal/tt/scheduler; build brief §8 A1).
	Checks         = counterVec("checks_total", "tks scheduled account checks that ended, by kind (check, walk, backfill), job status and whether they ended within their interval of being started (window: in, late; build brief §8 A1).", "kind", "result", "window")
	CheckOldest    = gauge("account_check_oldest_seconds", "Time since the active scheduled account with the oldest successful check was last checked well (leader scheduler).")
	RefreshOverdue = gauge("account_refresh_overdue", "Active scheduled tks accounts whose daily profile read is more than a day overdue (build brief §8 A1; leader scheduler).")
	// Paced admission of new accounts (docs/tiktok/discovery-plan.md §5.3).
	AdmissionQueue   = gauge("admission_queue", "tks accounts registered and waiting for paced admission before their first read (leader scheduler).")
	AdmittedAccounts = counter("admitted_accounts_total", "tks new accounts paced admission let start their first read.")
	// Discovery's entry check and dormant list (docs/tiktok/discovery-plan.md §3, §4).
	DiscoveryFindings   = counterVec("discovery_findings_total", "tks accounts discovery sources found, by source and the state the finding left the account in (added for one already monitored).", "source", "state")
	EntryChecks         = counterVec("entry_checks_total", "tks entry checks decided, by the account's new state (qualified, not_iraqi, dormant, pending for a retry).", "state")
	DiscoveredAccounts  = counter("discovered_accounts_total", "tks accounts discovery registered for monitoring.")
	DormantMoves        = counterVec("dormant_moves_total", "tks discovered accounts moved from monitoring to the dormant list, by reason (private, inactive, never_posted, banned, gone).", "reason")
	DiscoveryCandidates = gaugeVec("discovery_candidates", "tks accounts discovery found and does not monitor, by state (pending, checking, qualified, not_iraqi, dormant, blocked; added counts those it registered) (leader scheduler).", "state")
	// Discovery part B (docs/tiktok/discovery-plan.md §7): people a
	// monitored account's new posts, bio and comments bring in.
	DiscoverySightings        = counterVec("discovery_sightings_total", "tks people the persister recorded for discovery to count: from monitored accounts' new posts, bios and comments (part B: mention, reply, commenter) from their following and followers lists (part D: following, follower) and as authors of posts found on discovery pages (part C: search, hashtag, sound, feed, related), by source.", "source")
	DiscoverySightingsCounted = counter("discovery_sightings_counted_total", "tks part B sightings the leader scheduler counted into accounts' sources and candidates.")
	DiscoverySightingsBacklog = gauge("discovery_sightings_backlog", "tks part B and D sightings waiting to be counted (leader scheduler).")
	// Discovery part D (docs/tiktok/discovery-plan.md §9): monitored
	// accounts' following and followers lists.
	FollowPages     = counterVec("follow_pages_total", "tks following and followers list pages read, by direction (following, follower) and result (ok, hidden, gone, refused, error).", "direction", "result")
	FollowReads     = counterVec("follow_reads_total", "tks following and followers list reads that ended, by direction and why they stopped (end, known, cap, day_cap, stuck, hidden, gone, refused).", "direction", "stop")
	FollowLinks     = counterVec("follows_stored_total", "tks follow links the persister wrote, by direction and whether the link was new (new, seen).", "direction", "result")
	FollowListsDue  = gaugeVec("follow_lists_due", "tks following and followers lists due to be read and not yet claimed, by direction (leader scheduler).", "direction")
	FollowListState = gaugeVec("follow_lists", "tks following and followers lists by direction and state (new, ok, hidden, gone, refused) (leader scheduler).", "direction", "state")
	// Discovery part C (M12): TikTok's discovery pages.
	DiscoverPages   = counterVec("discover_pages_total", "tks discovery page reads (search, hashtag, sound, feed, related pages), by source and result (ok, gone, refused, error).", "source", "result")
	DiscoverReads   = counterVec("discover_reads_total", "tks discovery page reads that ended, by source and why they stopped (end, cap, day_cap, stuck, empty, gone, refused).", "source", "stop")
	DiscoverPosts   = counterVec("discover_posts_total", "tks posts found on discovery pages, by source.", "source")
	DiscoverTargets = gaugeVec("discover_targets", "tks discovery targets (keywords, hashtags, sounds, posts, the feed) by source and state (new, ok, gone, refused) (leader scheduler).", "source", "state")
	DiscoverDue     = gaugeVec("discover_targets_due", "tks discovery targets due to be read and not yet claimed, by source (leader scheduler).", "source")
	// tks's delay before a new post is stored (build brief §8 A2): the
	// buckets hold the targets (median 1.5 h, 99 % in 3 h, 99.9 % in 6 h).
	PostStoreDelay = histVec("post_store_delay_seconds", "Time from a new tks post's createTime to its row in Postgres, for posts first read by a scheduled check (mode incremental) or a metrics walk in its place (refresh); backfills are not counted.",
		[]float64{300, 900, 1800, 3600, 5400, 7200, 9000, 10800, 14400, 21600, 43200, 86400}, "mode")
	ScheduledAccounts = gaugeVec("scheduled_accounts", "tks accounts on the schedule, by account status (active, private, unavailable, banned, unknown).", "status")
	PostRefreshes     = counterVec("post_refreshes_total", "Post comment refreshes dispatched, by kind (refresh, recrawl).", "kind")
	BreakerState      = gaugeVec("breaker_state", "Circuit breaker state (0 closed, 1 half-open, 2 open).", "name")
	QueryShortCircuit = counterVec("tt_query_short_circuit_total", "Requests not sent because the query's breaker is open.", "query")
	SchedulerLeader   = gauge("scheduler_leader", "1 on the scheduler pod that holds the leader lock.")
	AIMDCeiling       = gauge("aimd_ceiling_rps", "Fleet-wide Facebook request ceiling set by AIMD.")
	AIMDAdjustments   = counterVec("aimd_adjustments_total", "AIMD ceiling changes by direction.", "direction")
	FleetRPS          = gauge("fleet_rps", "Facebook GraphQL responses per second across the fleet (last AIMD window).")
	BudgetUsed        = gaugeVec("budget_used", "Facebook traffic used today (UTC), by kind (requests, bytes) and lane.", "kind", "lane")
	BudgetProjected   = gaugeVec("budget_projected", "Facebook traffic projected by the end of the day, by kind.", "kind")
	BudgetLimit       = gaugeVec("budget_limit", "Daily traffic budget by kind (0 = none).", "kind")
	// tks's cost (build brief §8 A5; internal/tt/cost), priced by the leader
	// over the last 24 hours.
	CostPerAccount = gauge("cost_per_account_usd", "tks's cost per account a day in USD over the last 24 hours (0 without accounts).")
	CostDay        = gaugeVec("cost_usd_day", "tks's cost over the last 24 hours in USD, by part (proxy, browser, compute, storage, total).", "part")
	CostBudget     = gauge("cost_budget_per_account_usd", "The owner's budget per account a day in USD (tt.cost.budget_per_account_usd; 0 = none).")
	CostAlertShare = gauge("cost_budget_alert_share", "The share of the budget the cost alert fires at (tt.cost.alert_share).")
	CostInputs     = gaugeVec("cost_inputs", "What tks's cost was priced from, by input (accounts, ips, proxy_gb_day, browser_hours_day, stored_gb).", "input")
	CostOverBudget = gauge("cost_over_budget", "1 while tks's cost per account is over the budget, so backfills pause.")
	// SignerPageSeconds is the signer pages' browser time (A5's browser part).
	SignerPageSeconds    = counterVec("signer_page_seconds_total", "Seconds the browser signer's pages were open (opening included).").WithLabelValues()
	BudgetBackfillPaused = gauge("budget_backfill_paused", "1 while the budget holds backfill (p2) back.")
	BudgetStretch        = gauge("budget_stretch", "Interval factor the budget applies to the slow tiers (1 = none).")
	FleetBlockRate       = gauge("fleet_block_rate", "Share of fleet GraphQL responses that were blocks (last AIMD window).")
	LaneSlots            = gaugeVec("lane_slots", "Shared crawl slots per stage and lane: used, or tasks waiting for one.", "stage", "lane", "state")
	APIRequests          = counterVec("api_requests_total", "API requests.", "route", "code")
	KafkaProduce         = counterVec("kafka_produce_total", "Produced records.", "topic", "outcome")
	KafkaCommitted       = counterVec("kafka_commits_total", "Offset commits.", "group", "outcome")
	KafkaHandlerPanics   = counterVec("kafka_handler_panics_total", "Record handlers that panicked (the record is retried; its partition waits).", "group")
	ConsumerLag          = gaugeVec("kafka_consumer_lag", "Records behind the end of each topic, per fbs consumer group (leader scheduler).", "group", "topic")

	// tks-canary (internal/tt/canary; build brief M8).
	CanaryRuns        = counterVec("canary_runs_total", "tks canary runs by status (ok, degraded, error, skipped).", "status")
	CanaryLastSuccess = gauge("canary_last_success_timestamp_seconds", "When the last tks canary run that gave every operation a verdict finished.")
	CanaryChecks      = counterVec("canary_checks_total", "tks operation variant checks on canary accounts, by operation, the variant's status and the verdict (ok, stale, drift, inconclusive).", "operation", "status", "verdict")
	CanaryPromotions  = counterVec("canary_promotions_total", "Operation variants the tks canary made active, by operation and why (failed: the active one stopped answering; unsigned: it answers without a signature).", "operation", "why")
	CanaryHealthy     = gaugeVec("canary_operation_healthy", "1 when the operation's active variant answered on a canary account (or a working one was promoted), 0 when it failed with none to promote.", "operation")
	CanarySigner      = gauge("canary_signer_healthy", "1 when the last tks canary run had its signed checks signed, 0 when the signer failed every one (unchanged when the run signed nothing).")

	// Registry sync (fbs-registry-sync).
	RegistrySyncRuns        = counterVec("registry_sync_runs_total", "Registry sync runs by status (ok, degraded, error).", "status")
	RegistrySyncLastSuccess = gauge("registry_sync_last_success_timestamp_seconds", "When the last registry sync run that gave every query a verdict completed.")
	RegistryChecks          = counterVec("registry_checks_total", "Registry entry checks on canary pages by entry status and verdict.", "query", "status", "verdict")
	RegistryPromotions      = counterVec("registry_promotions_total", "doc_ids promoted to active.", "query")
	RegistryQueryHealthy    = gaugeVec("registry_query_healthy", "1 when the query's active doc_id parsed on a canary page (or a replacement was promoted), 0 when it failed with none to promote.", "query")
	RegistryDiscovered      = gauge("registry_discovered_operations", "Relay operations found in the canary pages' bundles in the last run.")
)
