# Changelog

All notable changes to OutboxRelay will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.10.2] - 2026-06-25

### Changed — SB-2140 Stage 1 migration safety on large tables

- **`add_commit_seq` is now schema-only.** Dropped the `LOCK TABLE ... SHARE ROW
  EXCLUSIVE` + full-table `SELECT DISTINCT` pre-seed (which blocked writes for the
  duration of a multi-GB scan) — the trigger already lazily seeds each partition
  from the global sequence high-water on first insert, so the pre-seed was
  redundant. The migration now only adds the column, sequencer, trigger, and a
  `CONCURRENTLY` index — no row rewrite. `CREATE TRIGGER` still briefly locks the
  table, so run it in a quiet window or with a short `lock_timeout` if long
  bulk-import transactions may be in flight.
- **Historical backfill moved out of the migration into a rake task**,
  `outbox_relay:backfill_commit_seq[batch_size,sleep_ms]` — batched by primary key
  (O(n), not the O(n²) `WHERE commit_seq IS NULL LIMIT n` re-scan), idempotent,
  resumable, throttleable, and safe to run while the trigger and consumers are
  live. Keeps a 1M+ row rewrite out of the deploy step, and exits non-zero while
  any committed NULL `commit_seq` remains so it gates the Stage 2 cutover. Verified
  end-to-end on PostgreSQL 16.

## [0.10.1] - 2026-06-25

### Fixed

- **`add_commit_seq` / `harden_commit_seq` migrations failed on PostgreSQL with
  `syntax error at or near "REFERENCING"` (SB-2140).** 0.10.0 generated a
  `CONSTRAINT TRIGGER ... REFERENCING NEW TABLE ... FOR EACH STATEMENT`, but
  PostgreSQL forbids transition tables and `FOR EACH STATEMENT` on a constraint
  trigger (constraint triggers are `FOR EACH ROW` only). The gem's CI runs on
  SQLite, so the trigger was never executed and the bug shipped. Rewritten as a
  **per-row** `DEFERRABLE INITIALLY DEFERRED` constraint trigger operating on
  `NEW` (assigns via `UPDATE ... WHERE id = NEW.id`).
- The per-row trigger takes a **per-topic advisory transaction lock**
  (`pg_advisory_xact_lock`, two-int form — a distinct lock space from the
  consumer's bigint advisory locks) so commit-edge assignment serializes per topic
  and concurrent multi-partition transactions (the drainer workload) are
  **deadlock-free**. The commit-edge ordering guarantee is preserved: per
  `(topic, partition)`, `commit_seq` order == commit order == visibility order.
- Generated migrations now wrap their raw `execute` / data backfill in
  `safety_assured { ... }` so they pass under **strong_migrations** in consuming
  apps without manual patching.
- Added a **Postgres integration spec** (`spec/pg/commit_seq_trigger_spec.rb`) plus
  a `spec_pg` CI job that runs the real DDL against PostgreSQL and asserts
  assignment, the lost-event race, and deadlock-freedom — closing the CI gap that
  let the syntax error ship. Runs locally when `OUTBOX_PG_URL` is set.

Upgrading from 0.10.0: re-bump the gem and regenerate the migration
(`rails generate outbox_relay:add_commit_seq`); the regenerated file replaces the
broken one. If you have not run the 0.10.0 migration, nothing else is needed.

## [0.10.0] - 2026-06-25

### Added — SB-2140 Stage 1 (write-only; consumers still read `sequence`)

This release installs the commit-ordered sequence machinery but **does not change
consumer behavior** — consumers still fetch and track offsets by `sequence`. It is
the intermediate step of a staged rollout for the long-transaction offset-skipping
bug; the consumer cursor flip to `commit_seq` ships in a follow-up release.

Background on the bug: the consumer cursor keys off `sequence`, assigned by
`nextval` at INSERT (inside the producer transaction) but only visible at COMMIT.
Under concurrency, assignment order ≠ commit order, so a lower-`sequence` row that
commits *after* a higher one on the same partition lands below the already-advanced
high-water offset and is **silently never delivered**. The fix is a second ordering
token, `commit_seq`, assigned at the COMMIT edge — populated here, consumed later.

- **`commit_seq` — a commit-ordered sequence per `(topic, partition_key)`.**
  Assigned at the COMMIT edge by a `DEFERRABLE INITIALLY DEFERRED`, statement-level
  PostgreSQL constraint trigger that serializes on a per-partition sequencer row
  (`outbox_relay_partition_seq`). Because the sequencer row lock is released only
  at commit, `commit_seq` order == commit order == visibility order per partition.
  The DEFERRED edge means a long bulk-import transaction holds the sequencer lock
  only for milliseconds at commit, not for the whole transaction. The column is
  written but **not yet read** by any consumer code in this release.
- **`rails generate outbox_relay:add_commit_seq`** — adds the `commit_seq` column,
  the `outbox_relay_partition_seq` sequencer, the assignment trigger, the partial
  unique index `idx_outbox_commit_seq_fetch`, seeds existing partitions from the
  global sequence high-water, and backfills historical rows with
  `commit_seq = sequence`. Fresh installs get all of this from the install generator.
- **`rails generate outbox_relay:harden_commit_seq`** (optional, run last) — adds a
  second deferred assert trigger that fails loud at COMMIT if any inserted row was
  left with a NULL `commit_seq` (i.e. the assignment trigger was dropped/disabled).
  A plain `NOT NULL` column does not work here — `commit_seq` is NULL during INSERT
  and only set at commit — so the assert trigger is the correct mechanism. Run only
  after verifying production has no committed NULL `commit_seq` rows.

Caveat: triggers do not fire under `session_replication_role = 'replica'` (logical
replication / DMS / blue-green apply). An app-written outbox is unaffected. See
`docs/COMMIT_SEQ.md` for the full design, rollout, and manual Postgres verification
runbook.

## [0.9.4] - 2026-05-15

### Added

- **`PartitionMonitor#stale_consumer_offsets`** — returns `ConsumerOffset` rows whose base `consumer_group` is no longer present in `config/outbox_consumers.yml`. Supplements `#stale_consumer_groups` (which returns just the names) with per-partition diagnostic data (offset id, topic, partition_key, last_consumed_sequence, last_consumed_at, heartbeat_at, claim state) suitable for ops review or automated pruning. Accepts `idle_for:` (skip offsets whose heartbeat is newer than the cutoff, with NULL heartbeats always eligible) and `exclude_claimed:` (default true — never report rows held by an active partition claim).
- **`rake outbox_relay:stale_consumers`** — read-only diagnostic that lists stale offsets grouped by consumer_group, shows which topics have their cleanup blocked and at what sequence, and exits 1 when any are found so CI / monitoring can detect drift.
- **`rake outbox_relay:prune_stale_consumers[idle_days]`** — manual cleanup that deletes stale offsets older than `idle_days` (required, no default — operator must explicitly choose grace window). Bounded by the same safety filters as `stale_consumer_offsets` (not-in-config, idle, not actively claimed). Emits `outbox_relay.stale_offsets.pruned` notification following the existing `outbox_relay.<category>.<event>` convention so dashboards subscribed to `/^outbox_relay\./` pick it up alongside cleanup events.

## [0.9.3] - 2026-05-11

### Fixed

- `PartitionMonitor#calculate_lag` now mirrors the predicates that `OutboxConsumer#fetch_batch` applies, so lag counts only events the consumer would actually pick up:
  - **`event_filter`** — filtered consumers on high-volume topics previously reported phantom lag, because every event the consumer correctly skips via filter was still counted as backlog (easily triggering `high_lag` alerts on a healthy, current consumer). The filter is resolved by instantiating the configured `consumer_class` (lookup is memoized per `(consumer_group, topic)`); when the class cannot be resolved the lag falls back to the unfiltered count and a `partition_monitor_event_filter_lookup_failed` log entry is emitted.
  - **`not_expired`** — expired events are abandoned by the system (`CleanupExpiredEventsJob` deletes them within minutes) and are skipped by `fetch_batch`; counting them as lag produced transient phantom spikes between expiry and cleanup.

  Complements 0.8.7's switch to `COUNT(*)` semantics.

## [0.9.2] - 2026-04-29

### Changed

- `CleanupExpiredEventsJob` now uses a time-budgeted inner loop. `cleanup_batch_size` is the size of a single DELETE chunk (default 10_000); total work per run is bounded by the new `cleanup_max_runtime` (default 30 seconds), shared across the DLQ and events phases. Recommend a `*/5 * * * *` schedule. The notification payload gains an `iterations: { dlq:, events: }` key.

## [0.9.0] - 2026-04-22

### Added

- **`OutboxRelay.default_event_ttl`** – optional module-level default TTL applied by `OutboxPublisher.publish` when the caller does not pass `:expires_at`. When set, every published event gets `expires_at = default_event_ttl.from_now`. Callers can opt out of the default by passing `expires_at: nil` explicitly (event never expires) or override with any `Time`. Must be an `ActiveSupport::Duration` — bare Integers are rejected with `OutboxRelay::ConfigurationError` (so `OutboxRelay.default_event_ttl = 14` is caught instead of silently meaning "14 seconds").
- **`OutboxRelay.dlq_resolved_ttl`** – optional module-level TTL for resolved DLQ entries. When set, `CleanupExpiredEventsJob` also deletes dead-letter rows with `resolution_status IN (resolved, reprocessed, ignored)` whose `resolved_at` is older than the TTL. TTL is measured from `resolved_at` (populated by `mark_as_resolved!` / `mark_as_reprocessed!` / `mark_as_ignored!`), not `created_at` — a DLQ entry created months ago but only just resolved is preserved for the full retention window. Unresolved / retrying entries are preserved unconditionally. Same type validation as `default_event_ttl`.
- **`OutboxRelay.cleanup_enabled` / `OutboxRelay.cleanup_batch_size`** as module-level `mattr_accessor`s so they can be configured via `Rails.application.config.outbox_relay.*` and propagated by the Rails engine. Previously these lived only on the (frozen) `OutboxRelay.configuration` object and could not be set from host apps in production.
- **`outbox_relay.cleanup.completed` ActiveSupport::Notifications event** emitted by `CleanupExpiredEventsJob` after every run — success, timeout, or failure — with payload `{ events_deleted:, dlq_deleted:, duration:, error_class:, timeout: }`. Follows the gem-wide `outbox_relay.<category>.<event>` naming so subscribers using the documented `/^outbox_relay\./` regex will see it. On failure, phase-1 deletion counts are preserved in the payload; `error_class` names the exception; `timeout: true` signals `PG::QueryCanceled` was swallowed.
- `CleanupExpiredEventsJob#perform` now returns the same hash it emits. On timeout the hash additionally carries `timeout: true`; on re-raised errors the job raises after emitting the notification.
- New generator `rails generate outbox_relay:add_dlq_cleanup_index` to add the partial index (`idx_dlq_resolved_at_cleanup`) that the DLQ retention sweep uses. Fresh installs get this index from the install generator.

### Changed

- `OutboxPublisher.publish` signature: `expires_at: nil` → `**opts`. The publisher now distinguishes "key not passed" (applies `default_event_ttl`) from "explicit `nil`" (never expires). Unknown keyword arguments are rejected with `ArgumentError` (so `expire_at:` typos still fail fast). Backwards-compatible for existing callers that pass `expires_at: <Time>` or omit it entirely.
- `OutboxPublisher.publish` no longer wraps every `StandardError` in `PublishError`. Only `ActiveRecord::RecordInvalid` is reclassified (preserves the existing contract). `ActiveRecord::ConnectionNotEstablished`, PG driver errors, `OutboxRelay::ConfigurationError`, and programmer errors (`NoMethodError`, `NameError`) propagate as their original class, so callers can handle them distinctly.
- `CleanupExpiredEventsJob`:
  - Reads `cleanup_enabled` / `cleanup_batch_size` from the module-level accessors instead of the frozen configuration instance.
  - Rewrote the SQL filter `sequence < ALL(SELECT COALESCE(MIN(...)))` to `sequence < (SELECT COALESCE(MIN(...)))`. Single-value subquery makes these equivalent and portable across PostgreSQL and SQLite (used in tests).
  - DLQ retention now keys off `resolved_at` instead of `created_at` (see above).
  - Instrumentation moved to an `ensure` block so the notification fires on every run. Notification-subscriber errors are isolated and no longer misclassify as cleanup failures.

### Removed

- `OutboxRelay::Configuration#cleanup_enabled` / `#cleanup_batch_size` (and their `DEFAULT_CLEANUP_*` constants). These previously lived on the frozen `OutboxRelay.configuration` instance and were **not** read by the cleanup job — setting them from host code was a silent no-op. Use the module-level `OutboxRelay.cleanup_enabled` / `OutboxRelay.cleanup_batch_size` instead (which also flow from `Rails.application.config.outbox_relay.*`).

## [0.8.7] - 2026-03-29

### Fixed

- **Accurate lag calculation** - All lag metrics (`PartitionMonitor#calculate_lag`, `OutboxConsumer#lag`, `ConsumerOffset#lag`) now use `COUNT(*)` of events after the consumer's offset instead of `max_sequence - last_consumed_sequence`. The old sequence-gap formula wildly overstated lag because sequence numbers are global across all topics. A consumer filtering for rare event types could show 194k "lag" when actual pending events were 0.

## [0.8.6] - 2026-01-26

### Changed

- **Reduced log verbosity for multi-instance deployments** - Changed lifecycle events from INFO to DEBUG level to reduce log volume by ~85% in ECS environments with many instances:
  - `worker_started` - Now DEBUG (was INFO). Worker starts are frequent when multiple instances compete for partitions.
  - `heartbeat_started` / `heartbeat_stopped` - Now DEBUG (was INFO). Routine infrastructure noise.
  - `partition_claimed` / `partition_claim_released` - Now DEBUG (was INFO). Expected behavior in distributed systems.
  - Removed `topic_description` and `consumer_group_description` from `worker_started` payload to reduce log size.

- **Operational visibility preserved** - Important events remain at actionable levels:
  - `worker_stopped` - INFO (with total_processed, loop_count for metrics)
  - `partition_claim_lost` - ERROR (critical: another worker took over)
  - `heartbeat_*_failed` - ERROR (requires investigation)
  - Use `rake outbox_relay:status` for operational visibility instead of logs.

## [0.8.5] - 2026-01-25

### Added

- **PartitionMonitor class** - Detects orphaned partitions and monitors partition health
  - `orphaned_partitions` - Returns partitions without active workers
  - `partition_health` - Returns status for all expected partitions (`:active`, `:stale`, `:orphaned`)
  - `health_report` - Comprehensive report with totals and problem partitions
  - `partition_lag` - Returns lag for specific partition
  - Addresses production incident where ShipStation partition went unprocessed for 5+ hours

- **Supervisor health check loop** - Periodic health monitoring (default: every 30 seconds)
  - Emits instrumentation events for orphaned partitions
  - Emits instrumentation events for high lag partitions
  - Emits instrumentation events for stale workers

- **Partition health instrumentation events**:
  - `outbox_relay.partition_health.orphaned` - Critical: partition has no active worker
  - `outbox_relay.partition_health.high_lag` - Warning: partition lag exceeds threshold
  - `outbox_relay.partition_health.stale_worker` - Warning: worker heartbeat is stale
  - `outbox_relay.partition_health.worker_missing` - Critical: expected worker not found

- **Monitoring configuration** in `config/outbox_consumers.yml`:
  - `lag_alert_threshold` - Alert when lag exceeds this (default: 100)
  - `orphan_check_interval` - Health check interval in seconds (default: 30)
  - `stale_worker_timeout` - Consider worker stale after this many seconds (default: 60)

- **ConsumerOffset scopes**:
  - `.orphaned` - Partitions without active claims
  - `.actively_claimed` - Partitions with active claims

### Fixed

- **Orphaned partition alerts only when lag > 0** - Reduces alert noise by only emitting `partition_health.orphaned` events when there are actually pending events to process. Orphaned partitions with `lag=0` are normal (no events to process, worker released claim). This prevents excessive Sentry/monitoring alerts that can cause rate limiting.

- **Orphaned partition re-check before alert** - Prevents false alerts during container failover by re-checking if a partition is still orphaned before emitting the alert. This eliminates race conditions where a new worker claims the partition between the health report query and alert emission.

- **DLQ cache invalidation** - Immediately invalidate DLQ cache after adding event to prevent retry burn

## [0.8.0] - 2025-12-15

### Added

- **`auto_offset_reset` parameter** - Control where new consumer groups start consuming:
  - `:latest` (default) - Start from the latest sequence, skip historical events (safe for production deploys)
  - `:earliest` - Start from sequence 0, process all historical events (for backfill scenarios)
  - Follows Kafka's `auto.offset.reset` semantics
  - Prevents accidental reprocessing of all historical events when deploying new consumers

### Changed

- **ConsumerOffset.find_or_initialize_for** now accepts `auto_offset_reset` parameter
- **OutboxConsumer#initialize** now accepts `auto_offset_reset` parameter (default: `:latest`)

### Migration Notes

**No migration required** - existing consumers continue to work as before.

**New behavior for NEW consumer groups:**
- Previously: New consumer groups started from sequence 0 (process all history)
- Now: New consumer groups start from latest sequence by default (skip history)

**To process historical events** (backfill scenario):
```ruby
class BackfillConsumer < OutboxRelay::OutboxConsumer
  def initialize(partition_key:)
    super(
      consumer_group: "backfill_consumer",
      topic: "order_lifecycle",
      partition_key: partition_key,
      auto_offset_reset: :earliest  # Process all historical events
    )
  end
end
```

## [0.4.0] - 2025-11-11

### Added

- **Solid Queue-style CLI** - Generator now creates `bin/outbox_relay` executable in Rails apps (following Solid Queue pattern)
- **Thor-based CLI** - Migrated from OptionParser to Thor for better command structure and help output
- **Automatic fork-safety** - Generated `bin/outbox_relay` automatically handles macOS fork-safety environment variables

### Changed

- **BREAKING**: Removed `rake outbox_relay:start` task - use `./bin/outbox_relay` instead
- **Installation process** - `rails generate outbox_relay:install` now creates `bin/outbox_relay` executable
- **All documentation updated** - README, STATUS, CHANGELOG, and docs now reference `./bin/outbox_relay`
- **CLI warning messages** - Updated macOS fork-safety warnings to reference generated executable

### Migration Guide

If upgrading from 0.3.x:

1. Run generator to create new executable: `bin/rails generate outbox_relay:install`
2. Update Procfile/systemd/docker: Change `bundle exec rake outbox_relay:start` to `./bin/outbox_relay`
3. Start using new command: `./bin/outbox_relay` (with optional flags like `--polling-interval 0.5`)

Note: Utility rake tasks (status, lag, cleanup, etc.) remain unchanged.

## [0.3.0] - 2025-11-11

### Changed

- **ActiveSupport::Notifications instrumentation** - Replaced Sentry direct calls with Rails instrumentation events
- All errors and critical events now emit via `ActiveSupport::Notifications` for flexible backend integration
- Applications can subscribe to `outbox_relay.*` events and route to any monitoring backend (Sentry, DataDog, New Relic)

## [0.2.0] - 2025-01-10

### Added

- **Enterprise-grade error handling** - Comprehensive error handling across all components with Sentry integration
- **Explicit partition count configuration** - Support for explicit partition_count in OUTBOX_CONSUMERS config to prevent silent data loss
- **Dynamic procline updates** - Worker process titles now show real-time status (processing/waiting)
- **Orphaned partition detection** - Automatic warning when events exist outside configured partition range
- **Signal queue bounds** - Protection against signal flooding (max 10 signals with deduplication)
- **Exponential backoff for restarts** - Prevents restart storms during infrastructure issues (1s → 60s cap)
- **Heartbeat failure tracking** - Workers shutdown after 5 consecutive heartbeat failures
- **SKIP LOCKED optimization** - Parallel workers no longer contend for same events
- **DLQ caching** - 5-second cache for DLQ queries eliminates expensive subqueries
- **Smart delay fallback** - Assumes backlog when lag calculation fails after processing events
- **Comprehensive documentation** - 400+ lines of technical documentation covering architecture, design decisions, and patterns

### Changed

- **Improved error context** - All errors now include detailed context about why they're unrecoverable and expected operator actions
- **Enhanced rake tasks** - Stop, status, lag, and cleanup tasks now have comprehensive error handling with clear output
- **Better CLI setup** - Step-by-step error handling with actionable suggestions for each failure mode
- **Metadata sanitization** - Process metadata now sanitized via JSON round-trip to prevent serialization errors
- **Instrumentation safety** - All instrumentation calls wrapped with safe_instrument to prevent failures from blocking operations
- **Consumer offset tracking** - New consumer offsets now include initial heartbeat timestamp

### Fixed

- **SQL injection vulnerability** - Advisory lock queries now use parameterized SQL (C1)
- **Race conditions** - Fixed TOCTOU race condition in process_id checks using local caching (C2, H20)
- **Fork safety** - Comprehensive fork safety documentation and environment checks for macOS (C8, M5)
- **Database reconnection** - Added exponential backoff retry logic for post-fork reconnection (H1)
- **Callback error handling** - Boot/shutdown callbacks now have proper error handling with selective re-raise (C4)
- **Registration failures** - Process registration failures now properly raise errors instead of silent failure (C5)
- **Fork failures** - System-level fork failures now gracefully logged instead of crashing supervisor (C6)
- **Signal handling** - Failed signal delivery now tracked and reported for manual cleanup (C3)
- **Deregistration logging** - Nil checks prevent errors when deregistering never-registered processes (H3)
- **Polling instrumentation** - Separated instrumentation errors from polling errors for better debugging (H11)
- **Offset update logging** - Distinguish between consume_message failures and offset update failures (H12)
- **DLQ save errors** - Added explicit error handling for DLQ entry save failures with retry logic (H13)
- **Sequence generation** - Added nil checks and clear error messages for sequence generation failures (H16)
- **Logger silencing** - Continue polling even if logger.silence fails (H18)
- **Process ID race** - Cache process_id to avoid TOCTOU issues in registered? checks (H20)

### Security

- **SQL injection prevention** - All advisory lock queries now use ActiveRecord.sanitize_sql_array with parameter binding
- **Input validation** - Lock keys validated as integers before use in SQL queries
- **Safe metadata handling** - JSON serialization prevents code injection via metadata

### Performance

- **SKIP LOCKED** - Reduces lock contention in multi-worker deployments, enables true parallel processing
- **DLQ caching** - 5-second TTL cache eliminates 10ms-1s subquery on every poll cycle
- **Smart fallback** - Better throughput under partial failures by assuming backlog when lag query fails

### Documentation

- **Worker architecture** - Comprehensive documentation of fork-based design, dynamic polling, and safety mechanisms
- **Advisory lock algorithm** - Deep dive into concurrent event processing with timeline examples
- **Signal handling** - Explanation of queue-based signal handling and Unix signal restrictions
- **Interruptible sleep** - Documentation of responsive sleep pattern for fork-based workers
- **Fork safety** - 82-line guide to fork safety challenges across platforms
- **ProcessRegistry upgrade path** - Clear guidance on when and how to upgrade to persistent registry
- **Error handling context** - Detailed explanations of why errors are unrecoverable and operator actions

### Operations

- **Improved rake tasks** - All tasks (stop, status, lag, cleanup) now have comprehensive error handling
- **Better monitoring** - Dynamic procline shows processing status and progress
- **Operator guidance** - Error messages include specific actions for operators to take
- **Orphaned detection** - Automatic detection and warning of misconfigured partition keys

## Statistics

- **51 issues fixed** (89% completion rate)
  - CRITICAL: 10/10 (100%)
  - HIGH: 20/20 (100%)
  - MEDIUM: 21/27 (78%)
- **~1,500 lines** added/modified across 15 files
- **0 breaking changes** - 100% backward compatible
- **400+ lines** of technical documentation added

## Migration Notes

### Backward Compatibility

All changes are backward compatible. No migration required for existing deployments.

### Optional Enhancements

**Explicit partition count:**
```ruby
# Before (queries database):
OUTBOX_CONSUMERS = {
  "notifications" => {
    "user_events" => "NotificationsConsumer"
  }
}

# After (recommended - no DB query):
OUTBOX_CONSUMERS = {
  "notifications" => {
    "user_events" => {
      consumer_class: "NotificationsConsumer",
      partition_count: 4  # Explicit
    }
  }
}
```

**macOS fork safety:**
```bash
# The generated bin/outbox_relay executable automatically handles this:
./bin/outbox_relay
```

## Contributors

- Claude (AI Assistant) - Code review, fixes, and documentation
- Generated with [Claude Code](https://claude.com/claude-code)

---

**Note:** This CHANGELOG covers a comprehensive code quality and reliability improvement initiative that addressed 51 issues across security, reliability, performance, and documentation.
