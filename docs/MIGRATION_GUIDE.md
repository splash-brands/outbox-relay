# Migration Guide: State Machine Removal

> **⚠️ HISTORICAL DOCUMENT**: This migration guide describes a refactoring that has **already been completed** in the OutboxRelay codebase. The current version uses immutable event log architecture **without** state machine columns. This document is preserved for historical reference and to document the architectural decisions.
>
> **If you are installing OutboxRelay for the first time**, you do **NOT** need this migration guide. Simply follow the installation instructions in the main [README.md](../README.md).
>
> **If you are upgrading from a pre-1.0 version** that had state machine columns, this guide documents the changes that were made.

---

## Original Purpose

Step-by-step guide that was used to migrate from state-based to immutable event architecture.

**Impact**: Breaking change that required schema migrations and code updates.

**Estimated downtime**: 5-15 minutes (depending on table size)

---

## What Changed

### Before (Old Architecture - Removed)
```ruby
create_table :outbox_events do |t|
  t.string :state              # ❌ REMOVED
  t.integer :retry_count       # ❌ REMOVED
  t.text :error_message        # ❌ REMOVED (moved to dead_letter_events)
  t.timestamp :last_error_at   # ❌ REMOVED
  t.integer :lock_version      # ❌ REMOVED (replaced by advisory locks)
  # ...
end
```

### After (Current Architecture)
```ruby
create_table :outbox_events do |t|
  # Event identity and data only - no state machine
  t.bigint :sequence, null: false
  t.string :topic, null: false
  t.string :event_name
  t.jsonb :payload
  t.jsonb :headers
  t.integer :partition_key
  t.timestamp :created_at
  t.timestamp :expires_at
end
```

---

## Historical Migration Instructions

The following sections describe the original migration process:

---

## Table of Contents

1. [Pre-Migration Checklist](#pre-migration-checklist)
2. [Migration Steps](#migration-steps)
3. [Rollback Plan](#rollback-plan)
4. [Post-Migration Validation](#post-migration-validation)
5. [Troubleshooting](#troubleshooting)

---

## Pre-Migration Checklist

### 1. Understand Current State

```bash
# Check table sizes
psql -d your_database -c "
  SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
  FROM pg_tables
  WHERE tablename IN ('outbox_events', 'consumer_offsets', 'dead_letter_events')
  ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
"

# Check event counts by state
psql -d your_database -c "
  SELECT state, COUNT(*)
  FROM outbox_events
  GROUP BY state;
"

# Check consumer offsets
psql -d your_database -c "
  SELECT consumer_group, topic, last_consumed_sequence, heartbeat_at
  FROM consumer_offsets
  ORDER BY consumer_group, topic;
"
```

### 2. Backup Database

```bash
# Full database backup
pg_dump your_database > backup_before_migration_$(date +%Y%m%d_%H%M%S).sql

# Or just the relevant tables
pg_dump your_database \
  -t outbox_events \
  -t consumer_offsets \
  -t dead_letter_events \
  > outbox_backup_$(date +%Y%m%d_%H%M%S).sql
```

### 3. Stop Workers

```bash
# Stop all OutboxRelay workers
bundle exec rake outbox_relay:stop

# Verify no workers running
ps aux | grep outbox_relay

# Check no active heartbeats
psql -d your_database -c "
  SELECT * FROM consumer_offsets
  WHERE heartbeat_at > NOW() - INTERVAL '5 minutes';
"
```

**Expected result:** No active consumers.

### 4. Review Code Changes

**Files that will be modified:**
- `lib/outbox_relay/models/outbox_event.rb` - Remove state machine
- `lib/outbox_relay/models/outbox_consumer.rb` - Add advisory locks, update fetch_batch
- `lib/outbox_relay/models/dead_letter_event.rb` - Add "retrying" status
- Database migrations (2 new migrations)

**Git diff preview:**
```bash
git diff main migration-branch -- lib/outbox_relay/models/
```

---

## Migration Steps

### Step 1: Deploy Code (Without Migrations)

**Goal**: Deploy new code that's backward-compatible with old schema.

```bash
# 1. Checkout branch with code changes
git checkout migration-branch

# 2. Bundle install (no new gems required)
bundle install

# 3. Deploy code WITHOUT running migrations yet
# This allows for graceful transition
git push production main
```

**Critical:** Ensure code gracefully handles both old and new schema:

```ruby
# lib/outbox_relay/models/outbox_event.rb
def mark_as_consumed!
  # No-op if state column doesn't exist
  update!(state: "consumed") if respond_to?(:state)
end
```

### Step 2: Run Migrations

**Migration 1: Remove state machine columns**

```ruby
# db/migrate/YYYYMMDDHHMMSS_remove_state_machine_from_outbox_events.rb
class RemoveStateMachineFromOutboxEvents < ActiveRecord::Migration[7.0]
  def up
    # Remove columns (data loss is OK - we're redesigning)
    remove_column :outbox_events, :state, if_exists: true
    remove_column :outbox_events, :retry_count, if_exists: true
    remove_column :outbox_events, :error_message, if_exists: true
    remove_column :outbox_events, :last_error_at, if_exists: true
    remove_column :outbox_events, :lock_version, if_exists: true
  end

  def down
    # Rollback: restore columns
    add_column :outbox_events, :state, :string, default: "pending", if_not_exists: true
    add_column :outbox_events, :retry_count, :integer, default: 0, if_not_exists: true
    add_column :outbox_events, :error_message, :text, if_not_exists: true
    add_column :outbox_events, :last_error_at, :timestamp, if_not_exists: true
    add_column :outbox_events, :lock_version, :integer, default: 0, if_not_exists: true
  end
end
```

**Migration 2: Update dead_letter_events**

```ruby
# db/migrate/YYYYMMDDHHMMSS_add_retrying_status_to_dead_letter_events.rb
class AddRetryingStatusToDeadLetterEvents < ActiveRecord::Migration[7.0]
  def up
    # Remove old constraint if exists
    execute <<-SQL
      ALTER TABLE dead_letter_events
      DROP CONSTRAINT IF EXISTS check_resolution_status;
    SQL

    # Add new constraint with "retrying"
    execute <<-SQL
      ALTER TABLE dead_letter_events
      ADD CONSTRAINT check_resolution_status
      CHECK (resolution_status IN ('retrying', 'unresolved', 'resolved', 'reprocessed', 'ignored'));
    SQL
  end

  def down
    # Remove constraint
    execute <<-SQL
      ALTER TABLE dead_letter_events
      DROP CONSTRAINT IF EXISTS check_resolution_status;
    SQL

    # Restore old constraint without "retrying"
    execute <<-SQL
      ALTER TABLE dead_letter_events
      ADD CONSTRAINT check_resolution_status
      CHECK (resolution_status IN ('unresolved', 'resolved', 'reprocessed', 'ignored'));
    SQL
  end
end
```

**Run migrations:**

```bash
# Production
RAILS_ENV=production bundle exec rake db:migrate

# Check migration status
RAILS_ENV=production bundle exec rake db:migrate:status | grep outbox
```

**Expected output:**
```
up     YYYYMMDDHHMMSS  Remove state machine from outbox events
up     YYYYMMDDHHMMSS  Add retrying status to dead letter events
```

### Step 3: Verify Schema

```bash
# Check outbox_events schema
psql -d your_database -c "\d outbox_events"
```

**Expected columns:**
```
 Column        | Type      | Nullable | Default
---------------+-----------+----------+---------
 id            | bigint    | not null |
 sequence      | bigint    | not null |
 topic         | varchar   | not null |
 event_name    | varchar   | not null |
 event_id      | uuid      |          |
 payload       | jsonb     |          |
 headers       | jsonb     |          |
 partition_key | integer   | not null | 0
 created_at    | timestamp | not null |
 expires_at    | timestamp |          |
```

**Missing (removed):**
- ❌ `state`
- ❌ `retry_count`
- ❌ `error_message`
- ❌ `last_error_at`
- ❌ `lock_version`

### Step 4: Data Cleanup (Optional)

If you had events in terminal states, they're now just events:

```sql
-- Count events that were in "consumed" or "dead_letter" states
-- (This query won't work after migration - just for documentation)
-- SELECT state, COUNT(*) FROM outbox_events GROUP BY state;

-- After migration, all events are equal
-- Cleanup old events based on created_at instead:
DELETE FROM outbox_events WHERE created_at < NOW() - INTERVAL '7 days';
```

### Step 5: Restart Workers

```bash
# Start OutboxRelay workers with new code
bundle exec rake outbox_relay:start

# Or via systemd/docker/kubernetes
systemctl restart outbox_relay
# docker-compose restart outbox_relay
# kubectl rollout restart deployment/outbox-relay
```

### Step 6: Monitor Startup

```bash
# Watch logs
tail -f log/production.log | grep outbox_relay

# Check consumer offsets are updating
watch -n 1 'psql -d your_database -c "
  SELECT
    consumer_group,
    topic,
    last_consumed_sequence,
    NOW() - heartbeat_at AS seconds_ago
  FROM consumer_offsets
  ORDER BY consumer_group, topic;
"'
```

**Expected behavior:**
- Heartbeats updating every few seconds
- `last_consumed_sequence` incrementing
- No errors in logs

---

## Rollback Plan

### If Issues Occur Within 1 Hour

**Step 1: Stop new workers**
```bash
bundle exec rake outbox_relay:stop
```

**Step 2: Rollback database**
```bash
# Rollback last 2 migrations
RAILS_ENV=production bundle exec rake db:rollback STEP=2

# Verify schema
psql -d your_database -c "\d outbox_events"
# Should see: state, retry_count, etc. columns restored
```

**Step 3: Rollback code**
```bash
# Deploy previous version
git checkout main
git reset --hard origin/main~1  # Previous commit
git push production main --force

# Or use your deployment tool's rollback
# heroku rollback
# kubectl rollout undo deployment/outbox-relay
```

**Step 4: Restore data (if needed)**
```bash
# Restore from backup
psql -d your_database < backup_before_migration_YYYYMMDD_HHMMSS.sql
```

**Step 5: Restart old workers**
```bash
bundle exec rake outbox_relay:start
```

### If Issues Occur After > 1 Hour

**Don't rollback!** Too much new data. Instead, fix forward:

1. Identify specific issue
2. Deploy hotfix
3. Monitor carefully

---

## Post-Migration Validation

### 1. Functional Tests

**Test multi-consumer groups:**

```ruby
# Rails console
event = OutboxRelay::OutboxEvent.create!(
  topic: "test_topic",
  event_name: "test_event",
  payload: { test: true },
  partition_key: 0
)

# Check if multiple consumer groups can fetch it
consumer_a = TestConsumerA.new(partition_key: 0)
consumer_b = TestConsumerB.new(partition_key: 0)

consumer_a.fetch_batch(1)  # Should include event
consumer_b.fetch_batch(1)  # Should also include event ✅
```

**Test advisory locks:**

```ruby
# Start two workers in same consumer group
# They should NOT process same event twice

worker_a = TestConsumerA.new(partition_key: 0)
worker_b = TestConsumerA.new(partition_key: 0)

# Process same event in parallel
threads = [
  Thread.new { worker_a.process_event(event) },
  Thread.new { worker_b.process_event(event) }
]
threads.each(&:join)

# Only one should have processed it
ConsumerOffset.find_by(
  consumer_group: "test_consumer_a",
  topic: "test_topic"
).last_consumed_sequence  # Should be event.sequence (not 2x!)
```

**Test DLQ:**

```ruby
# Create failing consumer
class FailingConsumer < OutboxRelay::OutboxConsumer
  def consume_message(event)
    raise "Test error"
  end
end

consumer = FailingConsumer.new(partition_key: 0)
4.times { consumer.consume_batch }  # Fail 4 times

# Check DLQ
dlq = DeadLetterEvent.last
dlq.consumer_group  # => "failing_consumer"
dlq.resolution_status  # => "unresolved" (after max_retries)
```

### 2. Performance Tests

```bash
# Create 10k test events
psql -d your_database -c "
  INSERT INTO outbox_events (sequence, topic, event_name, payload, partition_key, created_at)
  SELECT
    generate_series(1, 10000),
    'perf_test',
    'test_event',
    '{\"test\": true}'::jsonb,
    0,
    NOW();
"

# Time processing
time bundle exec rails runner "
  consumer = PerfTestConsumer.new(partition_key: 0)
  consumer.consume_all
"
```

**Expected:** Similar performance to old implementation (~1000-2000 events/second per worker).

### 3. Monitoring Checks

```bash
# Check consumer lag
bundle exec rake outbox_relay:lag

# Check DLQ size
psql -d your_database -c "
  SELECT consumer_group, COUNT(*)
  FROM dead_letter_events
  WHERE resolution_status = 'unresolved'
  GROUP BY consumer_group;
"

# Check advisory locks (should be empty when idle)
psql -d your_database -c "
  SELECT * FROM pg_locks WHERE locktype = 'advisory';
"
```

---

## Troubleshooting

### Issue 1: "Column 'state' does not exist"

**Symptom:**
```
PG::UndefinedColumn: ERROR: column "state" does not exist
```

**Cause:** Code referencing old `state` column after migration.

**Solution:**
```bash
# Find all references
grep -r "\.state" app/consumers/ app/models/

# Remove or update references
# Old: event.state == "pending"
# New: true (all events are "pending" by default)
```

### Issue 2: Events Not Being Processed

**Symptom:** Consumer offset not advancing.

**Diagnosis:**
```bash
# Check if workers are running
ps aux | grep outbox_relay

# Check heartbeats
psql -d your_database -c "
  SELECT * FROM consumer_offsets
  WHERE heartbeat_at > NOW() - INTERVAL '5 minutes';
"

# Check if events exist
psql -d your_database -c "
  SELECT COUNT(*) FROM outbox_events WHERE created_at > NOW() - INTERVAL '1 hour';
"
```

**Solutions:**
- Restart workers if not running
- Check logs for errors
- Verify consumer configuration

### Issue 3: High DLQ Growth

**Symptom:** Many events going to DLQ immediately after migration.

**Diagnosis:**
```sql
SELECT
  consumer_group,
  error_message,
  COUNT(*)
FROM dead_letter_events
WHERE created_at > NOW() - INTERVAL '1 hour'
GROUP BY consumer_group, error_message
ORDER BY COUNT(*) DESC;
```

**Common causes:**
- Consumer expecting old state machine
- External service down
- Schema mismatch in consumer logic

**Solution:** Fix consumer code or external dependencies.

### Issue 4: Advisory Lock Contention

**Symptom:** Logs showing many "event_locked_by_another_worker" messages.

**Diagnosis:**
```sql
SELECT
  objid AS lock_key,
  COUNT(*) AS waiters
FROM pg_locks
WHERE locktype = 'advisory' AND NOT granted
GROUP BY objid
ORDER BY COUNT(*) DESC;
```

**Solution:**
- Check if too many workers for partition count
- Increase partition count
- Verify lock key generation logic

### Issue 5: Performance Degradation

**Symptom:** Processing slower after migration.

**Diagnosis:**
```bash
# Check query performance
EXPLAIN ANALYZE
SELECT * FROM outbox_events
WHERE topic = 'orders'
  AND partition_key = 0
  AND sequence > 1000
ORDER BY sequence
LIMIT 50;
```

**Solution:**
- Verify indexes exist (especially on topic, partition_key, sequence)
- Check DLQ subquery performance
- Consider materialized view for DLQ exclusion if very large

---

## Success Criteria

✅ **Migration successful if:**

1. All workers show active heartbeats
2. Consumer offsets advancing
3. Multiple consumer groups processing same events
4. No duplicate processing within same consumer group
5. DLQ tracking failures per consumer group
6. Performance within 20% of old implementation
7. No unexpected errors in logs for 24 hours

---

## Timeline

**Recommended schedule:**

- **T-7 days**: Review plan with team, schedule maintenance window
- **T-3 days**: Test migration in staging environment
- **T-1 day**: Backup database, prepare rollback scripts
- **T+0 hours**: Execute migration during low-traffic period
- **T+1 hour**: Validate and monitor
- **T+24 hours**: Final validation, declare success

**Maintenance window:** 5-15 minutes (workers stopped)

---

## Support

**If you encounter issues:**

1. Check logs: `tail -f log/production.log | grep outbox_relay`
2. Check this guide's [Troubleshooting](#troubleshooting) section
3. Review [ARCHITECTURE_DECISION.md](./ARCHITECTURE_DECISION.md) for design rationale
4. Open GitHub issue with:
   - Error messages
   - Database schema (`\d outbox_events`)
   - Worker logs
   - Consumer offset state

---

**Final Note:** This is a significant architectural change. Take your time, test thoroughly in staging, and have rollback plan ready. The benefits (correct multi-consumer support) far outweigh the migration effort.
