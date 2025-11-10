# Architecture Decision Record: Removing State Machine from OutboxRelay

**Status**: Implemented ✅
**Original Date**: 2025-11-03
**Implementation Date**: 2025-11-03
**Author**: Rafal & Claude
**Impact**: Breaking change - migration completed

> **Note**: This architecture has been **fully implemented**. The OutboxRelay codebase now uses immutable event log pattern without state machine columns. This document explains the "why" behind the current architecture.

---

## Context

OutboxRelay is a PostgreSQL-based message queue implementing the Transactional Outbox pattern, designed to replace Kafka for multi-consumer group scenarios. During implementation review, we discovered a critical bug that prevents multiple consumer groups from processing the same event.

## Problem Discovery

### The Bug

Current implementation marks events as "consumed" when ONE consumer group processes them:

```ruby
# outbox_consumer.rb:195
def process_event(event)
  ActiveRecord::Base.transaction do
    event.mark_as_processing!
    consume_message(event)
    event.mark_as_consumed!  # ❌ BUG: Blocks other consumer groups!
    update_offset(event)
  end
end
```

### Why This Breaks Multi-Consumer

**Scenario:**
```
Event #100 created with state="pending"

Consumer Group "notifications":
  1. Fetches event #100 (state="pending")
  2. Processes it successfully
  3. Marks as state="consumed"  ❌

Consumer Group "analytics":
  1. Tries to fetch new events
  2. Query: WHERE state="pending" AND sequence > offset
  3. Event #100 has state="consumed", so it's SKIPPED ❌
  4. Analytics never processes event #100!
```

### Root Cause Analysis

The fundamental issue: **conflating event lifecycle with consumption state**.

In a single-consumer system (like Sidekiq):
- Job is created → "pending"
- Job is processing → "processing"
- Job is done → "consumed"
- This makes sense - ONE worker, ONE lifecycle

In a multi-consumer system (like Kafka):
- Event is created → exists in log
- Consumer Group A processes it → Consumer Group A's offset advances
- Consumer Group B processes it → Consumer Group B's offset advances
- **Event itself has no "consumed" state** - it just exists

## The Deeper Realization

### States Are Per-Consumer, Not Per-Event

During discussion, we realized an even more fundamental problem:

**Event #100 can simultaneously be:**
- ✅ Successfully processed by Consumer Group "notifications"
- ❌ Failed 3 times by Consumer Group "analytics"
- ⏳ Not yet processed by Consumer Group "audit_log"

The question "What is the state of Event #100?" **has no meaningful answer**.

The correct questions are:
- "What is Consumer Group A's state relative to Event #100?" → processed
- "What is Consumer Group B's state relative to Event #100?" → failed
- "What is Consumer Group C's state relative to Event #100?" → not yet processed

### Events Are Immutable Facts

In Kafka's model (which we're trying to replicate):
- Events are **immutable facts** in a log
- Events don't have "state" - they just exist
- Consumer groups maintain their own position (offset) in the log
- Consumer groups maintain their own failure tracking (DLQ)

**Event log**: "These things happened"
**Consumer state**: "I've processed up to here, and I failed on these"

## Decision & Implementation

### Removed State Machine Entirely from outbox_events

**Old schema** (pre-1.0 - WRONG):
```ruby
create_table :outbox_events do |t|
  t.string :state              # ❌ Remove
  t.integer :retry_count       # ❌ Remove (retry is per-consumer)
  t.text :error_message        # ❌ Remove (already in dead_letter_events)
  t.timestamp :last_error_at   # ❌ Remove (already in dead_letter_events)
  t.integer :lock_version      # ❌ Remove (replaced by advisory locks)

  # Keep these:
  t.bigint :sequence           # ✅ Event identity
  t.string :topic              # ✅ Event identity
  t.string :event_name         # ✅ Event identity
  t.jsonb :payload             # ✅ Event data
  t.jsonb :headers             # ✅ Event metadata
  t.integer :partition_key     # ✅ Routing
  t.timestamp :created_at      # ✅ Lifecycle
  t.timestamp :expires_at      # ✅ TTL
end
```

**Current schema** (implemented - CORRECT):
```ruby
create_table :outbox_events do |t|
  # Event identity
  t.bigint :sequence, null: false
  t.string :topic, null: false
  t.string :event_name, null: false
  t.uuid :event_id

  # Event data
  t.jsonb :payload
  t.jsonb :headers

  # Routing
  t.integer :partition_key, null: false, default: 0

  # Lifecycle (TTL cleanup)
  t.timestamp :created_at, null: false
  t.timestamp :expires_at
end
```

**That's it.** Events are immutable facts. No state. No retry tracking. Just data.

## Implementation

### 1. Consumer Offsets Track Success

Each consumer group maintains its own offset:

```ruby
create_table :consumer_offsets do |t|
  t.string :consumer_group, null: false
  t.string :topic, null: false
  t.bigint :last_consumed_sequence, null: false
  t.uuid :last_consumed_event_id
  t.timestamp :last_consumed_at
  t.timestamp :heartbeat_at
end
```

**Interpretation:**
- If Consumer Group "notifications" has `last_consumed_sequence = 100`
- It means: "I successfully processed all events 1-100 for this topic"
- Next fetch will start at sequence 101

### 2. Dead Letter Events Track Failures

Each consumer group maintains its own DLQ:

```ruby
create_table :dead_letter_events do |t|
  t.references :outbox_event, foreign_key: true
  t.string :consumer_group, null: false  # ← Failures are per-consumer!
  t.integer :total_retries, null: false
  t.text :error_message
  t.text :error_backtrace
  t.jsonb :error_context

  # Resolution tracking
  t.string :resolution_status, default: "retrying"
  # Values: "retrying", "unresolved", "resolved", "reprocessed", "ignored"
end
```

**Interpretation:**
- If there's a DLQ entry: `(event_id: 100, consumer_group: "analytics", resolution_status: "retrying")`
- It means: "Consumer Group 'analytics' failed on event #100 and is retrying"
- Other consumer groups can still process event #100 successfully

### 3. Query Logic

**Fetch batch:**
```ruby
def fetch_batch
  OutboxEvent
    # Get events I haven't processed yet
    .where("sequence > ?", current_offset.last_consumed_sequence)

    # Exclude events I've already failed on
    .where.not(
      id: DeadLetterEvent
        .where(consumer_group: consumer_group)
        .where(resolution_status: ["retrying", "unresolved"])
        .select(:outbox_event_id)
    )

    .order(:sequence)
    .limit(batch_size)
end
```

**Translation:** "Give me events I haven't processed AND haven't given up on."

### 4. Advisory Locks Prevent Duplicates

Within the same consumer group, we need to prevent two workers from processing the same event simultaneously:

```ruby
def advisory_lock_key(event)
  # Combine event sequence + consumer group hash
  event_sequence = event.sequence & 0xFFFFFFFF  # 32 bits
  group_hash = Zlib.crc32(consumer_group) & 0xFFFFFFFF  # 32 bits
  (event_sequence << 32) | group_hash
end

def process_event(event)
  lock_key = advisory_lock_key(event)

  ActiveRecord::Base.transaction do
    # Try to acquire lock
    locked = connection.execute(
      "SELECT pg_try_advisory_xact_lock(#{lock_key})"
    ).first["pg_try_advisory_xact_lock"]

    return false unless locked  # Another worker in same group is processing

    # We have the lock - process event
    consume_message(event)
    update_offset(event.sequence)

    # Lock automatically released on COMMIT
  end
rescue StandardError => e
  handle_failure(event, e)
end
```

**Key insight:** Lock key includes BOTH event and consumer_group:
- Worker A (group "notifications") and Worker B (group "analytics") → **different lock keys** → can process in parallel ✅
- Worker A (group "notifications") and Worker C (group "notifications") → **same lock key** → only one can process ✅

### 5. Retry Logic

**On first failure:**
```ruby
def handle_failure(event, error)
  # Create DLQ entry for THIS consumer group
  DeadLetterEvent.create!(
    outbox_event: event,
    consumer_group: consumer_group,
    total_retries: 1,
    error_message: error.message,
    error_backtrace: error.backtrace.first(20).join("\n"),
    resolution_status: "retrying"
  )
end
```

**On subsequent failures:**
```ruby
def handle_failure(event, error)
  dlq = DeadLetterEvent.find_by(
    outbox_event: event,
    consumer_group: consumer_group
  )

  if dlq
    dlq.increment!(:total_retries)

    if dlq.total_retries >= max_retries
      # Give up
      dlq.update!(resolution_status: "unresolved")
    end
  else
    # First failure - create entry
    DeadLetterEvent.create!(...)
  end
end
```

**Important:** Event #100 remains in `outbox_events` table. It's just excluded from this consumer group's queries via DLQ join.

### 6. TTL Cleanup

Since events never transition to "consumed", we need TTL-based cleanup:

```ruby
# Rake task: outbox_relay:cleanup
desc "Clean up old events (default: 7 days)"
task cleanup: :environment do
  retention_days = ENV.fetch("RETENTION_DAYS", 7).to_i
  cutoff = retention_days.days.ago

  count = OutboxRelay::OutboxEvent
    .where("created_at < ?", cutoff)
    .delete_all

  puts "Deleted #{count} events older than #{retention_days} days"
end
```

**Recommendation:** Run daily via cron/scheduler.

## Comparison: Before vs After

### Before (BROKEN)

**Event lifecycle:**
```
pending → processing → consumed
  ↓
failed → dead_letter
```

**Problem:** Once "consumed", no other consumer group can see it.

### After (CORRECT)

**Event lifecycle:**
```
created → [exists in log forever] → deleted after TTL
```

**Consumer group states:**
```
Consumer Group A: offset=100 (processed 1-100)
Consumer Group B: offset=50, DLQ=[100,101] (processed 1-50, failed on 100-101)
Consumer Group C: offset=0 (hasn't started)
```

**Event #100:**
- Just exists in the log
- Group A: processed ✅
- Group B: in DLQ ❌
- Group C: ready to process ⏳

## Benefits

### 1. Correctness
- ✅ Multiple consumer groups can process same events
- ✅ No hidden state transitions blocking other consumers

### 2. Simplicity
- ✅ No complex state machine with 5 states and validation rules
- ✅ Events are simple: they exist or they don't
- ✅ Consumer state is explicit: offsets + DLQ

### 3. Kafka-like Mental Model
- ✅ Familiar for developers coming from Kafka
- ✅ Same semantics: immutable log + consumer offsets
- ✅ Clear separation: events (facts) vs consumer state (tracking)

### 4. Performance
- ✅ Fewer columns in main table → smaller row size
- ✅ Simpler queries → better query planner decisions
- ✅ No optimistic locking overhead → replaced by advisory locks

### 5. Observability
- ✅ Clear metrics: "events in log", "consumer lag", "DLQ size per group"
- ✅ No ambiguous "pending vs processing" states
- ✅ Easy to answer: "How far behind is Consumer Group X?"

## Migration Path

### Phase 1: Schema Changes
```ruby
# Migration 1: Remove state machine columns
class RemoveStateFromOutboxEvents < ActiveRecord::Migration[7.0]
  def change
    remove_column :outbox_events, :state
    remove_column :outbox_events, :retry_count
    remove_column :outbox_events, :error_message
    remove_column :outbox_events, :last_error_at
    remove_column :outbox_events, :lock_version
  end
end

# Migration 2: Update DLQ resolution_status enum
class UpdateDeadLetterEventStatuses < ActiveRecord::Migration[7.0]
  def up
    # Add "retrying" as a valid status
    execute <<-SQL
      ALTER TABLE dead_letter_events
      ADD CONSTRAINT check_resolution_status
      CHECK (resolution_status IN ('retrying', 'unresolved', 'resolved', 'reprocessed', 'ignored'))
    SQL
  end
end
```

### Phase 2: Code Changes
1. Remove all state machine methods from `OutboxEvent`
2. Implement advisory lock methods in `OutboxConsumer`
3. Update `fetch_batch` with DLQ exclusion join
4. Implement retry logic using `DeadLetterEvent`
5. Add TTL cleanup rake task

### Phase 3: Deployment
1. Deploy code changes
2. Run migrations
3. Monitor consumer offsets and DLQ
4. Verify multi-consumer group behavior

## Testing Strategy

### Critical Tests

1. **Multi-consumer scenario:**
   ```ruby
   it "allows multiple consumer groups to process same event" do
     event = create(:outbox_event, sequence: 100)

     group_a = NotificationsConsumer.new
     group_b = AnalyticsConsumer.new

     # Both process successfully
     expect { group_a.process_event(event) }.to change { group_a.current_offset.last_consumed_sequence }.to(100)
     expect { group_b.process_event(event) }.to change { group_b.current_offset.last_consumed_sequence }.to(100)

     # Event still exists
     expect(event.reload).to be_persisted
   end
   ```

2. **Per-consumer DLQ:**
   ```ruby
   it "tracks failures per consumer group" do
     event = create(:outbox_event, sequence: 100)

     group_a = NotificationsConsumer.new
     group_b = AnalyticsConsumer.new

     # Group A succeeds
     group_a.process_event(event)
     expect(group_a.current_offset.last_consumed_sequence).to eq(100)

     # Group B fails
     allow(group_b).to receive(:consume_message).and_raise("Error")
     3.times { group_b.process_event(event) }

     # Group B has DLQ entry
     dlq = DeadLetterEvent.find_by(outbox_event: event, consumer_group: group_b.consumer_group)
     expect(dlq.total_retries).to eq(3)
     expect(dlq.resolution_status).to eq("unresolved")

     # Group A can still process new events
     event_101 = create(:outbox_event, sequence: 101)
     expect { group_a.process_event(event_101) }.to change { group_a.current_offset.last_consumed_sequence }.to(101)
   end
   ```

3. **Advisory lock contention:**
   ```ruby
   it "prevents duplicate processing within same consumer group" do
     event = create(:outbox_event, sequence: 100)

     worker_a = NotificationsConsumer.new
     worker_b = NotificationsConsumer.new  # Same group!

     processed_count = Concurrent::AtomicFixnum.new(0)

     threads = [
       Thread.new { worker_a.process_event(event); processed_count.increment },
       Thread.new { worker_b.process_event(event); processed_count.increment }
     ]
     threads.each(&:join)

     # Only ONE worker processed it
     expect(processed_count.value).to eq(1)
   end
   ```

## Risks and Mitigations

### Risk 1: Database Growth
**Issue:** Events never transition to "consumed", table grows indefinitely

**Mitigation:**
- Implement TTL cleanup (run daily)
- Monitor table size
- Set reasonable retention (7-30 days)
- Add alerts for unexpected growth

### Risk 2: DLQ Query Performance
**Issue:** Subquery to exclude DLQ events might be slow

**Mitigation:**
- Add index: `CREATE INDEX idx_dlq_event_group ON dead_letter_events(outbox_event_id, consumer_group)`
- Monitor query performance
- Consider materialized view if needed

### Risk 3: Advisory Lock Collisions
**Issue:** CRC32 hash collisions could cause false lock contention

**Mitigation:**
- Use 64-bit key space: 32 bits sequence + 32 bits group hash
- Collision probability extremely low (<< 1 in billion)
- Monitor lock wait times
- Can switch to MD5 hash if needed

## Alternatives Considered

### Alternative 1: Keep "consumed" state per consumer group
**Idea:** Add `consumed_by` jsonb column with array of consumer groups

**Rejected because:**
- Complex to query: `WHERE NOT consumed_by @> '["my_group"]'`
- Still couples event state to consumption
- Doesn't scale with many consumer groups
- Advisory locks are cleaner solution

### Alternative 2: Separate table per consumer group
**Idea:** Each consumer group has its own events table

**Rejected because:**
- Massive duplication
- Schema management nightmare
- Defeats purpose of single source of truth

### Alternative 3: Reference counting
**Idea:** Track how many consumer groups need to process event

**Rejected because:**
- Requires knowing all consumer groups upfront
- Doesn't handle dynamic consumer groups
- Complex lifecycle management

## Conclusion

Removing the state machine from `outbox_events` was the correct architectural decision. It aligns with Kafka's proven model of immutable event logs + consumer state tracking. The implementation is simpler, more correct, and more maintainable than the previous state machine approach.

**Key principle:** Events are immutable facts. Consumer groups maintain their own state relative to those facts.

---

## Implementation Status ✅

This architecture has been **fully implemented** in OutboxRelay:

1. ✅ Schema migrations completed - no state machine columns in current codebase
2. ✅ Advisory lock implementation using `pg_try_advisory_xact_lock`
3. ✅ Per-consumer DLQ with resolution statuses
4. ✅ Offset tracking per consumer group
5. ✅ TTL-based cleanup rake task
6. ✅ Comprehensive tests validating multi-consumer scenarios
7. ✅ Production deployment with fork-based workers

**Current State:**
- The migration templates in `lib/generators/outbox_relay/install/templates/` contain the immutable event schema
- `OutboxEvent` model has no state machine methods
- `OutboxConsumer` implements advisory locks for duplicate prevention
- `DeadLetterEvent` handles per-consumer-group failure tracking

**For New Users:**
- Simply run `rails generate outbox_relay:install` and you'll get the correct architecture
- No migration from old schema needed

**For Historical Context:**
- See [MIGRATION_GUIDE.md](./MIGRATION_GUIDE.md) for the original migration plan (historical document)

---

**References:**
- Kafka consumer groups: https://kafka.apache.org/documentation/#consumergroups
- PostgreSQL advisory locks: https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS
- Outbox pattern: https://microservices.io/patterns/data/transactional-outbox.html
