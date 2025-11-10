# Retry Logic: Per-Consumer Group Failures

**Purpose**: Handle consumer failures with retry logic tracked per consumer group in the dead letter events table.

---

## Table of Contents

1. [Core Concept](#core-concept)
2. [Why Per-Consumer Retries](#why-per-consumer-retries)
3. [Resolution Status Lifecycle](#resolution-status-lifecycle)
4. [Implementation](#implementation)
5. [Configuration](#configuration)
6. [Manual Intervention](#manual-intervention)

---

## Core Concept

### The Problem

**Events don't fail. Consumers fail.**

Event #100 is just data: `{ order_id: 123, action: "created" }`.

What fails is the **consumer's attempt to process** that event:

```ruby
Consumer Group "notifications":
  consume_message(event #100)
  → EmailService.send_email(...)
  → ✅ SUCCESS

Consumer Group "analytics":
  consume_message(event #100)
  → AnalyticsAPI.track_event(...)
  → ❌ FAILURE: "HTTP 503 Service Unavailable"
```

**Key insight:** The failure belongs to the consumer group, not the event.

### The Solution

Track failures in `dead_letter_events` with `consumer_group` as part of the key:

```ruby
dead_letter_events:
  {
    outbox_event_id: 100,
    consumer_group: "analytics",  # ← Failure is scoped to this group
    total_retries: 1,
    error_message: "HTTP 503 Service Unavailable",
    resolution_status: "retrying"
  }
```

**Result:**
- Event #100 remains in `outbox_events` (immutable)
- "notifications" consumer group: continues processing ✅
- "analytics" consumer group: retries event #100 ⏳

---

## Why Per-Consumer Retries

### Scenario: External Service Outage

```ruby
# Consumer Group "analytics" depends on external API
class AnalyticsConsumer < OutboxRelay::OutboxConsumer
  def consume_message(event)
    AnalyticsAPI.track_event(event.payload)  # ← External HTTP call
  end
end

# External API goes down
AnalyticsAPI.track_event(...)  # → raises "Connection refused"
```

**With per-event retries (WRONG):**
```ruby
# Event gets retry_count incremented
event.retry_count += 1

# After 3 retries, event marked as "dead_letter"
event.state = "dead_letter"

# Now NO consumer group can process it! ❌
# "notifications" is blocked by analytics' failure
```

**With per-consumer retries (CORRECT):**
```ruby
# Create DLQ entry for THIS consumer group
DeadLetterEvent.create!(
  outbox_event_id: event.id,
  consumer_group: "analytics",
  total_retries: 1,
  resolution_status: "retrying"
)

# Event remains available
# "notifications" continues processing ✅
# "analytics" retries automatically ⏳
```

---

## Resolution Status Lifecycle

### Status Values

```ruby
RESOLUTION_STATUSES = [
  "retrying",     # Actively retrying (total_retries < max_retries)
  "unresolved",   # Gave up after max retries (needs manual intervention)
  "resolved",     # Manually marked as fixed
  "reprocessed",  # Manually re-published and processed successfully
  "ignored"       # Manually marked as not worth fixing
]
```

### State Transitions

```
[No DLQ entry]
      ↓
   (first failure)
      ↓
 "retrying" ←─────────┐
      ↓               │
   (retry)            │
      ↓               │
   (fails again)      │
      ├───────────────┘
      ↓
   (total_retries >= max_retries)
      ↓
 "unresolved" ────────┬──→ "resolved" (manual fix + mark)
                      ├──→ "reprocessed" (manual republish)
                      └──→ "ignored" (not worth fixing)
```

### Example Flow

```ruby
# Attempt 1: Create DLQ entry
event_id: 100, consumer_group: "analytics"
total_retries: 1, resolution_status: "retrying"

# Attempt 2: Increment retries
total_retries: 2, resolution_status: "retrying"

# Attempt 3 (final): Increment retries
total_retries: 3, resolution_status: "retrying"

# Attempt 4: Give up
total_retries: 3, resolution_status: "unresolved"

# Now skipped in fetch_batch query
# Requires manual intervention
```

---

## Implementation

### 1. Failure Detection

```ruby
def process_event(event)
  lock_key = advisory_lock_key(event)

  ActiveRecord::Base.transaction do
    locked = acquire_advisory_lock(lock_key)
    return false unless locked

    # Try to process
    consume_message(event)  # ← User's business logic
    update_offset(event)

    true
  end
rescue StandardError => e
  # Failure detected!
  handle_failure(event, e)
  raise  # Re-raise for logging
end
```

### 2. Failure Handling

```ruby
def handle_failure(event, error)
  # Find or create DLQ entry for THIS consumer group
  dlq = DeadLetterEvent.find_or_initialize_by(
    outbox_event_id: event.id,
    consumer_group: consumer_group
  )

  if dlq.new_record?
    # First failure
    dlq.assign_attributes(
      consumer_class: self.class.name,
      original_topic: event.topic,
      original_event_name: event.event_name,
      original_payload: event.payload,
      original_headers: event.headers,
      total_retries: 1,
      error_message: error.message,
      error_backtrace: error.backtrace&.first(20)&.join("\n"),
      error_context: build_error_context(event, error),
      resolution_status: "retrying"
    )
    dlq.save!
  else
    # Subsequent failure
    dlq.with_lock do
      dlq.increment!(:total_retries)
      dlq.update!(
        error_message: error.message,
        error_backtrace: error.backtrace&.first(20)&.join("\n"),
        updated_at: Time.current
      )

      # Check if we should give up
      if dlq.total_retries >= max_retries
        dlq.update!(resolution_status: "unresolved")
      end
    end
  end

  # Log failure
  logger.error(
    event_name: "event_processing_failed",
    event_id: event.event_id,
    consumer_group: consumer_group,
    total_retries: dlq.total_retries,
    error: error.message
  )

  # Report to Sentry if available
  report_to_sentry(event, error, dlq) if defined?(Sentry)
end
```

### 3. Retry Filtering

Events with "unresolved" status are excluded from fetch:

```ruby
def fetch_batch(batch_size)
  OutboxEvent
    .where(topic: topic, partition_key: partition_key)
    .where("sequence > ?", current_offset.last_consumed_sequence)
    # Exclude events we've given up on
    .where.not(
      id: DeadLetterEvent
        .where(consumer_group: consumer_group)
        .where(resolution_status: "unresolved")
        .select(:outbox_event_id)
    )
    .order(:sequence)
    .limit(batch_size)
end
```

**Events with "retrying" status are NOT excluded** - they'll be retried automatically!

### 4. Automatic Retry Delay

**Optional:** Add exponential backoff:

```ruby
def should_retry_now?(dlq)
  return true if dlq.resolution_status != "retrying"

  # Exponential backoff: 1s, 2s, 4s, 8s, ...
  delay = [2 ** dlq.total_retries, 60].min.seconds

  Time.current > (dlq.updated_at + delay)
end

def fetch_batch(batch_size)
  OutboxEvent
    .where(...)
    .where.not(
      id: DeadLetterEvent
        .where(consumer_group: consumer_group)
        .where(resolution_status: "unresolved")
        .or(
          DeadLetterEvent.where(
            consumer_group: consumer_group,
            resolution_status: "retrying"
          ).where("updated_at > ?", lambda {
            # Custom SQL for exponential backoff check
            "NOW() - INTERVAL '1 second' * POWER(2, total_retries)"
          })
        )
        .select(:outbox_event_id)
    )
    .order(:sequence)
    .limit(batch_size)
end
```

**Note:** This adds complexity. For most cases, immediate retry is fine.

---

## Configuration

### Per-Consumer Max Retries

```ruby
class NotificationsConsumer < OutboxRelay::OutboxConsumer
  def initialize(partition_key:)
    super(
      consumer_group: "notifications",
      topic: "order_events",
      partition_key: partition_key,
      dead_letter_config: {
        max_retries: 5  # ← Custom retry limit
      }
    )
  end
end

class AnalyticsConsumer < OutboxRelay::OutboxConsumer
  def initialize(partition_key:)
    super(
      consumer_group: "analytics",
      topic: "order_events",
      partition_key: partition_key,
      dead_letter_config: {
        max_retries: 2  # ← Different limit!
      }
    )
  end
end
```

### Global Default

```ruby
# config/initializers/outbox_relay.rb
OutboxRelay.configure do |config|
  config.default_max_retries = 3  # Default for all consumers
end

# Consumer uses default if not specified
class AuditConsumer < OutboxRelay::OutboxConsumer
  def initialize(partition_key:)
    super(
      consumer_group: "audit",
      topic: "order_events",
      partition_key: partition_key
      # No dead_letter_config → uses default max_retries: 3
    )
  end
end
```

### Custom Error Handling

```ruby
class NotificationsConsumer < OutboxRelay::OutboxConsumer
  def consume_message(event)
    send_notification(event)
  rescue EmailService::PermanentError => e
    # Don't retry permanent errors - mark as unresolved immediately
    raise UnretryableError.new(e.message)
  rescue EmailService::TemporaryError => e
    # Retry temporary errors normally
    raise e
  end
end

# In base consumer:
def handle_failure(event, error)
  if error.is_a?(UnretryableError)
    # Skip retries, go straight to unresolved
    dlq = create_or_find_dlq(event)
    dlq.update!(resolution_status: "unresolved")
  else
    # Normal retry logic
    handle_retryable_failure(event, error)
  end
end
```

---

## Manual Intervention

### Inspecting DLQ

```ruby
# View unresolved events for a consumer group
dlq_events = DeadLetterEvent
  .where(consumer_group: "analytics")
  .where(resolution_status: "unresolved")
  .order(created_at: :desc)

dlq_events.each do |dlq|
  puts "Event ID: #{dlq.outbox_event_id}"
  puts "Total retries: #{dlq.total_retries}"
  puts "Error: #{dlq.error_message}"
  puts "Payload: #{dlq.original_payload}"
  puts "---"
end
```

### Resolving DLQ Events

**Option 1: Mark as resolved (fixed upstream)**

```ruby
# Fixed the root cause (e.g., external service is back up)
# Mark event as resolved without reprocessing
dlq = DeadLetterEvent.find(123)
dlq.mark_as_resolved!(notes: "External API is back online, but event no longer relevant")
```

**Option 2: Reprocess event**

```ruby
# Republish event to be processed again
dlq = DeadLetterEvent.find(123)

OutboxEvent.create!(
  topic: dlq.original_topic,
  event_name: dlq.original_event_name,
  payload: dlq.original_payload,
  headers: dlq.original_headers,
  partition_key: 0  # or calculate from headers
)

dlq.mark_as_reprocessed!(notes: "Republished for reprocessing")
```

**Option 3: Mark as ignored**

```ruby
# Event is not worth fixing (e.g., test data, old event)
dlq = DeadLetterEvent.find(123)
dlq.mark_as_ignored!(notes: "Test event, not production data")
```

### Bulk Operations

```ruby
# Mark all DLQ events for a specific error as ignored
DeadLetterEvent
  .where(consumer_group: "analytics")
  .where(resolution_status: "unresolved")
  .where("error_message LIKE ?", "%test environment%")
  .each do |dlq|
    dlq.mark_as_ignored!(notes: "Test environment error, not production")
  end

# Reprocess all unresolved events for a consumer group
DeadLetterEvent
  .where(consumer_group: "analytics")
  .where(resolution_status: "unresolved")
  .where("created_at > ?", 1.day.ago)
  .each do |dlq|
    OutboxEvent.create!(
      topic: dlq.original_topic,
      event_name: dlq.original_event_name,
      payload: dlq.original_payload,
      headers: dlq.original_headers
    )
    dlq.mark_as_reprocessed!
  end
```

### Rake Tasks

```ruby
# lib/tasks/outbox_relay_dlq.rake
namespace :outbox_relay do
  desc "Show DLQ summary"
  task dlq_summary: :environment do
    DeadLetterEvent
      .where(resolution_status: "unresolved")
      .group(:consumer_group)
      .count
      .each do |group, count|
        puts "#{group}: #{count} unresolved events"
      end
  end

  desc "Show DLQ details for consumer group"
  task :dlq_details, [:consumer_group] => :environment do |t, args|
    dlq_events = DeadLetterEvent
      .where(consumer_group: args[:consumer_group])
      .where(resolution_status: "unresolved")
      .order(created_at: :desc)
      .limit(10)

    dlq_events.each do |dlq|
      puts "=" * 80
      puts "Event ID: #{dlq.outbox_event_id}"
      puts "Total retries: #{dlq.total_retries}"
      puts "Created at: #{dlq.created_at}"
      puts "Error: #{dlq.error_message}"
      puts "Payload: #{dlq.original_payload.to_json}"
      puts
    end
  end

  desc "Reprocess DLQ event"
  task :dlq_reprocess, [:dlq_id] => :environment do |t, args|
    dlq = DeadLetterEvent.find(args[:dlq_id])

    OutboxEvent.create!(
      topic: dlq.original_topic,
      event_name: dlq.original_event_name,
      payload: dlq.original_payload,
      headers: dlq.original_headers
    )

    dlq.mark_as_reprocessed!(notes: "Manually reprocessed via rake task")
    puts "Event reprocessed successfully"
  end
end
```

**Usage:**
```bash
bundle exec rake outbox_relay:dlq_summary
bundle exec rake outbox_relay:dlq_details[analytics]
bundle exec rake outbox_relay:dlq_reprocess[123]
```

---

## Monitoring

### DLQ Alerts

```ruby
# config/initializers/dlq_monitoring.rb
class DLQMonitor
  def self.check_and_alert
    # Alert on high DLQ size
    DeadLetterEvent
      .where(resolution_status: "unresolved")
      .group(:consumer_group)
      .count
      .each do |group, count|
        if count > 1000
          alert("High DLQ size for #{group}: #{count} unresolved events")
        end
      end

    # Alert on rapidly growing DLQ
    recent_count = DeadLetterEvent
      .where(resolution_status: "unresolved")
      .where("created_at > ?", 1.hour.ago)
      .count

    if recent_count > 100
      alert("DLQ growing rapidly: #{recent_count} failures in last hour")
    end

    # Alert on specific error patterns
    common_errors = DeadLetterEvent
      .where(resolution_status: "unresolved")
      .where("created_at > ?", 1.hour.ago)
      .group(:error_message)
      .count
      .sort_by { |_, count| -count }
      .first(5)

    common_errors.each do |error, count|
      if count > 50
        alert("Frequent error (#{count} times): #{error.truncate(100)}")
      end
    end
  end

  def self.alert(message)
    # Send to Slack, PagerDuty, etc.
    Rails.logger.error("[DLQ ALERT] #{message}")
    # SlackNotifier.notify(message)
  end
end

# Schedule with cron/sidekiq-scheduler
# */5 * * * * cd /app && bundle exec rails runner "DLQMonitor.check_and_alert"
```

### Metrics to Track

1. **DLQ size per consumer group**
   ```ruby
   DeadLetterEvent
     .where(resolution_status: "unresolved")
     .group(:consumer_group)
     .count
   ```

2. **DLQ growth rate**
   ```ruby
   DeadLetterEvent
     .where(resolution_status: "unresolved")
     .where("created_at > ?", 1.hour.ago)
     .count
   ```

3. **Common error types**
   ```ruby
   DeadLetterEvent
     .where(resolution_status: "unresolved")
     .group("LEFT(error_message, 100)")
     .count
     .sort_by { |_, count| -count }
     .first(10)
   ```

4. **Average retries before giving up**
   ```ruby
   DeadLetterEvent
     .where(resolution_status: "unresolved")
     .average(:total_retries)
   ```

---

## Summary

### Key Principles

1. **Failures are per-consumer**: Event doesn't fail, consumer does
2. **DLQ is per-consumer-group**: Same event can be in DLQ for one group but not another
3. **Automatic retries**: Events with "retrying" status are automatically retried
4. **Manual resolution**: Events with "unresolved" status need human intervention
5. **Full context**: DLQ stores complete snapshot for debugging

### Best Practices

✅ **Do:**
- Set reasonable max_retries (3-5 is typical)
- Monitor DLQ size and growth rate
- Investigate common error patterns
- Reprocess DLQ events after fixing root cause
- Use resolution notes for audit trail

❌ **Don't:**
- Set max_retries too low (< 2) - external services have transient errors
- Set max_retries too high (> 10) - wastes resources on permanent errors
- Ignore DLQ - unresolved events indicate real problems
- Reprocess without fixing root cause - will just fail again

### Comparison to Event-Level Retries

| Aspect | Event-Level (WRONG) | Per-Consumer (CORRECT) |
|--------|---------------------|------------------------|
| Retry count | Global for event | Per consumer group |
| Failure impact | Blocks all consumers | Isolated to one consumer |
| Retry logic | In `outbox_events` table | In `dead_letter_events` table |
| Monitoring | "Event #100 failed" | "Consumer 'analytics' failed on event #100" |
| Resolution | Fix event or mark dead | Fix consumer or reprocess for that group |

---

**Next:** See [MIGRATION_GUIDE.md](./MIGRATION_GUIDE.md) for step-by-step migration from old architecture.
