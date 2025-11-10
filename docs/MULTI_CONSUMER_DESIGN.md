# Multi-Consumer Group Design

**Purpose**: Enable multiple independent consumer groups to process the same events, similar to Kafka consumer groups.

---

## Table of Contents

1. [Kafka Comparison](#kafka-comparison)
2. [Architecture Overview](#architecture-overview)
3. [Consumer Group Semantics](#consumer-group-semantics)
4. [Query Strategy](#query-strategy)
5. [Example Scenarios](#example-scenarios)
6. [Monitoring and Observability](#monitoring-and-observability)

---

## Kafka Comparison

### Kafka's Model

In Kafka, events are stored in **immutable logs** (topics):

```
Topic: "order_events"
├─ Event #1: { order_id: 100, action: "created" }
├─ Event #2: { order_id: 101, action: "created" }
├─ Event #3: { order_id: 100, action: "updated" }
└─ ...

Consumer Group "notifications":
  └─ Offset: 2 (processed events 1-2)

Consumer Group "analytics":
  └─ Offset: 3 (processed events 1-3)

Consumer Group "audit_log":
  └─ Offset: 0 (hasn't started)
```

**Key properties:**
1. Events never change after creation (immutable)
2. Events don't have a "consumed" status
3. Each consumer group tracks its own offset
4. Multiple consumer groups can process the same events
5. Retention policy deletes old events (time-based TTL)

### OutboxRelay's Model

We replicate Kafka's semantics using PostgreSQL:

```
Table: outbox_events
├─ Event #1: { sequence: 1, topic: "order_events", payload: {...} }
├─ Event #2: { sequence: 2, topic: "order_events", payload: {...} }
├─ Event #3: { sequence: 3, topic: "order_events", payload: {...} }
└─ ...

Table: consumer_offsets
├─ { consumer_group: "notifications", topic: "order_events", last_consumed_sequence: 2 }
├─ { consumer_group: "analytics", topic: "order_events", last_consumed_sequence: 3 }
└─ { consumer_group: "audit_log", topic: "order_events", last_consumed_sequence: 0 }

Table: dead_letter_events
└─ { outbox_event_id: 5, consumer_group: "analytics", resolution_status: "unresolved" }
```

**Mapping:**
| Kafka | OutboxRelay |
|-------|-------------|
| Topic partition | `outbox_events` filtered by `topic` + `partition_key` |
| Consumer offset | `consumer_offsets.last_consumed_sequence` |
| Event retention | TTL-based cleanup (DELETE old events) |
| Consumer failure | `dead_letter_events` (per consumer group) |

---

## Architecture Overview

### Three-Table Design

```
┌─────────────────────────────────────┐
│      outbox_events                  │
│  (Immutable event log)              │
│                                     │
│  • sequence (unique, monotonic)     │
│  • topic                            │
│  • payload, headers                 │
│  • partition_key                    │
│  • created_at (for TTL)             │
│                                     │
│  NO state, NO retry_count!          │
└─────────────────────────────────────┘
              ↓ referenced by
    ┌──────────────────┬──────────────────┐
    ↓                  ↓                  ↓
┌─────────────┐  ┌──────────────┐  ┌──────────────┐
│  consumer_  │  │  consumer_   │  │ dead_letter_ │
│  offsets    │  │  offsets     │  │ events       │
│             │  │              │  │              │
│ Group A     │  │ Group B      │  │ Group B      │
│ offset: 100 │  │ offset: 50   │  │ event_id: 75 │
│             │  │              │  │ retries: 3   │
└─────────────┘  └──────────────┘  └──────────────┘

     ✅               ✅                ❌
  Processed        Processed         Failed
  1-100            1-50              on #75
```

### Data Flow

```
1. Event Creation
   └─> INSERT INTO outbox_events (sequence, topic, payload, ...)

2. Consumer Group Processing
   ├─> Worker fetches batch:
   │   └─> SELECT * FROM outbox_events
   │       WHERE sequence > my_offset
   │       AND NOT IN (my_dlq_events)
   │
   ├─> For each event:
   │   ├─> Try advisory lock (per consumer_group + event)
   │   ├─> If locked: consume_message()
   │   └─> Update offset: last_consumed_sequence = event.sequence
   │
   └─> On failure:
       └─> INSERT INTO dead_letter_events (consumer_group, event_id, ...)

3. Cleanup (daily cron)
   └─> DELETE FROM outbox_events WHERE created_at < 7.days.ago
```

---

## Consumer Group Semantics

### Independent Processing

Each consumer group is **completely independent**:

```ruby
# Consumer Group "notifications"
class NotificationsConsumer < OutboxRelay::OutboxConsumer
  def initialize(partition_key:)
    super(
      consumer_group: "notifications",
      topic: "order_events",
      partition_key: partition_key
    )
  end

  def consume_message(event)
    # Send email, push notification
    NotificationService.send_order_notification(event.payload)
  end
end

# Consumer Group "analytics"
class AnalyticsConsumer < OutboxRelay::OutboxConsumer
  def initialize(partition_key:)
    super(
      consumer_group: "analytics",
      topic: "order_events",
      partition_key: partition_key
    )
  end

  def consume_message(event)
    # Track in data warehouse
    AnalyticsTracker.track_order_event(event.payload)
  end
end
```

**Key point:** Same event #100 will be processed by BOTH consumers!

### Offset Management

Each consumer group maintains its own offset:

```ruby
# Consumer Group "notifications"
offset = ConsumerOffset.find_by(
  consumer_group: "notifications",
  topic: "order_events"
)
offset.last_consumed_sequence  # => 150

# Consumer Group "analytics"
offset = ConsumerOffset.find_by(
  consumer_group: "analytics",
  topic: "order_events"
)
offset.last_consumed_sequence  # => 50

# They're independent!
# Notifications is 100 events ahead of Analytics
```

### Failure Isolation

Failures are per-consumer-group:

```ruby
Event #75

Consumer Group "notifications":
  ✅ Processed successfully
  offset.last_consumed_sequence = 75

Consumer Group "analytics":
  ❌ Failed with "API timeout"
  DeadLetterEvent.create!(
    outbox_event_id: 75,
    consumer_group: "analytics",
    error_message: "API timeout",
    resolution_status: "retrying"
  )

Consumer Group "audit_log":
  ⏳ Not yet processed
  offset.last_consumed_sequence = 0
```

**Result:** Event #75 is:
- Successfully processed by "notifications" ✅
- In DLQ for "analytics" ❌
- Waiting to be processed by "audit_log" ⏳

---

## Query Strategy

### Fetch Batch Query

```ruby
def fetch_batch(batch_size: 50)
  # Get events that:
  # 1. I haven't processed yet (sequence > my_offset)
  # 2. I haven't given up on (not in my DLQ with "unresolved")
  # 3. Match my topic and partition

  OutboxEvent
    .where(topic: topic)
    .where(partition_key: partition_key)
    .where("sequence > ?", current_offset.last_consumed_sequence)
    .where.not(
      id: DeadLetterEvent
        .where(consumer_group: consumer_group)
        .where(resolution_status: ["unresolved"])
        .select(:outbox_event_id)
    )
    .order(:sequence)
    .limit(batch_size)
end
```

**SQL generated:**
```sql
SELECT *
FROM outbox_events
WHERE topic = 'order_events'
  AND partition_key = 0
  AND sequence > 50
  AND id NOT IN (
    SELECT outbox_event_id
    FROM dead_letter_events
    WHERE consumer_group = 'analytics'
      AND resolution_status = 'unresolved'
  )
ORDER BY sequence
LIMIT 50;
```

### Performance Optimization

**Index requirements:**
```sql
-- Primary fetch query
CREATE INDEX idx_outbox_events_topic_partition_sequence
  ON outbox_events(topic, partition_key, sequence);

-- DLQ exclusion subquery
CREATE INDEX idx_dlq_consumer_event
  ON dead_letter_events(consumer_group, resolution_status, outbox_event_id);

-- Offset lookup
CREATE UNIQUE INDEX idx_consumer_offsets_group_topic
  ON consumer_offsets(consumer_group, topic);
```

**Query plan:**
```
Limit (cost=0.29..8.31 rows=50 width=...)
  -> Index Scan using idx_outbox_events_topic_partition_sequence
       Index Cond: (topic = 'order_events' AND partition_key = 0 AND sequence > 50)
       Filter: NOT IN (SubPlan 1)
         SubPlan 1:
           -> Index Scan using idx_dlq_consumer_event
                Index Cond: (consumer_group = 'analytics' AND resolution_status = 'unresolved')
```

**Typical query time:** 1-5ms

---

## Example Scenarios

### Scenario 1: Three Consumer Groups, Fresh Start

**Setup:**
```ruby
# Create events
OutboxEvent.create!(sequence: 1, topic: "orders", payload: {order_id: 100})
OutboxEvent.create!(sequence: 2, topic: "orders", payload: {order_id: 101})
OutboxEvent.create!(sequence: 3, topic: "orders", payload: {order_id: 102})

# Three consumer groups
notifications = NotificationsConsumer.new(partition_key: 0)
analytics = AnalyticsConsumer.new(partition_key: 0)
audit = AuditLogConsumer.new(partition_key: 0)
```

**Initial state:**
```
outbox_events: [1, 2, 3]

consumer_offsets:
  notifications → 0
  analytics → 0
  audit → 0

dead_letter_events: []
```

**After each processes:**
```ruby
notifications.consume_batch  # Processes 1, 2, 3
analytics.consume_batch      # Processes 1, 2, 3
audit.consume_batch          # Processes 1, 2, 3
```

**Final state:**
```
outbox_events: [1, 2, 3]  # Still there!

consumer_offsets:
  notifications → 3  ✅
  analytics → 3      ✅
  audit → 3          ✅

dead_letter_events: []
```

### Scenario 2: One Consumer Group Fails

**Setup:**
```ruby
# Analytics consumer fails on event #2
allow(analytics).to receive(:consume_message) do |event|
  raise "Database error" if event.sequence == 2
  # Success for other events
end
```

**Processing:**
```ruby
notifications.consume_batch  # Processes 1, 2, 3 successfully
analytics.consume_batch      # Processes 1, fails on 2, processes 3
audit.consume_batch          # Processes 1, 2, 3 successfully
```

**State after first pass:**
```
outbox_events: [1, 2, 3]

consumer_offsets:
  notifications → 3  ✅
  analytics → 1      ⚠️  (stopped at failure)
  audit → 3          ✅

dead_letter_events:
  { event_id: 2, consumer_group: "analytics", total_retries: 1, resolution_status: "retrying" }
```

**Retry logic:**
```ruby
# Analytics retries automatically
analytics.consume_batch  # Retries event #2, fails again

dead_letter_events:
  { event_id: 2, consumer_group: "analytics", total_retries: 2, resolution_status: "retrying" }

# After max_retries (e.g., 3):
analytics.consume_batch  # Final retry, fails

dead_letter_events:
  { event_id: 2, consumer_group: "analytics", total_retries: 3, resolution_status: "unresolved" }

# Now event #2 is skipped for analytics
analytics.consume_batch  # Fetches events 3, 4, 5... (skips 2)

consumer_offsets:
  analytics → 5  (processed 1, skipped 2, processed 3-5)
```

### Scenario 3: Consumer Group Lag

**Setup:**
```ruby
# Analytics is slow, notifications is fast
1000.times do |i|
  OutboxEvent.create!(sequence: i + 1, topic: "orders", payload: {order_id: 1000 + i})
end

# Notifications processes quickly
notifications.consume_all  # Processes all 1000

# Analytics is slow
analytics.consume_batch(batch_size: 10)  # Processes 10 at a time
```

**State:**
```
consumer_offsets:
  notifications → 1000  ✅ (caught up)
  analytics → 10        ⚠️  (lagging)

Lag calculation:
  notifications.lag  # => 0 (latest_sequence - last_consumed = 1000 - 1000)
  analytics.lag      # => 990 (latest_sequence - last_consumed = 1000 - 10)
```

**Monitoring alert:**
```ruby
# Alert if consumer group is > 1000 events behind
ConsumerOffset.all.each do |offset|
  lag = offset.lag
  if lag > 1000
    alert("Consumer group #{offset.consumer_group} has high lag: #{lag} events")
  end
end
```

### Scenario 4: Adding New Consumer Group

**Setup:**
```ruby
# Events 1-1000 already exist
# notifications and analytics have processed all

# Add new consumer group: "fraud_detection"
fraud = FraudDetectionConsumer.new(partition_key: 0)
```

**Initial state:**
```
consumer_offsets:
  notifications → 1000
  analytics → 1000
  fraud → 0  (new!)
```

**Processing:**
```ruby
# Fraud detection starts from beginning
fraud.consume_batch(batch_size: 100)  # Processes events 1-100
fraud.consume_batch(batch_size: 100)  # Processes events 101-200
# ...eventually catches up
```

**Result:** New consumer group can process ALL historical events! 🎉

**Note:** This assumes events haven't been TTL-deleted yet. For very old events:
```ruby
# Only process events from last 7 days
fraud = FraudDetectionConsumer.new(partition_key: 0)
fraud.current_offset.update!(
  last_consumed_sequence: OutboxEvent.where("created_at < ?", 7.days.ago).maximum(:sequence) || 0
)
# Now starts from 7 days ago
```

---

## Monitoring and Observability

### Key Metrics

**1. Consumer Lag (per consumer group)**
```ruby
def consumer_lag(consumer_group, topic)
  offset = ConsumerOffset.find_by(consumer_group: consumer_group, topic: topic)
  latest_sequence = OutboxEvent.where(topic: topic).maximum(:sequence) || 0
  latest_sequence - offset.last_consumed_sequence
end

# Example:
consumer_lag("notifications", "orders")  # => 0 (caught up)
consumer_lag("analytics", "orders")      # => 500 (lagging)
```

**2. Processing Rate (events/second)**
```ruby
# Track in logs or metrics system
logger.info(
  event_name: "batch_processed",
  consumer_group: consumer_group,
  topic: topic,
  processed_count: processed_count,
  duration_ms: duration,
  rate: (processed_count / duration * 1000).round(2)
)
```

**3. DLQ Size (per consumer group)**
```ruby
def dlq_size(consumer_group)
  DeadLetterEvent
    .where(consumer_group: consumer_group)
    .where(resolution_status: "unresolved")
    .count
end

# Alert if DLQ grows too large
if dlq_size("analytics") > 100
  alert("Analytics DLQ has #{dlq_size('analytics')} unresolved events")
end
```

**4. Consumer Health**
```ruby
def consumer_healthy?(consumer_group, topic)
  offset = ConsumerOffset.find_by(consumer_group: consumer_group, topic: topic)

  # Check recent heartbeat
  return false if offset.heartbeat_at < 5.minutes.ago

  # Check lag is reasonable
  lag = consumer_lag(consumer_group, topic)
  return false if lag > 10_000

  # Check DLQ size
  dlq = dlq_size(consumer_group)
  return false if dlq > 1000

  true
end
```

### Dashboards

**Grafana/DataDog queries:**

```sql
-- Consumer lag over time
SELECT
  consumer_group,
  topic,
  (SELECT MAX(sequence) FROM outbox_events WHERE topic = co.topic) - last_consumed_sequence AS lag,
  NOW() AS time
FROM consumer_offsets co
ORDER BY lag DESC;

-- Processing rate (requires time-series data)
SELECT
  consumer_group,
  topic,
  COUNT(*) / 60 AS events_per_second
FROM consumer_offset_history
WHERE timestamp > NOW() - INTERVAL '1 minute'
GROUP BY consumer_group, topic;

-- DLQ growth rate
SELECT
  consumer_group,
  COUNT(*) AS unresolved_count
FROM dead_letter_events
WHERE resolution_status = 'unresolved'
  AND created_at > NOW() - INTERVAL '1 hour'
GROUP BY consumer_group;
```

### Alerts

**Critical alerts:**

1. **Consumer stopped processing**
   ```
   Condition: heartbeat_at > 10 minutes ago
   Action: Page on-call engineer
   ```

2. **High lag (> 10k events)**
   ```
   Condition: lag > 10,000
   Action: Investigate slow consumer, consider scaling
   ```

3. **DLQ growing rapidly**
   ```
   Condition: DLQ size increased by > 100 in last hour
   Action: Check for systemic errors in consumer logic
   ```

4. **Event creation stopped**
   ```
   Condition: No new events in last 5 minutes
   Action: Check event publisher, may indicate app issue
   ```

---

## Summary

### Key Principles

1. **Events are immutable facts**: Never modified after creation
2. **Consumer groups are independent**: Each tracks its own offset and DLQ
3. **Parallel processing**: Multiple groups can process same events simultaneously
4. **Failure isolation**: One group's failures don't affect others
5. **TTL cleanup**: Old events deleted after retention period

### Comparison to Kafka

| Feature | Kafka | OutboxRelay |
|---------|-------|-------------|
| Event storage | Distributed log | PostgreSQL table |
| Consumer offset | Kafka coordinator | `consumer_offsets` table |
| Consumer failure | Consumer internal retry | `dead_letter_events` table |
| Retention | Time-based + size | Time-based (TTL cleanup) |
| Partitioning | Built-in | `partition_key` column |
| Ordering | Per-partition | Per-partition (via `partition_key`) |
| Scalability | Horizontal (brokers) | Vertical (PostgreSQL) |

### When to Use Multi-Consumer Groups

✅ **Good use cases:**
- Multiple downstream systems need same events
- Different processing speeds (fast notifications, slow analytics)
- Independent failure domains (one system down shouldn't block others)
- Audit/compliance requirements (separate audit consumer group)

❌ **Bad use cases:**
- Single consumer, simple job queue (use Sidekiq instead)
- Very high throughput (> 100k events/sec, consider Kafka)
- Need exactly-once semantics across multiple databases (use 2PC)

---

**Next:** See [RETRY_LOGIC.md](./RETRY_LOGIC.md) for details on per-consumer retry strategies.
