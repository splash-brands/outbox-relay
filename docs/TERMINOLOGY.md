# OutboxRelay Terminology

Quick reference glossary of key terms and concepts.

---

## Core Concepts

### Event
**Immutable fact** that exists in the event log. Does NOT have lifecycle states.

```ruby
OutboxRelay::OutboxEvent {
  sequence: 12345,
  topic: "order_events",
  event_name: "order_created",
  payload: { order_id: 123 },
  partition_key: 0,
  created_at: "2025-11-03 10:00:00"
}
```

**Key properties:**
- Never changes after creation
- No "pending", "processing", or "consumed" state
- Same event can be processed by multiple consumer groups
- Deleted after TTL (default: 7 days)

---

### Consumer Group
**Independent processor** of events with its own progress tracking.

```ruby
consumer_group: "notifications_p0"
```

**Characteristics:**
- Named uniquely: `{base_name}_p{partition_number}`
- Tracks own offset (last processed sequence)
- Has own Dead Letter Queue
- Multiple groups can process same events independently
- Similar to Kafka consumer groups

**Examples:**
- `notifications_p0` - Notifications service, partition 0
- `notifications_p1` - Notifications service, partition 1
- `analytics_p0` - Analytics service, partition 0
- `striven_p0` - ERP sync service, partition 0

---

### Offset
**Last successfully processed sequence** for a consumer group.

```ruby
OutboxRelay::ConsumerOffset {
  consumer_group: "notifications_p0",
  topic: "order_events",
  last_consumed_sequence: 12340,
  last_consumed_at: "2025-11-03 10:05:00"
}
```

**Meaning:** "Consumer group 'notifications_p0' has successfully processed everything up to sequence 12340"

**Updates:**
- Increments atomically after successful processing
- Never decreases (validation prevents)
- Used in queries: `WHERE sequence > last_consumed_sequence`

---

### Sequence
**Global monotonic counter** for all events across all topics.

```ruby
sequence: 1, 2, 3, 4, 5, ...
```

**Properties:**
- Generated from PostgreSQL sequence: `outbox_relay_outbox_events_sequence`
- Never resets or reuses
- Provides total ordering of events
- Used for offset tracking

**Note:** NOT per-topic offset like Kafka. Global across all topics for simplicity.

---

### Partition / Partition Key
**Routing mechanism** for parallel processing and ordering guarantees.

```ruby
partition_key: 2  # Integer 0, 1, 2, ...
```

**Purpose:**
- Events with same partition_key go to same partition
- Each partition processed by dedicated worker
- Guarantees ordering within partition
- Enables parallelism across partitions

**Calculation:**
```ruby
partition_key = Zlib.crc32(routing_key.to_s) % partition_count
# Example: order_id "123" → CRC32 → % 4 → partition 2
```

**Naming:**
- Consumer groups suffixed with partition: `notifications_p0`, `notifications_p1`
- Separate offset per partition

---

### Dead Letter Queue (DLQ)
**Per-consumer-group failure tracking** for events that cannot be processed.

```ruby
OutboxRelay::DeadLetterEvent {
  outbox_relay_outbox_event_id: 100,
  consumer_group: "analytics_p0",
  total_retries: 3,
  error_message: "ArgumentError: invalid data",
  resolution_status: "unresolved"
}
```

**Resolution Statuses:**
- `retrying` - Will be retried automatically (total_retries < max_retries)
- `unresolved` - Max retries exceeded, excluded from fetch, needs manual intervention
- `resolved` - Manually marked as resolved without reprocessing
- `reprocessed` - Successfully reprocessed after manual intervention
- `ignored` - Permanently ignored

**Key behaviors:**
- Same event can be in DLQ for one consumer group but not others
- Events with `unresolved` status excluded from fetch queries
- Manual intervention required for `unresolved` events

---

### Advisory Lock
**PostgreSQL lock mechanism** for duplicate prevention within consumer group.

```ruby
lock_key = (event.sequence << 32) | Zlib.crc32(consumer_group)
```

**Purpose:**
- Prevents multiple workers from same consumer group processing same event
- Different consumer groups → different lock keys → parallel processing ✅
- Same consumer group → same lock key → only one worker processes ✅

**Properties:**
- Transaction-scoped (released on COMMIT/ROLLBACK)
- Non-blocking (`pg_try_advisory_xact_lock`)
- Automatically released on crash

**Example:**
```
Event #100:
- Lock for "notifications_p0": (100 << 32) | hash("notifications_p0") = X
- Lock for "analytics_p0":     (100 << 32) | hash("analytics_p0")     = Y
- X ≠ Y → Both can process in parallel
```

---

### Lag
**Number of events pending processing** for a consumer group.

```ruby
lag = latest_sequence - last_consumed_sequence
```

**Examples:**
- Lag 0 = Caught up ✅
- Lag 10 = 10 events behind ⚠️
- Lag 1000+ = Significant backlog 🚨

**Monitoring:**
```bash
bundle exec rake outbox_relay:lag
```

**Dynamic Delay Algorithm:**
- Lag > batch_size → 10ms polling (high backlog)
- 0 < Lag < batch_size → 100ms polling (some backlog)
- Lag = 0 → 1s polling (idle)

---

### Supervisor
**Main process** that manages worker processes.

```
Supervisor (PID 12345)
├── Worker 1 (notifications_p0)
├── Worker 2 (notifications_p1)
└── Worker 3 (analytics_p0)
```

**Responsibilities:**
- Fork worker processes
- Monitor worker health (heartbeats)
- Restart crashed workers
- Handle signals (TERM, INT, QUIT)
- Graceful shutdown coordination

---

### Worker
**Forked process** that polls and processes events for specific consumer/partition.

```ruby
Worker {
  consumer_group: "notifications_p0",
  consumer_class: "OrderUpdatesConsumer",
  partition_key: 0,
  topic: "order_events",
  pid: 12346
}
```

**Lifecycle:**
```
1. Fork from supervisor
2. Reconnect to database (fork-safety)
3. Instantiate consumer
4. Enter polling loop
5. Process batches
6. Update offset
7. Handle signals
8. Exit gracefully
```

**Isolation:** Each worker runs in separate process for fault tolerance.

---

### Immutable Event Log
**Architectural pattern** where events are facts, not lifecycle entities.

**Characteristics:**
- Events never change after creation
- No state transitions (pending → processing → consumed)
- Consumer groups track progress independently
- TTL-based cleanup (not consumption-based)
- Same model as Kafka, EventStore, NATS Streaming

**Contrast with Job Queue:**

| Aspect | Job Queue | Immutable Log |
|--------|-----------|---------------|
| **Entity** | Job (has state) | Event (immutable fact) |
| **Lifecycle** | pending → processing → done | Created → (exists) → TTL deleted |
| **Consumers** | Single (or retry) | Multiple independent groups |
| **Progress** | Job marked done | Consumer tracks offset |
| **Failure** | Job retries/fails | Consumer group DLQ |

---

### Transactional Outbox Pattern
**Design pattern** for reliable event publishing.

```ruby
Order.transaction do
  order = Order.create!(attributes)

  # Event written in same transaction
  OutboxRelay::OutboxEvent.create!(
    topic: "order_events",
    event_name: "order_created",
    payload: { order_id: order.id }
  )
end
```

**Guarantees:**
- Event published atomically with business logic
- No lost events (both commit or both rollback)
- No duplicate events from retry logic

---

### At-Least-Once Delivery
**Delivery guarantee** provided by OutboxRelay.

**Meaning:**
- Every consumer group will process each event **at least once**
- May process same event multiple times (due to crashes, retries)
- Consumers must be idempotent

**Comparison:**
- **At-most-once**: May lose events ❌
- **At-least-once**: May duplicate events ✅ (OutboxRelay)
- **Exactly-once**: No loss, no duplicates (requires distributed transactions)

**Idempotency Strategies:**
```ruby
# 1. Upsert with unique constraint
User.upsert({ id: user_id, ... }, unique_by: :id)

# 2. Check before processing
return if already_processed?(event.event_id)

# 3. Store event_id in target table
Order.create!(order_data.merge(event_id: event.event_id))
```

---

### Heartbeat
**Liveness indicator** for workers.

```ruby
ConsumerOffset {
  heartbeat_at: "2025-11-03 10:05:00",
  consumer_instance_id: "notifications_p0-hostname-12346"
}
```

**Purpose:**
- Supervisor monitors worker health
- Stale heartbeat (> 5 minutes) indicates dead worker
- Used for alerting and automatic restart

**Updates:**
- Every 10 loops or 10 seconds
- Written to `consumer_offsets.heartbeat_at`

---

### TTL (Time To Live)
**Retention period** for events before deletion.

```ruby
# Default: 7 days
expires_at: 7.days.from_now
```

**Cleanup:**
```bash
bundle exec rake outbox_relay:cleanup[7]  # Delete events older than 7 days
```

**Safety:**
- Only deletes events processed by ALL consumer groups
- Never deletes events in active DLQ (unresolved/retrying)
- Configurable per topic

---

### Fork-Based Architecture
**Process model** for worker isolation.

```
Supervisor (process)
  ├─ fork() → Worker 1 (separate process)
  ├─ fork() → Worker 2 (separate process)
  └─ fork() → Worker 3 (separate process)
```

**Benefits:**
- Process isolation (crash doesn't affect others)
- Separate memory space (no shared state)
- OS-level resource management
- Simple restart logic

**Critical:** Must reconnect to database after fork:
```ruby
ActiveRecord::Base.connection.reconnect!
```

---

### Dynamic Polling Delay
**Adaptive algorithm** for balancing latency and CPU usage.

```ruby
if lag > batch_size
  0.01  # 10ms - High backlog
elsif lag > 0
  0.1   # 100ms - Some backlog
else
  1.0   # 1s - Idle
end
```

**Result:**
- Low latency when events pending (< 100ms)
- Low CPU usage when idle (1s)
- Automatic adjustment without configuration

---

## Common Patterns

### Consumer Group Naming
```
{base_name}_p{partition}
```

Examples:
- `notifications_p0`, `notifications_p1`, `notifications_p2`
- `analytics_p0`
- `striven_p0`, `striven_p1`

### Query Pattern
```ruby
# Fetch events I haven't processed and haven't given up on
OutboxEvent
  .where(topic: topic)
  .where(partition_key: partition_key)
  .where("sequence > ?", last_consumed_sequence)
  .where.not(id: dlq_events.select(:outbox_event_id))
  .order(:sequence)
  .limit(batch_size)
```

### Error Handling Flow
```
1. Event processing fails
2. Create/update DLQ entry (increment total_retries)
3. If total_retries < max_retries: status = "retrying" (auto-retry)
4. If total_retries >= max_retries: status = "unresolved" (exclude from fetch)
5. Manual intervention required for "unresolved"
```

---

## Quick Command Reference

```bash
# Start workers
./bin/outbox_relay

# Check status
rake outbox_relay:status

# Check lag
rake outbox_relay:lag

# View configuration
rake outbox_relay:config

# Clean up old events
rake outbox_relay:cleanup[7]

# Stop gracefully
rake outbox_relay:stop
```

---

## See Also

- [Architecture Decision Record](./ARCHITECTURE_DECISION.md) - Why we built it this way
- [Multi-Consumer Design](./MULTI_CONSUMER_DESIGN.md) - How multiple consumers work
- [Advisory Locks](./ADVISORY_LOCKS.md) - Duplicate prevention mechanism
- [Retry Logic](./RETRY_LOGIC.md) - Failure handling details
- [Migration Guide](./MIGRATION_GUIDE.md) - Upgrading from old architecture

---

**Last updated:** 2025-11-07
