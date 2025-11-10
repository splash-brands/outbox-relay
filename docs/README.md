# OutboxRelay Documentation

Complete technical documentation for the multi-consumer architecture refactoring.

---

## Overview

This documentation set explains why and how OutboxRelay transitioned from a state-based event system to an immutable event log architecture, enabling true multi-consumer group support.

**TL;DR:** We discovered that marking events as "consumed" breaks multi-consumer groups. Events should be immutable facts. Consumer groups track their own state via offsets and DLQ.

---

## Document Index

### 1. [Architecture Decision Record](./ARCHITECTURE_DECISION.md)
**Start here!** Complete explanation of the problem, solution, and rationale.

**Topics covered:**
- Discovery of the consumed state bug
- Why state belongs to consumers, not events
- Comparison to Kafka's model
- Schema changes (removing state machine)
- Implementation strategy
- Benefits and trade-offs

**Audience:** All developers, architects, technical leads

**Reading time:** 15 minutes

---

### 2. [Advisory Locks](./ADVISORY_LOCKS.md)
Deep technical dive on PostgreSQL advisory locks for duplicate prevention.

**Topics covered:**
- What are advisory locks?
- Why we need them (per-consumer-group locking)
- Lock key generation strategy
- Implementation details
- Edge cases (crashes, timeouts, contention)
- Performance considerations
- Troubleshooting

**Audience:** Backend developers implementing consumers

**Reading time:** 10 minutes

---

### 3. [Multi-Consumer Design](./MULTI_CONSUMER_DESIGN.md)
How multiple consumer groups work together to process the same events.

**Topics covered:**
- Kafka comparison
- Three-table architecture (events, offsets, DLQ)
- Consumer group semantics
- Query strategy (offset-based + DLQ exclusion)
- Example scenarios (fresh start, failures, lag, new consumers)
- Monitoring and observability

**Audience:** All developers working with consumers

**Reading time:** 12 minutes

---

### 4. [Retry Logic](./RETRY_LOGIC.md)
Per-consumer group failure handling and retry strategies.

**Topics covered:**
- Why failures are per-consumer
- Resolution status lifecycle (retrying → unresolved)
- Implementation (DLQ tracking)
- Configuration (max_retries, custom error handling)
- Manual intervention (reprocessing, marking resolved)
- Monitoring DLQ health

**Audience:** Developers implementing consumers, DevOps

**Reading time:** 10 minutes

---

### 5. [Migration Guide](./MIGRATION_GUIDE.md)
Step-by-step guide to migrate from old to new architecture.

**Topics covered:**
- Pre-migration checklist
- Migration steps (code deploy, schema changes)
- Rollback plan
- Post-migration validation
- Troubleshooting common issues
- Timeline and success criteria

**Audience:** DevOps, technical leads, anyone performing migration

**Reading time:** 15 minutes

---

## Quick Reference

### Key Concepts

**Event**: Immutable fact. Just exists in the log.
```ruby
{ sequence: 100, topic: "orders", payload: {...} }
```

**Consumer Offset**: "I've successfully processed everything up to sequence N"
```ruby
{ consumer_group: "notifications", topic: "orders", last_consumed_sequence: 100 }
```

**Dead Letter Event**: "Consumer group X failed on event Y"
```ruby
{ consumer_group: "analytics", outbox_event_id: 100, total_retries: 3, resolution_status: "unresolved" }
```

### Schema Summary

**Before (WRONG):**
```
outbox_events:
  - state (pending, processing, consumed, failed, dead_letter)
  - retry_count
  - error_message
  - lock_version
```

**After (CORRECT):**
```
outbox_events:
  - sequence, topic, event_name, payload, headers
  - partition_key, created_at, expires_at
  - NO STATE!

consumer_offsets:
  - consumer_group, topic, last_consumed_sequence

dead_letter_events:
  - consumer_group, outbox_event_id
  - total_retries, resolution_status
```

### Query Pattern

```ruby
# Fetch events I haven't processed and haven't given up on
OutboxEvent
  .where("sequence > ?", my_offset)
  .where.not(id: my_dlq_events.select(:outbox_event_id))
  .order(:sequence)
  .limit(50)
```

### Processing Flow

```
1. Fetch batch (offset-based + DLQ exclusion)
2. For each event:
   a. Try advisory lock (per consumer_group + event)
   b. If locked: consume_message()
   c. Update offset
3. On success: offset advances
4. On failure: create/update DLQ entry, retry
```

---

## FAQ

### Q: Why remove the state column?

**A:** State implies a single lifecycle, but each consumer group has its own relationship to an event. Event #100 can be successfully processed by "notifications" while simultaneously failing for "analytics". There's no single "state" that captures this.

### Q: How do we know when an event is "done"?

**A:** We don't! Events are never "done" - they just exist in the log. Consumer groups track what they've processed via offsets. After TTL (e.g., 7 days), events are deleted regardless of processing status.

### Q: Won't the table grow forever?

**A:** No. TTL-based cleanup deletes events older than N days (configurable). Just like Kafka's retention policy.

### Q: What if a consumer group fails on an event?

**A:** That consumer group's failure is tracked in `dead_letter_events`. Other consumer groups can still process the event successfully. After max retries, the failing consumer group skips that event (via DLQ exclusion query).

### Q: How do we prevent duplicate processing?

**A:** Advisory locks. Lock key includes both event sequence and consumer group hash. Different consumer groups → different locks → parallel processing. Same consumer group → same lock → only one worker processes.

### Q: What about exactly-once semantics?

**A:** OutboxRelay guarantees **at-least-once** delivery per consumer group. For exactly-once, consumers must be idempotent (use upsert, check for duplicates, etc.). This is the same as Kafka.

### Q: Can I add a new consumer group to process historical events?

**A:** Yes! New consumer groups start at offset 0 and can process all events in the log (unless TTL-deleted). You can also manually set the starting offset if you only want recent events.

### Q: What happens if a worker crashes during processing?

**A:** Advisory lock is automatically released (transaction rollback). Offset is not updated. Event will be reprocessed by another worker or the same worker after restart.

---

## Diagrams

### Architecture Overview

```
┌─────────────────────────────────────┐
│      outbox_events (Log)            │
│  [100] [101] [102] [103] [104] ...  │
└─────────────────────────────────────┘
              ↓ read by
    ┌─────────┴──────────┬──────────────┐
    ↓                    ↓              ↓
┌───────────┐      ┌───────────┐  ┌───────────┐
│ Consumer  │      │ Consumer  │  │ Consumer  │
│ Group A   │      │ Group B   │  │ Group C   │
├───────────┤      ├───────────┤  ├───────────┤
│ offset:   │      │ offset:   │  │ offset:   │
│   103     │      │   50      │  │   0       │
├───────────┤      ├───────────┤  ├───────────┤
│ DLQ:      │      │ DLQ:      │  │ DLQ:      │
│   none    │      │   [75]    │  │   none    │
└───────────┘      └───────────┘  └───────────┘
     ✅                ⚠️              ⏳
  Processed        Failed on       Not started
  1-103            #75, at 50      yet
```

### Event Lifecycle

```
Event created
      ↓
[Exists in log]
      ↓
   ┌──┴──┐
   ↓     ↓
Group A  Group B
processes processes
   ✅     ✅
   ↓     ↓
Offsets advance
   ↓
[Event still exists]
   ↓
(7 days later)
   ↓
TTL cleanup
   ↓
Event deleted
```

### Retry Flow

```
Event #100
   ↓
Consumer processes
   ↓
  Fails
   ↓
Create DLQ entry
(resolution_status: "retrying", total_retries: 1)
   ↓
Retry automatically
   ↓
  Fails again
   ↓
Update DLQ
(total_retries: 2)
   ↓
... repeat ...
   ↓
total_retries >= max_retries
   ↓
Update DLQ
(resolution_status: "unresolved")
   ↓
Excluded from fetch queries
   ↓
Manual intervention required
```

---

## Code Examples

### Defining a Consumer

```ruby
class NotificationsConsumer < OutboxRelay::OutboxConsumer
  def initialize(partition_key:)
    super(
      consumer_group: "notifications",
      topic: "order_events",
      partition_key: partition_key,
      dead_letter_config: { max_retries: 3 }
    )
  end

  def consume_message(event)
    # Your business logic
    OrderNotificationService.send(event.payload)
  end
end
```

### Publishing an Event

```ruby
OutboxRelay::OutboxEvent.create!(
  topic: "order_events",
  event_name: "order_created",
  payload: { order_id: 123, customer_id: 456 },
  headers: { correlation_id: "abc-123" },
  partition_key: 0
)
```

### Checking Consumer Lag

```ruby
offset = OutboxRelay::ConsumerOffset.find_by(
  consumer_group: "notifications",
  topic: "order_events"
)

latest_sequence = OutboxRelay::OutboxEvent
  .where(topic: "order_events")
  .maximum(:sequence) || 0

lag = latest_sequence - offset.last_consumed_sequence
puts "Lag: #{lag} events"
```

### Inspecting DLQ

```ruby
dlq_events = OutboxRelay::DeadLetterEvent
  .where(consumer_group: "analytics")
  .where(resolution_status: "unresolved")
  .order(created_at: :desc)
  .limit(10)

dlq_events.each do |dlq|
  puts "Event #{dlq.outbox_event_id}: #{dlq.error_message}"
end
```

---

## Related Resources

- [PostgreSQL Advisory Locks Documentation](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS)
- [Kafka Consumer Groups](https://kafka.apache.org/documentation/#consumergroups)
- [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)
- [At-Least-Once vs Exactly-Once Delivery](https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/)

---

## Change Log

- **2025-11-03**: Initial documentation for multi-consumer architecture
- **2025-11-03**: Architecture implementation completed
- **2025-11-07**: Documentation updated to reflect implementation status

---

## Contributing

Found an error or have a suggestion? Please:

1. Open an issue on GitHub
2. Submit a pull request with corrections
3. Add examples or clarifications

This documentation is living and will evolve with the project.

---

**Note:** OutboxRelay implements an immutable event log architecture (similar to Kafka), not a traditional job queue with state machines. Events are facts that exist in the log; consumer groups track their own progress via offsets and DLQ. This architectural choice enables true multi-consumer scenarios where different services process the same events at their own pace.
