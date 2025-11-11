# OutboxRelay

Production-ready PostgreSQL-based message queue with continuous polling implementing the Transactional Outbox pattern.

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE.txt)
[![Ruby Version](https://img.shields.io/badge/ruby-3.0+-red.svg)](https://www.ruby-lang.org)
[![Rails Version](https://img.shields.io/badge/rails-7.0+-red.svg)](https://rubyonrails.org)

## 🎯 Overview

OutboxRelay is a high-performance, long-running process system that continuously polls PostgreSQL for outbox events and processes them through configured consumers. Unlike traditional job queue approaches with fixed intervals, OutboxRelay provides near real-time processing (< 1 second latency) with intelligent dynamic delays based on workload.

**Key Features:**

- 🚀 **Sub-second latency** - Dynamic polling (10ms-1s) based on workload
- 🔒 **Concurrent-safe** - PostgreSQL advisory locks for duplicate prevention
- 👥 **Multi-consumer support** - Multiple consumer groups process same events independently
- 🔄 **Fork-based workers** - Process isolation for fault tolerance
- 📊 **Partition support** - Parallel processing across partitions
- 💀 **Dead letter queue** - Per-consumer-group failure tracking
- 🎯 **Event filtering** - Process only specific event types
- 📈 **Monitoring** - Built-in lag tracking and status reporting
- 🔧 **Zero dependencies** - Pure PostgreSQL, no external queue systems

## 🛡️ Production Readiness for ECS/Container Deployments

OutboxRelay implements **all critical production patterns** for reliable container deployments:

- ✅ **Self-pipe trick** - Instant signal response (no polling delay on shutdown)
- ✅ **Database-backed process registry** - Supervisors and workers tracked in PostgreSQL
- ✅ **Automatic heartbeat** - Worker health monitoring with TimerTask
- ✅ **Dead process cleanup** - Rake tasks to prune stale process records
- ✅ **Orphaned event recovery** - PostgreSQL advisory locks auto-release on crash
- ✅ **Rails executor wrapper** - Proper thread-safe context for forked workers
- ✅ **Signal handling** - TERM/INT for graceful, QUIT for immediate shutdown
- ✅ **Supervisor monitoring** - Workers detect supervisor death and self-terminate
- ✅ **Restart backoff** - Exponential backoff prevents restart storms
- ✅ **Failed worker tracking** - Visibility into fork failures and startup issues

**Container-Friendly Features:**
- Fork-based architecture with proper database reconnection
- Graceful shutdown with configurable timeout (default: 30s)
- Health check support via process registry
- macOS development + Linux production validated

## 📊 Performance Comparison

| Metric | Traditional Job Queue | OutboxRelay |
|--------|----------------------|---------------|
| **Latency** | 10-60 seconds | 10ms-1s (avg 1s idle) |
| **CPU overhead** | Medium (job spawn) | Low (continuous polling) |
| **Backlog response** | Fixed delay | Dynamic (10ms-1s) |
| **Fault tolerance** | Job retry system | Auto-restart workers |
| **Infrastructure** | Redis/Queue system | Pure PostgreSQL |

## 🏗️ Architecture

```
OutboxRelay::Supervisor (main process)
  ├── Fork-based worker processes (one per consumer/partition)
  ├── Signal handling (TERM, INT, QUIT)
  ├── Automatic worker restart with exponential backoff
  └── Process registration and heartbeat monitoring

OutboxRelay::Worker (per partition)
  ├── Continuous polling loop with dynamic delay
  ├── Batch processing (configurable size)
  ├── Intelligent backlog detection
  └── Graceful shutdown handling
```

### Key Design Patterns

1. **Immutable Event Log** - Events are facts that exist, not lifecycle entities (like Kafka)
2. **Multi-Consumer Groups** - Multiple groups process same events independently
3. **Per-Consumer Tracking** - Each consumer group tracks progress via offsets and DLQ
4. **Advisory Locks** - PostgreSQL locks prevent duplicate processing within same group
5. **Fork-based isolation** - Each worker runs in its own process for fault isolation
6. **Dynamic polling delay** - Adjusts delay based on workload (0.01s to polling_interval)
7. **Process supervision** - Supervisor monitors and restarts failed workers

### Event Processing Model

OutboxRelay follows an **immutable event log** pattern similar to Kafka:

- **Events don't have states** - No "pending", "processing", or "consumed" states
- **Events are immutable** - Once created, events never change
- **Consumer groups are independent** - Each group tracks its own progress
- **Failures are per-consumer** - Event #100 can succeed for Group A, fail for Group B

This enables true multi-consumer scenarios where different services process the same events at their own pace.

## 📦 Installation

Add to your Gemfile:

```ruby
gem 'outbox_relay'
```

Then run:

```bash
bundle install
bin/rails generate outbox_relay:install
bin/rails db:migrate
```

This will:
- Create PostgreSQL tables (`outbox_relay_outbox_events`, `outbox_relay_consumer_offsets`, `outbox_relay_dead_letter_events`, `outbox_relay_processes`)
- Generate initializer at `config/initializers/outbox_relay.rb`
- Generate YAML configuration at `config/outbox_consumers.yml`
- Create executable at `bin/outbox_relay` for starting the server

## 🚀 Quick Start

### 1. Configure Topics and Consumers

Edit `config/outbox_consumers.yml`:

```yaml
# Define your topics with partition counts
topics:
  order_updates:
    partitions: 4
    description: "Order lifecycle events"

  notifications:
    partitions: 2
    description: "User notification events"

# Define consumer groups
consumer_groups:
  order_processor:
    description: "Processes order events"
    topics:
      - name: order_updates
        class: "OrderUpdatesConsumer"
        partitions: all  # Process all partitions

  notification_sender:
    description: "Sends notifications"
    topics:
      - name: notifications
        class: "NotificationConsumer"
        partitions: all
```

### 2. Define a Consumer

Create a consumer in `app/consumers/`:

```ruby
# app/consumers/order_updates_consumer.rb
class OrderUpdatesConsumer < OutboxRelay::OutboxConsumer
  def initialize(partition_key:)
    super(
      consumer_group: "order_processor",
      topic: "order_updates",
      partition_key: partition_key,
      event_filter: ["created", "updated"], # optional
      dead_letter_config: { max_retries: 2 } # optional
    )
  end

  def consume_message(event)
    # Access event data:
    # - event.payload (Hash/Array)
    # - event.headers (Hash)
    # - event.event_name (String)
    # - event.event_id (UUID)

    order_id = event.payload["order_id"]
    order = Order.find(order_id)

    case event.event_name
    when "created"
      OrderNotificationService.notify_created(order)
    when "updated"
      OrderNotificationService.notify_updated(order)
    end
  end
end
```

### 3. Publish Events

Publish events using OutboxPublisher service (recommended):

```ruby
# In your service/model/controller
OutboxPublisher.publish(
  topic: "order_updates",
  payload: {
    order_id: order.id,
    customer_id: order.customer_id,
    total: order.total
  },
  headers: {
    event_name: "created",
    partition_key: order.id.to_s  # Required for partitioned topics
  }
)

# Or directly (not recommended - use OutboxPublisher instead):
OutboxRelay::OutboxEvent.create!(
  topic: "order_updates",
  event_name: "created",
  payload: { order_id: order.id },
  headers: {},
  partition_key: 0 # defaults to 0 if not provided
)
```

### 4. Start Workers

```bash
# Start with default settings
./bin/outbox_relay

# Or with custom options
./bin/outbox_relay --polling-interval 0.5 --batch-size 200

# View help
./bin/outbox_relay help
```

### 5. Monitor

```bash
# Check running processes
bundle exec rake outbox_relay:status

# Check consumer lag
bundle exec rake outbox_relay:lag

# View configuration
bundle exec rake outbox_relay:config

# Stop gracefully
bundle exec rake outbox_relay:stop
```

## ⚙️ Configuration

### Initializer Options

In `config/initializers/outbox_relay.rb`:

```ruby
OutboxRelay.configure do |config|
  # Polling interval (seconds) - default delay when no backlog
  config.polling_interval = 1.0

  # Batch size - events processed per poll
  config.batch_size = 100

  # Max loops before worker restart (prevents memory leaks)
  config.max_loops = 1000

  # Graceful shutdown timeout (seconds)
  config.shutdown_timeout = 30

  # Silence ActiveRecord query logs for polling
  config.silence_polling = true
end

# Configure custom logger
if Rails.application.config.respond_to?(:custom_logger)
  OutboxRelay.custom_logger = Rails.application.config.custom_logger
end
```

### CLI Options

```bash
# View all available options
./bin/outbox_relay help start

# Start with custom options
./bin/outbox_relay \
  --polling-interval 0.5 \
  --batch-size 50 \
  --max-loops 500 \
  --log-level debug

# Or using short flags
./bin/outbox_relay -p 0.5 -b 50 -m 500 -l debug
```

### YAML Configuration

The `config/outbox_consumers.yml` file supports:

**Environment-specific overrides:**
```yaml
# config/outbox_consumers.production.yml
consumer_groups:
  order_processor:
    topics:
      - name: order_updates
        class: "OrderUpdatesConsumer"
        partitions: [0, 1, 2, 3]  # Production uses specific partitions
```

**Partition strategies:**
```yaml
topics:
  high_volume_topic:
    partitions: 8  # Creates 8 workers (partitions 0-7)

consumer_groups:
  processor:
    topics:
      - name: high_volume_topic
        class: "Processor"
        partitions: all  # Process all 8 partitions
      # OR
      - name: high_volume_topic
        class: "Processor"
        partitions: [0, 1, 2, 3]  # Process only partitions 0-3
```

When publishing events, partition is calculated automatically using CRC32:

```ruby
OutboxPublisher.publish(
  topic: "order_updates",
  payload: { order_id: 123 },
  headers: {
    event_name: "created",
    partition_key: "123"  # Same key always goes to same partition
  }
)
```

## 🔄 Dynamic Delay Algorithm

OutboxRelay intelligently adjusts polling delay based on workload:

```ruby
def calculate_next_delay(consumer, processed_count)
  if processed_count > 0
    lag = consumer.lag

    if lag > batch_size
      0.01  # 10ms - High backlog, poll immediately
    elsif lag > 0
      0.1   # 100ms - Some backlog, poll quickly
    else
      polling_interval  # 1s - No backlog, normal interval
    end
  else
    polling_interval  # 1s - Nothing processed, wait
  end
end
```

**Result**: The system automatically speeds up when there's a backlog and slows down when idle.

- **High backlog** (lag > batch_size): 10ms polling - very fast response
- **Partial backlog** (0 < lag < batch_size): 100ms polling - balanced
- **No backlog** (lag = 0): 1s polling - minimal CPU usage

## 📈 Monitoring & Observability

### Rake Tasks

```bash
# Show all running processes with CPU/memory
rake outbox_relay:status

# Show lag across all consumers
rake outbox_relay:lag

# Show worker configuration
rake outbox_relay:config

# Clean up old processed events (default: 7 days TTL)
rake outbox_relay:cleanup
rake outbox_relay:cleanup[30]  # Custom TTL

# Stop all processes gracefully
rake outbox_relay:stop

# Process registry maintenance (production)
rake outbox_relay:process_status         # Show all registered processes
rake outbox_relay:prune_processes        # Clean up dead processes (60s timeout)
rake outbox_relay:prune_processes[30]    # Custom timeout (30s)

# Advisory lock health check
rake outbox_relay:check_locks            # Monitor PostgreSQL advisory locks
```

### Consumer Methods

```ruby
consumer = OrderUpdatesConsumer.new(partition_key: 0)

# Check lag (events pending)
lag = consumer.lag # => 150

# Check if consumer is processing
consumer.active? # => true/false

# Check last processed event
consumer.last_consumed_sequence # => 12345
consumer.last_consumed_at # => 2024-11-03 10:30:00 UTC
```

### Logging

OutboxRelay provides structured logging via custom_logger:

```ruby
# Worker lifecycle events
OutboxRelay.logger.info(
  event_name: "worker_started",
  consumer_group: "order_updates",
  topic: "order_updates",
  partition_key: 0
)

# Batch processing
OutboxRelay.logger.debug(
  event_name: "batch_processed",
  processed: 10,
  lag: 5,
  duration_ms: 42.5
)

# Errors
OutboxRelay.logger.error(
  event_name: "consumer_error",
  consumer_class: "OrderUpdatesConsumer",
  error: error.message,
  backtrace: error.backtrace.first(5)
)
```

## 💀 Dead Letter Queue

Failed events are tracked per consumer group in the dead letter queue:

```ruby
# Query dead letter events for a specific consumer group
dead_events = OutboxRelay::DeadLetterEvent
  .where(consumer_group: "order_updates")
  .where(resolution_status: ["unresolved", "retrying"])
  .order(created_at: :desc)

# Inspect failed event
dead_event = dead_events.first
dead_event.consumer_group # => "order_updates"
dead_event.error_message # => "ArgumentError: invalid order"
dead_event.error_backtrace # => ["app/consumers/...", ...]
dead_event.total_retries # => 3
dead_event.resolution_status # => "retrying" or "unresolved"
dead_event.original_payload # => { "order_id" => 123 }

# Resolution statuses:
# - "retrying": Event will be retried automatically
# - "unresolved": Max retries reached, needs manual intervention
# - "resolved": Manually resolved without reprocessing
# - "reprocessed": Successfully reprocessed
# - "ignored": Permanently ignored

# Reprocess manually
dead_event.update!(resolution_status: "reprocessed")
OutboxPublisher.publish(
  topic: dead_event.original_topic,
  payload: dead_event.original_payload,
  headers: dead_event.original_headers.merge(event_name: dead_event.original_event_name)
)
```

## 🧹 Maintenance & Cleanup

OutboxRelay includes a cleanup task to remove old processed events:

```bash
# Run cleanup with default TTL (7 days)
bundle exec rake outbox_relay:cleanup

# Custom TTL (30 days)
bundle exec rake outbox_relay:cleanup[30]
```

**Safety guarantees:**
- Only deletes events older than TTL
- Only deletes events processed by ALL consumer groups
- Never deletes events in active DLQ entries (unresolved/retrying)
- Cleans up resolved DLQ entries older than TTL

**Schedule via cron:**
```cron
# Daily cleanup at 2 AM
0 2 * * * cd /app && bundle exec rake outbox_relay:cleanup[7]
```

**Example output:**
```
OutboxRelay Cleanup
================================================================================
TTL: 7 days (before 2024-10-27 00:00:00 UTC)

Consumer Progress:
--------------------------------------------------------------------------------
  order_updates: All groups processed up to sequence 50000
  user_events: All groups processed up to sequence 12000

  order_updates: Deleted 10523 events
  user_events: Deleted 3421 events

DLQ Cleanup:
--------------------------------------------------------------------------------
  Resolved entries: Deleted 156 old entries

================================================================================
Total cleaned:
  Events: 13944
  DLQ entries: 156
  Combined: 14100
```

## 🚀 Production Deployment

### Pre-Production Checklist

Before deploying OutboxRelay to production:

- [ ] **Database Setup**
  - [ ] Run migrations: `rails db:migrate`
  - [ ] Verify tables exist: `outbox_relay_outbox_events`, `outbox_relay_consumer_offsets`, `outbox_relay_dead_letter_events`, `outbox_relay_processes`
  - [ ] Add indexes for performance (included in migrations)

- [ ] **Configuration**
  - [ ] Create `config/outbox_consumers.yml` with your topics and consumer groups
  - [ ] Set appropriate partition counts based on expected load
  - [ ] Configure `config/initializers/outbox_relay.rb` with production values
  - [ ] Test configuration: `bundle exec rake outbox_relay:config`

- [ ] **Consumers**
  - [ ] Implement all consumer classes referenced in YAML config
  - [ ] Add error handling for expected failure cases
  - [ ] Test consumers in staging environment
  - [ ] Verify consumers are idempotent (safe to process same event twice)

- [ ] **Monitoring**
  - [ ] Set up health check endpoint (see Health Checks section)
  - [ ] Configure alerting for high lag (> 1000 events)
  - [ ] Monitor process registry: `rake outbox_relay:process_status`
  - [ ] Set up log aggregation for structured logs

- [ ] **Cleanup**
  - [ ] Schedule daily cleanup: `0 2 * * * cd /app && bundle exec rake outbox_relay:cleanup[7]`
  - [ ] Schedule dead process cleanup: `*/5 * * * * cd /app && bundle exec rake outbox_relay:prune_processes`

- [ ] **Deployment**
  - [ ] Add to Procfile or systemd service
  - [ ] Configure graceful shutdown timeout (default: 30s)
  - [ ] Test deployment and rollback procedures
  - [ ] Verify workers restart after deployment

- [ ] **Post-Deployment**
  - [ ] Verify workers are running: `rake outbox_relay:status`
  - [ ] Check lag is processing: `rake outbox_relay:lag`
  - [ ] Monitor logs for errors
  - [ ] Test publishing and consuming events end-to-end

### Procfile

```yaml
# Procfile (for Heroku, Render, etc.)
web: bundle exec rails server
outbox_relay: ./bin/outbox_relay
worker: bundle exec sidekiq
```

### Docker

```dockerfile
# Start workers in production
CMD ["./bin/outbox_relay"]
```

### Systemd

```ini
[Unit]
Description=OutboxRelay Workers
After=postgresql.service

[Service]
Type=simple
User=deploy
WorkingDirectory=/var/www/app
ExecStart=/var/www/app/bin/outbox_relay
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### Health Checks

```ruby
# config/routes.rb
get '/health/outbox_relay', to: 'health#outbox_relay'

# app/controllers/health_controller.rb
def outbox_relay
  # Check if workers are running and healthy
  recent_processes = OutboxRelay::Process
    .where("last_heartbeat_at > ?", 2.minutes.ago)
    .count

  # Check lag across all consumer groups
  total_lag = OutboxRelay::ConsumerOffset.sum do |offset|
    offset.lag rescue 0
  end

  healthy = recent_processes > 0 && total_lag < 1000

  render json: {
    healthy: healthy,
    running_processes: recent_processes,
    total_lag: total_lag
  }, status: healthy ? 200 : 503
end
```

### Scheduled Maintenance

Set up these cron jobs for production:

```cron
# Daily event cleanup at 2 AM
0 2 * * * cd /app && bundle exec rake outbox_relay:cleanup[7]

# Prune dead processes every 5 minutes
*/5 * * * * cd /app && bundle exec rake outbox_relay:prune_processes[60]

# Monitor advisory locks daily
0 6 * * * cd /app && bundle exec rake outbox_relay:check_locks >> /var/log/outbox_relay_locks.log
```

## 🔧 Troubleshooting

### Workers not processing events

1. Check processes: `rake outbox_relay:status`
2. Check lag: `rake outbox_relay:lag`
3. Check logs: `tail -f log/production.log | grep outbox_relay`
4. Verify consumer registration in `config/outbox_consumers.yml`
5. Test configuration: `rake outbox_relay:config`

### High CPU usage

- Increase `polling_interval` (e.g., from 1s to 2s)
- Increase `batch_size` to process more events per poll
- Check consumer implementation for performance issues
- Monitor database query performance

### Memory leaks

- Reduce `max_loops` (workers restart more frequently)
- Monitor worker memory: `ps aux | grep outbox_relay`
- Check consumer code for memory retention
- Enable memory profiling in development

### Workers crashing

- Check logs for error traces
- Supervisor automatically restarts failed workers with exponential backoff
- Investigate consumer exception handling
- Review dead letter queue: `OutboxRelay::DeadLetterEvent.where(resolution_status: "unresolved")`
- Check for database connection issues

### High lag

- Increase number of partitions for parallel processing
- Increase `batch_size` to process more events per poll
- Add more consumer groups for specific partitions
- Check consumer processing speed
- Monitor database performance

## 🧪 Testing

### Testing Consumers

```ruby
RSpec.describe OrderUpdatesConsumer do
  let(:consumer) { described_class.new(partition_key: 0) }

  let(:event) do
    create(:outbox_event,
      topic: "order_updates",
      event_name: "created",
      payload: { "order_id" => order.id }
    )
  end

  it "processes order creation event" do
    expect { consumer.consume_message(event) }
      .to change { OrderNotification.count }.by(1)
  end
end
```

### Testing Event Publishing

```ruby
it "publishes event to outbox" do
  expect {
    service.create_order(params)
  }.to change { OutboxRelay::OutboxEvent.count }.by(1)

  event = OutboxRelay::OutboxEvent.last
  expect(event.topic).to eq("order_updates")
  expect(event.event_name).to eq("created")
  expect(event.payload["order_id"]).to eq(order.id)
end
```

## 📚 Advanced Usage

### Custom Error Handling

```ruby
class OrderUpdatesConsumer < OutboxRelay::OutboxConsumer
  def consume_message(event)
    process_order(event)
  rescue ActiveRecord::RecordNotFound => e
    # Log and skip - don't retry for missing records
    logger.warn("Order not found: #{event.payload['order_id']}")
    # Don't re-raise - event will be marked as consumed
  rescue ExternalApiError => e
    # Re-raise for retry via DLQ
    raise
  end
end
```

### Event Expiration

```ruby
# Publish with expiration
OutboxRelay::OutboxEvent.create!(
  topic: "temporary_events",
  event_name: "session_update",
  payload: data,
  expires_at: 1.hour.from_now
)

# Cleanup expired events (add to cron)
OutboxRelay::OutboxEvent.where("expires_at < ?", Time.current).delete_all
```

### Multi-Tenant Events

```ruby
# Scope events by tenant
OutboxPublisher.publish(
  topic: "tenant_events",
  payload: { tenant_id: current_tenant.id, data: data },
  headers: {
    event_name: "created",
    partition_key: current_tenant.id.to_s # Partition by tenant
  }
)

# Consumer filters by tenant
def consume_message(event)
  tenant_id = event.payload["tenant_id"]
  Tenant.find(tenant_id).switch do
    process_event(event)
  end
end
```

## 🤝 Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/splash-brands/outbox-relay.

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

## 📄 License

The gem is available as open source under the terms of the [MIT License](LICENSE.txt).

## 🙏 Acknowledgments

- Built with ❤️ for the Ruby community
- Influenced by [Karafka](https://karafka.io) consumer patterns
- Implements battle-tested patterns for production reliability

## 📚 Additional Resources

- [PostgreSQL FOR UPDATE SKIP LOCKED](https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE)
- [Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)
- [PostgreSQL Advisory Locks](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS)

---

**OutboxRelay** - Production-ready PostgreSQL message queue for Ruby on Rails 🚀
