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
- 🎯 **Partition claiming** - Database-backed distributed locks prevent duplicate workers (v0.8.0+)
- 💀 **Dead letter queue** - Per-consumer-group failure tracking
- 🎯 **Event filtering** - Process only specific event types
- 📈 **Monitoring** - ActiveSupport::Notifications for any backend (Sentry, DataDog, New Relic)
- 🔧 **Zero dependencies** - Pure PostgreSQL, no external queue systems
- 🎨 **Rails-native** - Deep Rails integration (Engine, LogSubscriber, Error Reporting)
- 🐛 **Auto error tracking** - Automatic Sentry/Bugsnag integration (Rails 7+)

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
- ✅ **Partition claiming** - Only one worker per partition across all instances (v0.8.0+)

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

> **Multi-Instance Deployments**: OutboxRelay supports multiple supervisors running simultaneously (e.g., in ECS, Kubernetes). Workers coordinate via PostgreSQL `FOR UPDATE SKIP LOCKED` to prevent duplicate processing. See [Multi-Instance Deployment](#-multi-instance-deployment) for details.

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

### Rails-Style Configuration (Recommended)

OutboxRelay integrates deeply with Rails through the Engine pattern. Configure via `config.outbox_relay` in `config/application.rb` or environment-specific files:

```ruby
# config/application.rb or config/environments/production.rb
module YourApp
  class Application < Rails::Application
    # OutboxRelay configuration
    config.outbox_relay.polling_interval = 1.0    # Polling interval (seconds)
    config.outbox_relay.batch_size = 100          # Events per batch
    config.outbox_relay.max_loops = 1000          # Worker restart after N loops
    config.outbox_relay.shutdown_timeout = 30     # Graceful shutdown timeout
    config.outbox_relay.silence_polling = true    # Reduce query logs

    # Optional: Custom logger
    config.outbox_relay.logger = Logger.new("log/outbox_relay.log")

    # Optional: Custom error handler (overrides default Rails.error)
    # By default, uses Rails.error for automatic Sentry/Bugsnag integration
    config.outbox_relay.on_thread_error = ->(exception) {
      Sentry.capture_exception(exception,
        level: :error,
        tags: { source: "outbox_relay" }
      )
    }
  end
end
```

**Benefits:**
- ✅ Rails-native configuration pattern
- ✅ Per-environment settings (`production.rb`, `staging.rb`)
- ✅ Automatic Rails error reporting (Rails 7+)
- ✅ Consistent with other Rails gems

### Backward-Compatible Configuration

The original style still works for non-Rails apps or existing setups:

```ruby
# config/initializers/outbox_relay.rb
OutboxRelay.polling_interval = 1.0
OutboxRelay.batch_size = 100
OutboxRelay.max_loops = 1000
OutboxRelay.shutdown_timeout = 30
OutboxRelay.silence_polling = true
```

**Note:** `config.outbox_relay` settings take precedence if both styles are used.

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

**Horizontal Scaling:**
> 💡 **For multi-instance deployments (ECS, Kubernetes)**: Set partition count to match or exceed your desired number of instances. For example, with 5 ECS tasks, use `partitions: 5` or higher. See [Multi-Instance Deployment](#-multi-instance-deployment) for detailed guidance.

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

### Automatic Structured Logging (New!)

OutboxRelay now includes **LogSubscriber** for automatic, unified logging of all operations:

```bash
# Example output with automatic structured logging
[OutboxRelay] Started worker-abc123 (kind: worker, topic: orders, partition: 0)
[OutboxRelay] Batch processed (duration: 123.4ms, events: 10, topic: orders)
[OutboxRelay] Worker stopped (uptime: 3600s, loops: 1000)
```

**Log Levels:**
- `INFO`: Process lifecycle, registration, fork events
- `WARN`: Errors, failures, unexpected conditions
- `DEBUG`: Polling, batches, heartbeats (requires `config.log_level = :debug`)

**Features:**
- ✅ Automatic duration tracking for all operations
- ✅ Colorized output (Rails-style)
- ✅ Consistent structured format
- ✅ No manual logging needed

**Enable Debug Logging:**
```ruby
# config/environments/development.rb
config.log_level = :debug  # See polling and batch details
```

### Manual Logging (Advanced)

For custom logging beyond automatic LogSubscriber:

```ruby
# Worker lifecycle events
OutboxRelay.logger.info(
  event_name: "worker_started",
  consumer_group: "order_updates",
  topic: "order_updates",
  partition_key: 0
)

# Custom metrics
OutboxRelay.logger.debug(
  event_name: "custom_metric",
  metric_name: "batch_latency",
  value: 42.5
)
```

### Automatic Error Reporting (New!)

**Rails 7+ Integration**: OutboxRelay automatically reports all thread errors to your configured error tracker (Sentry, Bugsnag, Rollbar, etc.) via `Rails.error`:

```ruby
# Happens automatically - no configuration needed!
# All OutboxRelay thread errors are reported to:
# - Sentry
# - Bugsnag
# - Rollbar
# - Any Rails.error subscriber

# Errors include:
#   - handled: false (for alerting)
#   - source: "outbox_relay" (for filtering)
#   - Full context (worker name, partition, etc.)
```

**Rails 6 Compatibility**: Automatically falls back to `Rails.logger.error`.

**Custom Error Handler** (optional):
```ruby
# config/application.rb
config.outbox_relay.on_thread_error = ->(exception) {
  # Custom handling with additional context
  Sentry.capture_exception(exception,
    level: :error,
    tags: {
      source: "outbox_relay",
      environment: Rails.env,
      worker_type: Thread.current[:worker_name]
    },
    extra: {
      partition: Thread.current[:partition_key]
    }
  )
}
```

### ActiveSupport::Notifications Instrumentation

OutboxRelay emits all errors and critical events via `ActiveSupport::Notifications`, allowing your application to choose any monitoring backend (Sentry, DataDog, New Relic, etc.).

**Subscribe to all OutboxRelay events:**

```ruby
# config/initializers/outbox_relay_instrumentation.rb

ActiveSupport::Notifications.subscribe(/^outbox_relay\./) do |name, start, finish, id, payload|
  # Route to your monitoring backend
  if payload[:exception]
    Sentry.capture_exception(
      payload[:exception],
      extra: payload.except(:exception),
      level: severity_to_sentry_level(payload[:severity])
    )
  elsif payload[:message]
    Sentry.capture_message(
      payload[:message],
      extra: payload.except(:message),
      level: severity_to_sentry_level(payload[:severity])
    )
  end
end

def severity_to_sentry_level(severity)
  case severity
  when "critical" then :fatal
  when "high" then :error
  when "warning" then :warning
  else :info
  end
end
```

**Subscribe to specific event categories:**

```ruby
# Worker errors only
ActiveSupport::Notifications.subscribe("outbox_relay.worker.poll_error") do |name, start, finish, id, payload|
  Sentry.capture_exception(
    payload[:exception],
    extra: {
      consumer_class: payload[:consumer_class],
      partition_key: payload[:partition_key],
      loop_count: payload[:loop_count]
    }
  )
end

# Heartbeat failures only (with escalation)
ActiveSupport::Notifications.subscribe("outbox_relay.heartbeat.failure") do |name, start, finish, id, payload|
  # payload[:severity] escalates from "warning" to "critical" based on consecutive_failures
  Sentry.capture_exception(
    payload[:exception],
    extra: {
      process_id: payload[:process_id],
      consecutive_failures: payload[:consecutive_failures],
      max_failures: payload[:max_failures]
    },
    level: payload[:severity] == "critical" ? :fatal : :warning
  )
end

# Supervisor events only
ActiveSupport::Notifications.subscribe(/^outbox_relay\.supervisor\./) do |name, start, finish, id, payload|
  # Route supervisor-specific events
end
```

**Available Event Categories:**

```ruby
# Worker Events
outbox_relay.worker.poll_error                    # Critical: Worker processing failure
outbox_relay.worker.delay_calculation_error       # High: Lag query failed

# Heartbeat Events
outbox_relay.heartbeat.failure                    # Warning→Critical: Dynamic escalation
outbox_relay.heartbeat.task_error                 # High: Timer task failure
outbox_relay.heartbeat.start_error                # High: Heartbeat startup failed

# Process Lifecycle Events
outbox_relay.process.registration_failed          # Critical: Process can't register
outbox_relay.process.deregistration_failed        # High: Shutdown cleanup issue
outbox_relay.process.heartbeat_failed             # Warning→Critical: DB connectivity
outbox_relay.process.run_error                    # Error: Lock contention

# Supervisor Events
outbox_relay.supervisor.boot_incomplete           # Error: Failed worker starts
outbox_relay.supervisor.fork_error                # Critical: Fork system error
outbox_relay.supervisor.restart_abandoned         # Error: Excessive restarts

# Poller Events
outbox_relay.poller.poll_error                    # Warning: Polling error
outbox_relay.poller.instrumentation_error         # High: Framework error

# Model Events
outbox_relay.models.error                         # High: Generic model errors

# Task Events
outbox_relay.tasks.error                          # High: Rake task errors

# Callback Events
outbox_relay.callbacks.boot_failed                # Warning: Boot callback failed
outbox_relay.callbacks.shutdown_block_failed      # Warning: Shutdown block failed
outbox_relay.callbacks.shutdown_failed            # Warning: Shutdown callback failed

# Configuration Events
outbox_relay.configuration.partition_count_query_failed  # Critical: Config query failed
```

**Event Payload Structure:**

All events include:
- `exception` or `message`: The error/message
- `severity`: "critical", "high", "warning", or "error"
- `phase`: Operational phase (e.g., "fork", "poll", "shutdown")
- Additional context fields specific to each event type

**DataDog Example:**

```ruby
ActiveSupport::Notifications.subscribe(/^outbox_relay\./) do |name, start, finish, id, payload|
  if payload[:exception]
    Datadog::Tracing.active_trace&.set_error(payload[:exception])

    Datadog.statsd.increment(
      'outbox_relay.error',
      tags: [
        "event:#{name}",
        "severity:#{payload[:severity]}",
        "phase:#{payload[:phase]}"
      ]
    )
  end
end
```

**New Relic Example:**

```ruby
ActiveSupport::Notifications.subscribe(/^outbox_relay\./) do |name, start, finish, id, payload|
  if payload[:exception]
    NewRelic::Agent.notice_error(
      payload[:exception],
      custom_params: payload.except(:exception)
    )
  end
end
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

## 🌐 Multi-Instance Deployment

OutboxRelay is designed for **horizontal scaling** with multiple supervisor instances running simultaneously. This is the recommended pattern for container-based deployments (ECS, Kubernetes, Docker Swarm).

### Architecture

**Multiple supervisors are expected and intentional** in production deployments:

```
ECS/Kubernetes Deployment (3 replicas)
├─ Container 1: Supervisor-1 + Workers [partitions 0,1]
├─ Container 2: Supervisor-2 + Workers [partitions 0,1]
└─ Container 3: Supervisor-3 + Workers [partitions 0,1]

Total: 3 supervisors + 6 workers
```

Each container runs:
- **One supervisor process** - Manages forked workers in its container
- **N worker processes** - One per partition configured for that consumer group

### How Workers Coordinate

Workers across different containers **do not duplicate work** thanks to database-level coordination:

1. **`FOR UPDATE SKIP LOCKED`** - PostgreSQL row-level locks
   - Worker-1 in Container-A locks event #100
   - Worker-2 in Container-B tries to lock same event → skips immediately (no wait)
   - Workers naturally distribute work without blocking each other

2. **Advisory Locks** - Per consumer group deduplication
   - Even if two workers fetch the same event, only one acquires the advisory lock
   - The other worker silently skips and moves to next event
   - Different consumer groups can process the same event independently

3. **Offset Tracking** - Per-partition progress tracking
   - Each partition tracks its own progress per consumer group
   - Workers won't re-fetch already processed events

**Result**: Multiple supervisors cooperate seamlessly without configuration changes.

### Configuration for Multi-Instance Deployments

#### Optimal Partition Count

**Rule of thumb**: Set partition count to **match or exceed** your maximum number of instances.

**Examples:**

**2 ECS Tasks (containers)**
```yaml
topics:
  order_updates:
    partitions: 2  # Perfect - one partition per container

consumer_groups:
  order_processor:
    topics:
      - name: order_updates
        class: "OrderUpdatesConsumer"
        partitions: all
```

Result: Each container processes 1 partition efficiently, no wasted resources.

**10 ECS Tasks (high throughput)**
```yaml
topics:
  order_updates:
    partitions: 10  # One partition per container

  notifications:
    partitions: 20  # Two partitions per container for higher throughput

consumer_groups:
  order_processor:
    topics:
      - name: order_updates
        class: "OrderUpdatesConsumer"
        partitions: all  # All 10 workers process all partitions
```

Result: 10 containers × 1 worker = 10 workers for `order_updates`. Workers use `SKIP LOCKED` to distribute events.

**Autoscaling Scenario**
```yaml
topics:
  high_volume_events:
    partitions: 20  # Support up to 20 instances

consumer_groups:
  processor:
    topics:
      - name: high_volume_events
        class: "EventProcessor"
        partitions: all
```

- **Low traffic**: 2 containers → 4 workers (2 per container) share 20 partitions
- **High traffic**: 20 containers → 40 workers efficiently process all partitions
- **Scaling is seamless** - no configuration changes needed

#### What Happens with Mismatched Counts?

**Scenario: 2 partitions, 10 containers**
```yaml
topics:
  low_volume:
    partitions: 2  # ⚠️ Only 2 partitions

# With 10 ECS tasks:
# - 10 supervisors spawned ✓
# - 20 workers spawned (2 per container) ✓
# - But only 2 partitions to process ⚠️
```

**What happens:**
- All 20 workers compete for the same 2 partitions
- `SKIP LOCKED` ensures no duplication ✓
- **But**: 18 workers often idle, wasting resources ⚠️

**Solution**: Increase partitions to match container count, or reduce container count.

### Deployment Examples

#### ECS (Elastic Container Service)

**Task Definition** with 3 desired tasks:

```json
{
  "family": "outbox-relay",
  "containerDefinitions": [{
    "name": "outbox-relay",
    "image": "your-app:latest",
    "command": ["bundle", "exec", "outbox_relay", "start"],
    "memory": 512,
    "cpu": 256,
    "essential": true
  }]
}
```

**Service Configuration**:
```json
{
  "serviceName": "outbox-relay-service",
  "desiredCount": 3,
  "launchType": "FARGATE",
  "healthCheckGracePeriodSeconds": 60
}
```

**Result**: 3 containers, each with supervisor + workers. Workers coordinate via database.

#### Kubernetes

**Deployment** with 5 replicas:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: outbox-relay
spec:
  replicas: 5  # 5 independent supervisor instances
  selector:
    matchLabels:
      app: outbox-relay
  template:
    metadata:
      labels:
        app: outbox-relay
    spec:
      containers:
      - name: outbox-relay
        image: your-app:latest
        command: ["bundle", "exec", "outbox_relay", "start"]
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "500m"
```

**Configuration** (`config/outbox_consumers.yml`):
```yaml
topics:
  events:
    partitions: 10  # Support up to 10 replicas efficiently

consumer_groups:
  processor:
    topics:
      - name: events
        class: "EventProcessor"
        partitions: all
```

**Result**: 5 pods × 2 workers = 10 workers efficiently processing 10 partitions.

#### Docker Compose (Development/Testing)

```yaml
version: '3.8'
services:
  outbox-relay-1:
    build: .
    command: bundle exec outbox_relay start
    depends_on:
      - postgres
    environment:
      DATABASE_URL: postgresql://postgres@postgres/myapp_development

  outbox-relay-2:
    build: .
    command: bundle exec outbox_relay start
    depends_on:
      - postgres
    environment:
      DATABASE_URL: postgresql://postgres@postgres/myapp_development

  postgres:
    image: postgres:15
    ports:
      - "5432:5432"
```

**Result**: 2 supervisors running locally, coordinating via PostgreSQL.

### Monitoring Multi-Instance Deployments

#### Check All Running Processes

```bash
# Shows all supervisors and workers across all containers
bundle exec rake outbox_relay:status

# Example output:
# Process Registry (last 5 minutes)
# ================================================================================
# SUPERVISORS (3 running)
# --------------------------------------------------------------------------------
#   supervisor-abc123  | PID: 1  | Host: ecs-task-1 | Workers: 2 | Uptime: 2h
#   supervisor-def456  | PID: 1  | Host: ecs-task-2 | Workers: 2 | Uptime: 2h
#   supervisor-ghi789  | PID: 1  | Host: ecs-task-3 | Workers: 2 | Uptime: 1h
#
# WORKERS (6 running)
# --------------------------------------------------------------------------------
#   worker-orders-p0 | PID: 15 | Supervisor: abc123 | Loops: 523
#   worker-orders-p1 | PID: 16 | Supervisor: abc123 | Loops: 498
#   ... (6 total)
```

#### Monitor Event Distribution

```ruby
# Check which workers are actively processing
OutboxRelay::Process
  .where(kind: "worker")
  .where("last_heartbeat_at > ?", 1.minute.ago)
  .group_by(&:hostname)
  .transform_values(&:count)

# => {"ecs-task-1" => 2, "ecs-task-2" => 2, "ecs-task-3" => 2}
```

#### Health Check for Multi-Instance

```ruby
# app/controllers/health_controller.rb
def outbox_relay
  # Check if ANY supervisor is running (not requiring specific count)
  recent_supervisors = OutboxRelay::Process
    .where(kind: "supervisor")
    .where("last_heartbeat_at > ?", 2.minutes.ago)
    .count

  recent_workers = OutboxRelay::Process
    .where(kind: "worker")
    .where("last_heartbeat_at > ?", 2.minutes.ago)
    .count

  healthy = recent_supervisors > 0 && recent_workers > 0

  render json: {
    healthy: healthy,
    supervisors: recent_supervisors,
    workers: recent_workers,
    message: healthy ? "OutboxRelay operating normally" : "No active workers"
  }, status: healthy ? 200 : 503
end
```

### Best Practices

✅ **DO:**
- Set partition count to match or exceed maximum number of instances
- Use ECS/Kubernetes autoscaling - OutboxRelay handles it automatically
- Monitor process registry health (`rake outbox_relay:process_status`)
- Schedule dead process cleanup: `*/5 * * * * rake outbox_relay:prune_processes`

❌ **DON'T:**
- Try to enforce single supervisor (breaks horizontal scaling)
- Use file-based pidfiles for singleton (doesn't work across containers)
- Worry about seeing multiple supervisors in logs (this is correct!)
- Set partitions lower than your instance count (wastes resources)

### FAQ

**Q: I see 10 supervisors in my process registry. Is this a bug?**
A: No! With 10 ECS tasks, you should see 10 supervisors. Each container runs one supervisor.

**Q: Won't this cause duplicate event processing?**
A: No. Workers coordinate via `FOR UPDATE SKIP LOCKED` and advisory locks at the database level.

**Q: Should I limit to one supervisor?**
A: No. That defeats horizontal scaling. OutboxRelay is designed for multiple supervisors.

**Q: How do I scale up?**
A: Just increase ECS desired count or Kubernetes replicas. No configuration changes needed (if partitions ≥ instances).

**Q: What if one container dies?**
A: Other containers continue processing. The dead supervisor's workers are cleaned up by `rake outbox_relay:prune_processes`.

**Q: Can different containers process different partitions?**
A: By default, all containers process all partitions (coordinated by SKIP LOCKED). For dedicated partition assignment, use partition configuration:

```yaml
consumer_groups:
  processor_group_a:
    topics:
      - name: events
        partitions: [0, 1, 2]  # Container group A

  processor_group_b:
    topics:
      - name: events
        partitions: [3, 4, 5]  # Container group B
```

Then deploy with environment variables to load different consumer groups per container.

## 🎯 Partition Claiming (v0.8.0+)

OutboxRelay v0.8.0 introduces **Partition Claiming** - a database-backed distributed locking mechanism that ensures only one worker processes each partition at any given time. This eliminates duplicate processing and reduces resource waste in multi-instance deployments.

### The Problem

With `SKIP LOCKED`, multiple workers can spawn for the same partition across different ECS/Kubernetes instances:

```
Without Partition Claiming:
┌─────────────────┐      ┌─────────────────┐
│   ECS Task 1    │      │   ECS Task 2    │
│                 │      │                 │
│ Worker p0 ✓     │      │ Worker p0 ✓     │  ← Both spawn!
│ Worker p1 ✓     │      │ Worker p1 ✓     │  ← Both spawn!
│ Worker p2 ✓     │      │ Worker p2 ✓     │  ← Both spawn!
│ Worker p3 ✓     │      │ Worker p3 ✓     │  ← Both spawn!
└─────────────────┘      └─────────────────┘
Total: 8 workers for 4 partitions (50% waste)
```

While `SKIP LOCKED` prevents duplicate event processing, the redundant workers still:
- Consume CPU cycles polling the database
- Hold database connections
- Create lock contention
- Add unnecessary infrastructure cost

### The Solution

Partition Claiming ensures exactly one worker per partition:

```
With Partition Claiming:
┌─────────────────┐      ┌─────────────────┐
│   ECS Task 1    │      │   ECS Task 2    │
│                 │      │                 │
│ Worker p0 ✅    │      │ Worker p0 exit  │  ← Graceful exit
│ Worker p1 ✅    │      │ Worker p1 exit  │  ← Graceful exit
│ Worker p2 exit  │      │ Worker p2 ✅    │  ← Claims partition
│ Worker p3 exit  │      │ Worker p3 ✅    │  ← Claims partition
└─────────────────┘      └─────────────────┘
Total: 4 workers for 4 partitions (100% efficiency)
```

### How It Works

1. **Boot-time Claiming**: When a worker starts, it attempts to claim its partition by writing to `consumer_offsets`
2. **TTL-based Leases**: Claims expire after 30 seconds if not renewed
3. **Heartbeat Renewal**: Active workers renew their claims every 10 seconds via heartbeat
4. **Graceful Exit**: Workers that can't claim exit with code 0 (supervisor doesn't restart immediately)
5. **Automatic Failover**: If a worker dies, claim expires after 30s and another worker takes over

### Database Schema

The feature uses two columns on `outbox_relay_consumer_offsets`:

```ruby
# Added in v0.8.0 migration
t.string :claimed_by       # consumer_instance_id of claiming worker
t.timestamp :claimed_until # TTL expiration timestamp

add_index :outbox_relay_consumer_offsets,
          [:claimed_by, :claimed_until],
          name: "idx_consumer_offset_claim"
```

### Migration for Existing Installations

If upgrading from v0.7.x, create a migration:

```ruby
class AddPartitionClaimingToOutboxRelayConsumerOffsets < ActiveRecord::Migration[7.0]
  disable_ddl_transaction!

  def change
    safety_assured do
      add_column :outbox_relay_consumer_offsets, :claimed_by, :string
      add_column :outbox_relay_consumer_offsets, :claimed_until, :timestamp
    end

    add_index :outbox_relay_consumer_offsets,
              [:claimed_by, :claimed_until],
              name: "idx_consumer_offset_claim",
              algorithm: :concurrently
  end
end
```

### Observability

New log events for partition claiming:

```bash
# Successful claim
INFO partition_claimed consumer_group="orders" partition_key=0
     consumer_instance_id="orders-host1-12345-p0" claimed_until="2024-01-15T10:30:30Z"

# Failed claim (another worker holds it)
WARN partition_claim_failed consumer_group="orders" partition_key=0
     claimed_by="orders-host2-67890-p0" claimed_until="2024-01-15T10:30:30Z"

# Graceful exit
INFO worker_exiting_claim_unavailable consumer_group="orders" partition_key=0
     message="Partition already claimed by another worker. Exiting gracefully."

# Claim released on shutdown
INFO partition_claim_released consumer_group="orders" partition_key=0

# Claim lost (another worker stole it - worker stops)
ERROR partition_claim_lost consumer_group="orders" partition_key=0
      current_holder="orders-host3-11111-p0"
```

### Configuration

**No configuration required!** Partition claiming is automatic when columns exist.

The TTL (30 seconds) and renewal interval (10 seconds via heartbeat) are designed for reliability:
- 30s TTL gives ample time for failover detection
- 10s renewal (via existing heartbeat) ensures claims don't expire during normal operation
- 3x safety margin (10s renewal vs 30s expiry) handles transient issues

### SKIP LOCKED vs Partition Claiming

| Feature | SKIP LOCKED | Partition Claiming |
|---------|------------|-------------------|
| **Scope** | Event-level | Partition-level |
| **Prevents** | Duplicate event processing | Duplicate workers |
| **Workers spawned** | All | Only needed |
| **Resource usage** | Higher (redundant workers) | Optimal |
| **Mechanism** | Row lock during fetch | TTL-based lease |
| **Still needed?** | Yes (defense in depth) | Works together |

Both mechanisms work together:
1. **Partition Claiming** prevents redundant workers from spawning
2. **SKIP LOCKED** provides defense-in-depth for race conditions within the claim window

### Troubleshooting

**Workers keep exiting immediately:**
- Check logs for `partition_claim_failed` - another instance likely holds the claim
- This is expected behavior! Only one worker per partition should run.

**All workers exiting (none processing):**
- Check if claims are stuck from a crashed instance
- Wait 30 seconds for TTL expiration, or manually clear:
  ```ruby
  OutboxRelay::ConsumerOffset.update_all(claimed_by: nil, claimed_until: nil)
  ```

**Check current claims:**
```ruby
OutboxRelay::ConsumerOffset.all.each do |o|
  puts "#{o.consumer_group}: claimed_by=#{o.claimed_by}, until=#{o.claimed_until}"
end
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
