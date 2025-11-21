# Rails Configuration Migration Guide

OutboxRelay now supports Rails-style configuration through `config.outbox_relay` (OrderedOptions pattern).

This provides better Rails integration and allows configuring OutboxRelay alongside other Rails settings.

## 🔄 Migration Steps

### Before (Direct Module Configuration)

```ruby
# config/initializers/outbox_relay.rb
OutboxRelay.polling_interval = 2.0
OutboxRelay.batch_size = 200
OutboxRelay.max_loops = 5000
OutboxRelay.silence_polling = true
```

### After (Rails-style OrderedOptions) ✅ RECOMMENDED

```ruby
# config/application.rb or config/environments/production.rb
module YourApp
  class Application < Rails::Application
    # ...

    # OutboxRelay configuration
    config.outbox_relay.polling_interval = 2.0
    config.outbox_relay.batch_size = 200
    config.outbox_relay.max_loops = 5000
    config.outbox_relay.silence_polling = true

    # Optional: Custom logger
    config.outbox_relay.logger = Logger.new("log/outbox_relay.log")

    # Optional: Custom error handler (overrides default Rails.error)
    config.outbox_relay.on_thread_error = ->(exception) {
      Sentry.capture_exception(exception, level: :error, tags: { source: "outbox_relay" })
    }
  end
end
```

## 📊 Available Configuration Options

All existing `OutboxRelay` settings can now be configured via `config.outbox_relay`:

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `polling_interval` | Float | `1.0` | Seconds between polls for new events |
| `batch_size` | Integer | `100` | Max events per batch |
| `max_loops` | Integer | `1000` | Worker restarts after N loops |
| `shutdown_timeout` | Integer | `30` | Seconds to wait for graceful shutdown |
| `silence_polling` | Boolean | `true` | Reduce log noise during polling |
| `logger` | Logger | `Rails.logger` | Custom logger instance |
| `app_executor` | Executor | `app.executor` | Rails executor for context management |
| `on_thread_error` | Proc | Rails.error handler | Error callback |

## 🎯 New Features

### 1. Automatic Rails Error Reporting (Rails 7+)

OutboxRelay now automatically reports errors to your configured error tracker (Sentry, Bugsnag, etc.) via `Rails.error`:

```ruby
# Happens automatically - no configuration needed!
# Errors are reported with:
#   - handled: false (for alerting)
#   - source: "outbox_relay" (for filtering)
```

**For Rails 6:** Automatically falls back to `Rails.logger.error`.

**Custom Error Handler:**
```ruby
config.outbox_relay.on_thread_error = ->(exception) {
  # Custom handling
  Sentry.capture_exception(exception,
    level: :error,
    tags: {
      source: "outbox_relay",
      worker_type: Thread.current[:worker_name]
    }
  )
}
```

### 2. Structured Logging with LogSubscriber

All OutboxRelay operations now log automatically with structured format:

```
[OutboxRelay] Started worker-abc123 (kind: worker, topic: orders, partition: 0)
[OutboxRelay] Batch processed (duration: 123.4ms, events: 10, topic: orders)
[OutboxRelay] Worker stopped (uptime: 3600s, loops: 1000)
```

**Log Levels:**
- `INFO`: Process lifecycle, registration, fork events
- `WARN`: Errors, failures, unexpected conditions
- `DEBUG`: Polling, batches, heartbeats (noisy, requires `Rails.logger.level = :debug`)

**Enable Debug Logging:**
```ruby
# config/environments/development.rb
config.log_level = :debug
```

### 3. Per-Environment Configuration

Now you can configure OutboxRelay differently per environment:

```ruby
# config/environments/production.rb
config.outbox_relay.polling_interval = 1.0
config.outbox_relay.batch_size = 200

# config/environments/development.rb
config.outbox_relay.polling_interval = 5.0  # Slower in dev
config.outbox_relay.batch_size = 10         # Smaller batches
```

## 🔧 Backward Compatibility

**Good news:** The old style still works! Both patterns are supported:

```ruby
# Old style - still works
OutboxRelay.polling_interval = 2.0

# New style - recommended
config.outbox_relay.polling_interval = 2.0
```

However, `config.outbox_relay` settings take precedence if both are set.

## ✅ Recommended Migration Path

1. **Move settings from initializer to `config/application.rb`**
   - Easier to see all Rails configuration in one place
   - Per-environment configuration becomes natural

2. **Remove custom `on_thread_error` if using default error tracker**
   - Rails.error integration is automatic now
   - Only keep custom handler if you have special requirements

3. **Test in development first**
   ```bash
   # Should see structured logs with [OutboxRelay] prefix
   ./bin/outbox_relay
   ```

4. **Deploy to staging/production**
   - Monitor error tracker for OutboxRelay errors (should appear automatically)
   - Check logs for structured output

## 📝 Example: Full Migration

### Before
```ruby
# config/initializers/outbox_relay.rb
OutboxRelay.polling_interval = 2.0
OutboxRelay.batch_size = 200
OutboxRelay.silence_polling = true

OutboxRelay.on_thread_error = ->(exception) {
  Sentry.capture_exception(exception)
}
```

### After
```ruby
# config/application.rb
module YourApp
  class Application < Rails::Application
    config.outbox_relay.polling_interval = 2.0
    config.outbox_relay.batch_size = 200
    config.outbox_relay.silence_polling = true

    # Remove custom error handler - Rails.error is now automatic!
    # config.outbox_relay.on_thread_error = ...  ← DELETE THIS
  end
end

# Delete config/initializers/outbox_relay.rb
```

## 🐛 Troubleshooting

### Logs show duplicate entries?
- Check if you have both old-style manual logging AND LogSubscriber enabled
- LogSubscriber is automatic now, remove manual `logger.info` calls

### Errors not appearing in Sentry/Bugsnag?
- Ensure Rails 7+ OR custom `on_thread_error` is configured
- Check that your error tracker is properly initialized
- Verify with a test error: `OutboxRelay.on_thread_error.call(StandardError.new("test"))`

### Configuration not applied?
- Ensure settings are in `config.outbox_relay`, not `config.outbox` (typo)
- Settings must be in `config/application.rb` or `config/environments/*.rb`
- Initializers (`config/initializers/`) run AFTER engine initializers

## 🎉 Benefits Summary

✅ **Rails-native configuration** - follows Rails conventions
✅ **Automatic error reporting** - Sentry/Bugsnag integration out of the box
✅ **Structured logging** - consistent, colorful, with duration tracking
✅ **Per-environment config** - different settings for dev/prod
✅ **Backward compatible** - old style still works
✅ **Less boilerplate** - sensible defaults, less code to write
