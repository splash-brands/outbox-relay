# Retriable Exceptions in OutboxRelay

OutboxRelay supports **retriable exceptions** - transient errors (like rate limiting) that should trigger automatic retry with backoff instead of immediately going to the Dead Letter Queue (DLQ).

## Problem

When consuming events that call external APIs (like ShipStation, Stripe, etc.), you may encounter rate limiting:

```ruby
# Without retriable exceptions:
# 1. Event processing fails with Prop::RateLimited
# 2. Event immediately goes to DLQ
# 3. DLQ retry happens later, might hit rate limit again
# 4. Cycle repeats until max_retries exceeded
```

This wastes DLQ retries on transient errors that would succeed if we just waited a few seconds.

## Solution

Override hook methods in your consumer to handle rate limiting gracefully:

```ruby
class Shipments::OrderLifecycleConsumer < OutboxRelay::OutboxConsumer
  def initialize(partition_key: 0)
    super(
      consumer_group: "shipstation_order_fulfillment",
      topic: "order_lifecycle",
      partition_key: partition_key,
      dead_letter_config: { max_retries: 3 },
    )
  end

  def consume_message(event)
    case event.event_name
    when "marked_as_ordered"
      OrderFulfillment::ProcessMarkedAsOrderedService.new(event.payload, custom_logger).call
    end
    true
  end

  # Hook 1: Identify retriable exceptions
  def retriable_exception?(exception)
    exception.is_a?(Prop::RateLimited)
  end

  # Hook 2: Determine retry delay (optional - default uses exception.retry_after or 60s)
  def retry_delay_for(exception)
    case exception
    when Prop::RateLimited
      exception.retry_after  # Prop provides exact wait time
    else
      60  # Default fallback
    end
  end

  # Hook 3: Max retry attempts (optional - default is 5)
  def max_retriable_attempts
    5
  end
end
```

## How It Works

```
Event Processing Flow:
┌─────────────────────────────────────────────────────────────────┐
│  consume_message(event)                                          │
│       ↓                                                          │
│  [Exception raised]                                              │
│       ↓                                                          │
│  retriable_exception?(e) → true?                                 │
│       ↓ YES                    ↓ NO                              │
│  attempt < max_retriable?      → handle_event_failure (DLQ)      │
│       ↓ YES         ↓ NO                                         │
│  sleep(retry_delay) → handle_event_failure (DLQ)                 │
│       ↓                                                          │
│  retry consume_message                                           │
└─────────────────────────────────────────────────────────────────┘
```

## Hook Methods Reference

### `retriable_exception?(exception)`

Determines if an exception should trigger retry with backoff.

```ruby
# Default implementation (always returns false)
def retriable_exception?(exception)
  false
end

# Example override
def retriable_exception?(exception)
  exception.is_a?(Prop::RateLimited) ||
    exception.is_a?(Faraday::TimeoutError) ||
    exception.is_a?(Net::OpenTimeout)
end
```

**Returns:** `true` to retry with backoff, `false` for normal DLQ handling

### `retry_delay_for(exception)`

Determines how long to wait before retrying.

```ruby
# Default implementation
def retry_delay_for(exception)
  delay = if exception.respond_to?(:retry_after)
    exception.retry_after
  else
    60
  end
  [delay.to_i, 300].min  # Cap at 5 minutes
end

# Example override
def retry_delay_for(exception)
  case exception
  when Prop::RateLimited
    exception.retry_after  # Use API's recommendation
  when Faraday::TimeoutError
    5  # Short delay for timeouts
  else
    60  # Default
  end
end
```

**Returns:** Seconds to sleep before retry (capped at 300 seconds by default)

### `max_retriable_attempts`

Maximum number of retry attempts before falling through to DLQ.

```ruby
# Default implementation
def max_retriable_attempts
  5
end

# Example override (more attempts for rate-limited APIs)
def max_retriable_attempts
  10
end
```

**Returns:** Integer number of attempts

## Logging

When retriable exceptions occur, OutboxRelay logs:

```ruby
# Each retry attempt:
{
  event_name: "retriable_exception_waiting",
  event_id: "abc-123",
  sequence: 42,
  consumer_group: "my_consumer",
  error_class: "Prop::RateLimited",
  error: "Rate limit exceeded",
  attempt: 1,
  max_attempts: 5,
  retry_delay: 60
}

# After max attempts exceeded:
{
  event_name: "consume_message_failed",
  retriable_attempts_exhausted: true,
  # ... standard error fields
}
```

## Best Practices

### 1. Only Mark Truly Transient Errors as Retriable

```ruby
# GOOD: Rate limiting is transient
def retriable_exception?(e)
  e.is_a?(Prop::RateLimited)
end

# BAD: Don't retry validation errors - they'll never succeed
def retriable_exception?(e)
  e.is_a?(ActiveRecord::RecordInvalid)  # Don't do this!
end
```

### 2. Respect API Rate Limits

```ruby
def retry_delay_for(exception)
  case exception
  when Prop::RateLimited
    # Use the delay from Prop - it knows the exact reset time
    exception.retry_after
  end
end
```

### 3. Cap Max Attempts Appropriately

```ruby
# For APIs with short rate limit windows (1 minute)
def max_retriable_attempts
  5  # ~5 minutes max total wait
end

# For APIs with long rate limit windows (1 day)
def max_retriable_attempts
  3  # Don't wait forever
end
```

### 4. Consider Consumer Blocking

Retriable exceptions cause the consumer to **sleep**, blocking that worker. For high-throughput scenarios, consider:

- Lower `max_retriable_attempts`
- Shorter `retry_delay_for` values
- More worker processes

## Interaction with DLQ

Retriable attempts are **not counted** as DLQ retries:

- 5 retriable attempts (rate limiting)
- Then goes to DLQ with `total_retries: 1`
- DLQ retry happens
- 5 more retriable attempts
- Then `total_retries: 2`
- etc.

This separates transient errors from actual processing failures.

## Common Rate Limiting Gems

### Prop (Zendesk)

```ruby
def retriable_exception?(e)
  e.is_a?(Prop::RateLimited)
end

def retry_delay_for(e)
  e.retry_after if e.is_a?(Prop::RateLimited)
end
```

### Rack::Attack

```ruby
def retriable_exception?(e)
  e.is_a?(Rack::Attack::Throttled)
end
```

### Custom Rate Limiting

```ruby
class RateLimitExceeded < StandardError
  attr_reader :retry_after

  def initialize(message, retry_after:)
    super(message)
    @retry_after = retry_after
  end
end

def retriable_exception?(e)
  e.is_a?(RateLimitExceeded)
end
```
