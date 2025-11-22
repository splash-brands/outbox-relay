# frozen_string_literal: true

module OutboxRelay
  # OutboxConsumer - Base class for PostgreSQL-based message queue consumers
  # Replacement for Karafka::BaseConsumer
  #
  # Usage:
  #   class MyConsumer < OutboxRelay::OutboxConsumer
#     def initialize(partition_key:)
#       super(
#         consumer_group: "my_group",
#         topic: "my_topic",
#         partition_key: partition_key,  # REQUIRED: Partition number (0-based) to process
#         event_filter: ["created", "updated"],  # Optional: filter by event_name
#         dead_letter_config: { max_retries: 2 },  # Optional: DLQ configuration
#       )
#     end
#
#     def consume_message(event)
#       # Process event
#     end
#   end
#
# Partition Key:
# - Required parameter that determines which partition this consumer processes
# - Each partition is processed independently for parallelism
# - Partition number should be 0 to (partition_count - 1)
# - Orchestrator spawns one consumer instance per partition
  class OutboxConsumer
    attr_reader :consumer_group, :topic, :event_filter, :dead_letter_config, :partition_key

    def initialize(consumer_group:, topic:, partition_key:, event_filter: nil, dead_letter_config: {})
      @consumer_group = consumer_group
      @topic = topic
      @partition_key = partition_key
      @event_filter = Array.wrap(event_filter).compact
      @dead_letter_config = dead_letter_config
      @logger = OutboxRelay.logger
      @consumer_instance_id = build_consumer_instance_id
    end

  # Main consumption loop
  def consume_batch(batch_size: 50)
    events = fetch_batch(batch_size)
    return 0 if events.empty?

    processed_count = 0

    events.each do |event|
      process_event(event)
      processed_count += 1
    rescue ActiveRecord::ConnectionNotEstablished, PG::Error => db_error
      # Database connectivity issue - stop processing this batch
      @logger.error(
        event_name: "database_connectivity_error",
        event_id: event.event_id,
        error: db_error.message,
        backtrace: db_error.backtrace&.first(10)&.join("\n"),
      )
      raise # Stop processing batch - let supervisor restart worker
    rescue ActiveRecord::RecordInvalid, ActiveRecord::StatementInvalid => validation_error
      # Invalid data or constraint violation - this is a bug
      @logger.error(
        event_name: "critical_event_validation_error",
        event_id: event.event_id,
        consumer_group: consumer_group,
        error: validation_error.message,
        backtrace: validation_error.backtrace&.first(10)&.join("\n"),
      )

      OutboxRelay::Instrumentation::Models.error(
        validation_error,
        model: "OutboxConsumer",
        operation: "consume_event_validation",
        event_id: event.event_id,
        consumer_group: consumer_group,
        severity: "critical"
      )

      # Add to DLQ immediately - don't retry validation errors
      move_to_dead_letter_queue(event, validation_error)
      # Don't increment processed_count for failures
    rescue StandardError => e
      # Application-level errors in consume_message
      # Only catch errors from consumer logic, not infrastructure
      if recoverable_error?(e)
        handle_error(event, e)
        # Don't increment processed_count for failures
      else
        # System-level error - stop processing
        @logger.error(
          event_name: "critical_system_error_in_consumer",
          event_id: event.event_id,
          error_class: e.class.name,
          error: e.message,
          backtrace: e.backtrace&.first(10)&.join("\n"),
        )

        OutboxRelay::Instrumentation::Models.error(
          e,
          model: "OutboxConsumer",
          operation: "consume_event_system_error",
          event_id: event.event_id
        )

        raise # Stop processing - let supervisor handle
      end
    end

    processed_count
  end

  # Consume all pending events
  def consume_all(batch_size: 100)
    total_processed = 0

    loop do
      count = consume_batch(batch_size: batch_size)
      total_processed += count
      break if count.zero?

      # Small delay to prevent tight loop
      sleep(0.01) if count < batch_size
    end

    total_processed
  end

  # Check if there are pending events
  def pending_events?
    fetch_batch(1).any?
  end

  # Get consumer lag for THIS partition only (partition-aware)
  # This is the accurate lag metric for partition-specific workers
  #
  # NOTE: ConsumerOffset#lag calculates GLOBAL lag across all partitions,
  # which is incorrect for partition-specific consumers. Always use this method
  # for per-worker lag calculation.
  #
  # Use case: Dynamic delay calculation, monitoring dashboards
  def lag
    # Get latest sequence for this specific partition
    latest_sequence = OutboxRelay::OutboxEvent.where(topic: topic, partition_key: partition_key).maximum(:sequence) || 0
    latest_sequence - current_offset.last_consumed_sequence
  end

  private

  def fetch_batch(batch_size)
    # Get current offset
    offset = current_offset

    # Get IDs of events in DLQ for this consumer group (with caching)
    # These events should not be fetched again by this consumer group
    dlq_event_ids = fetch_dlq_event_ids

    # Fetch events after current offset, excluding DLQ events for this consumer group
    query = OutboxRelay::OutboxEvent
      .where(topic: topic)
      .where("sequence > ?", offset.last_consumed_sequence)
      .where.not(id: dlq_event_ids)
      .not_expired
      .by_sequence
      .limit(batch_size)

    # Apply partition filtering for parallel processing
    # Each worker processes only its assigned partition
    query = query.where(partition_key: partition_key)

    # Apply event filtering (matches Karafka's MarkingEventTypeFilter)
    query = query.where(event_name: event_filter) if event_filter.present?

    # Add FOR UPDATE SKIP LOCKED for efficient multi-worker operation
    # This allows workers to grab different events in parallel instead of competing
    # NOTE: This changes query semantics - events locked by other transactions are skipped
    query = query.lock("FOR UPDATE SKIP LOCKED")

    query.to_a
  end

  # Cache DLQ event IDs to avoid subquery on every poll
  # DLQ list changes infrequently but is queried constantly (every 10ms-1s)
  def fetch_dlq_event_ids
    @dlq_event_ids_cache ||= begin
      @dlq_cache_expires_at = Time.current + 5.seconds
      load_dlq_event_ids
    end

    if Time.current > @dlq_cache_expires_at
      @dlq_event_ids_cache = load_dlq_event_ids
      @dlq_cache_expires_at = Time.current + 5.seconds
    end

    @dlq_event_ids_cache
  end

  def load_dlq_event_ids
    OutboxRelay::DeadLetterEvent
      .where(consumer_group: consumer_group)
      .where(resolution_status: ["unresolved", "retrying"])
      .pluck(:outbox_relay_outbox_event_id)
  end

  # Advisory lock methods for duplicate prevention
  # ============================================================================
  # PostgreSQL advisory locks replace optimistic locking for concurrent safety.
  #
  # ## Why Advisory Locks?
  #
  # Traditional approaches don't work well for high-throughput event processing:
  #
  # 1. **Optimistic Locking** (version column):
  #    - Creates UPDATE contention on event records
  #    - Events are immutable facts - shouldn't be modified
  #    - Doesn't scale with multiple workers
  #
  # 2. **Database Transactions + SELECT FOR UPDATE**:
  #    - Long-running transactions block other workers
  #    - Lock escalation under high load
  #
  # 3. **Application-level Locks** (Redis, etc.):
  #    - Adds external dependency
  #    - Network calls for every event
  #
  # ## Advisory Lock Algorithm
  #
  # PostgreSQL advisory locks are:
  #   - Lightweight (no row locking)
  #   - Automatically released on connection close or transaction COMMIT/ROLLBACK
  #   - Never escalate to table locks
  #   - Session or transaction-scoped
  #
  # We use **transaction-scoped** locks (pg_try_advisory_xact_lock):
  #   - Lock acquired within transaction
  #   - Lock automatically released on COMMIT or ROLLBACK
  #   - No need for explicit unlock
  #   - Safe even if worker crashes mid-processing
  #
  # ## Lock Key Design
  #
  # Lock key must be unique per (event, consumer_group) to allow:
  #   - Multiple consumer groups processing same event (different lock keys)
  #   - Same consumer group NOT processing same event twice (same lock key)
  #
  # We use a 64-bit integer lock key:
  #   - High 32 bits: event sequence number (supports 4 billion events)
  #   - Low 32 bits: CRC32 hash of consumer_group name
  #
  # Example:
  #   Event sequence: 12345, Consumer group: "notifications"
  #   Lock key: (12345 << 32) | CRC32("notifications")
  #
  # ## Concurrency Example
  #
  # Given event 123 and two workers (Worker A, Worker B) in same consumer group:
  #
  #   Time | Worker A                    | Worker B
  #   -----|----------------------------|---------------------------
  #   T1   | BEGIN                      | BEGIN
  #   T2   | FETCH event 123            | FETCH event 123
  #   T3   | TRY_LOCK(123) → SUCCESS    | TRY_LOCK(123) → FAIL
  #   T4   | Process event              | Skip event (already locked)
  #   T5   | Update offset → COMMIT     | Fetch next event
  #   T6   | Lock auto-released         |
  #
  # Result: Event processed exactly once by Worker A.
  #
  # ## Edge Cases
  #
  # 1. **Worker crashes mid-processing**: Transaction rolls back → lock released
  # 2. **Database connection lost**: All locks auto-released
  # 3. **Hash collision** (extremely rare): CRC32 has 1 in 4 billion collision rate
  #    Impact: Different consumer groups might temporarily block each other
  # 4. **Sequence wraparound**: After 4 billion events, sequences repeat
  #    Impact: None - old events already processed and cleaned up
  #
  def advisory_lock_key(event)
    # Combine event sequence and consumer group into 64-bit lock key
    # High 32 bits: event sequence (max 4 billion events)
    # Low 32 bits: CRC32 hash of consumer group name
    event_sequence = event.sequence & 0xFFFFFFFF
    group_hash = Zlib.crc32(consumer_group) & 0xFFFFFFFF
    (event_sequence << 32) | group_hash
  end

  def acquire_advisory_lock(lock_key)
    # Try to acquire transaction-level advisory lock (pg_try_advisory_xact_lock)
    # Returns true if lock acquired, false if already locked by another session
    #
    # Lock lifecycle:
    # - Automatically released on COMMIT, ROLLBACK, or connection close
    # - Session-scoped: Worker crash or SIGKILL releases lock immediately
    # - Safe for fork-based architecture: No manual cleanup needed

    # Validate lock_key is a valid integer
    unless lock_key.is_a?(Integer)
      raise ArgumentError, "lock_key must be an integer, got #{lock_key.class}"
    end

    # Use parameterized query to prevent SQL injection
    result = ActiveRecord::Base.connection.execute(
      ActiveRecord::Base.sanitize_sql_array([
        "SELECT pg_try_advisory_xact_lock(?)",
        lock_key
      ])
    ).first

    if result.nil?
      @logger.error(
        event_name: "advisory_lock_query_returned_nil",
        lock_key: lock_key
      )
      return false  # Fail safe - don't process event
    end

    lock_acquired = result["pg_try_advisory_xact_lock"]

    if lock_acquired.nil?
      @logger.error(
        event_name: "advisory_lock_result_unexpected_format",
        lock_key: lock_key,
        result: result.inspect
      )
      return false  # Fail safe
    end

    lock_acquired == true

  rescue => e
    @logger.error(
      event_name: "advisory_lock_failed",
      lock_key: lock_key,
      error: e.message,
      backtrace: e.backtrace&.first(10)&.join("\n")
    )

    OutboxRelay::Instrumentation::Models.error(
      e,
      model: "OutboxConsumer",
      operation: "advisory_lock",
      lock_key: lock_key,
      consumer_group: consumer_group
    )

    # Fail safe - if we can't acquire lock, don't process
    false
  end

  def process_event(event)
    # Wrap in transaction for atomicity and concurrency safety
    #
    # CRITICAL: The transaction ensures:
    #   1. Advisory lock prevents duplicate processing within same consumer group
    #   2. Offset is updated atomically with message processing
    #   3. Lock is automatically released on COMMIT or ROLLBACK
    #
    # Multi-consumer support:
    # - Advisory lock key includes consumer group in the hash
    # - Different consumer groups get different lock keys for same event
    # - Consumer group A can process event 123 while group B processes it too
    #
    # Race condition prevention within same consumer group:
    # - Multiple workers from SAME group can fetch same event from database
    # - But only ONE succeeds at acquiring advisory lock
    # - Failed worker skips silently (lock already held)
    # - Database-level uniqueness on sequence prevents offset corruption
    #
    # Example concurrent execution (same consumer group):
    #   Worker A: fetch event 123 → acquire lock → SUCCESS → process
    #   Worker B: fetch event 123 → acquire lock → FAIL → skip silently
    #   Worker B moves to next event, Worker A continues processing
    ActiveRecord::Base.transaction do
      # Calculate advisory lock key (unique per event + consumer group)
      lock_key = advisory_lock_key(event)

      # Try to acquire lock - returns false if another worker already holds it
      unless acquire_advisory_lock(lock_key)
        @logger.debug(
          event_name: "event_already_being_processed",
          event_id: event.event_id,
          sequence: event.sequence,
          consumer_group: consumer_group,
        )
        return false # Skip this event, another worker is processing it
      end

      # Delegate to subclass for actual processing
      begin
        consume_message(event)
      rescue => e
        @logger.error(
          event_name: "consume_message_failed",
          event_id: event.event_id,
          sequence: event.sequence,
          consumer_group: consumer_group,
          error: e.message,
          error_class: e.class.name,
          backtrace: e.backtrace&.first(10)&.join("\n")
        )

        OutboxRelay::Instrumentation::Models.error(
          e,
          model: "OutboxConsumer",
          operation: "consume_message",
          event_id: event.event_id,
          sequence: event.sequence,
          consumer_group: consumer_group,
          phase: "consume_message"
        )

        # Record failure in DLQ for retry/monitoring
        handle_event_failure(event, e)

        # Re-raise to abort transaction - UNRECOVERABLE because:
        # 1. Event processing failed - can't mark as successfully processed
        # 2. Transaction rollback releases advisory lock for retry
        # 3. Offset not updated - event will be retried (with backoff via DLQ)
        raise
      end

      # Update offset atomically (lock auto-released on COMMIT)
      begin
        offset_updated = update_offset(event)

        # Log stale offset updates for visibility (Kafka-style out-of-order completion)
        unless offset_updated
          @logger.debug(
            event_name: "stale_offset_skipped",
            event_id: event.event_id,
            sequence: event.sequence,
            current_offset: current_offset.last_consumed_sequence,
            consumer_group: consumer_group,
            message: "Event processed successfully but offset not updated (out-of-order completion)"
          )
        end
      rescue => e
        # Offset update failures are CRITICAL - they indicate infrastructure issues
        # and can cause event reprocessing or offset corruption
        #
        # UNRECOVERABLE because:
        # 1. Message WAS successfully processed (can't undo side effects)
        # 2. But we can't record progress (offset update failed)
        # 3. Re-processing would cause duplicates (idempotency required)
        # 4. This indicates database issues, not business logic errors
        @logger.error(
          event_name: "offset_update_failed",
          event_id: event.event_id,
          sequence: event.sequence,
          consumer_group: consumer_group,
          error: e.message,
          error_class: e.class.name,
          backtrace: e.backtrace&.first(10)&.join("\n"),
          severity: "critical"
        )

        OutboxRelay::Instrumentation::Models.error(
          e,
          model: "OutboxConsumer",
          operation: "offset_update",
          event_id: event.event_id,
          sequence: event.sequence,
          consumer_group: consumer_group,
          phase: "offset_update",
          severity: "critical"
        )

        # Don't call handle_event_failure - message WAS processed successfully
        # This is a tracking failure, not a business logic failure
        raise # Re-raise to abort transaction and retry entire event
      end
    end

    true # Successfully processed
  end

  def consume_message(_event)
    # To be implemented by subclasses
    raise NotImplementedError, "Subclasses must implement consume_message"
  end

  def current_offset
    # Each partition has its own offset tracking
    # This allows parallel processing without conflicts
    @current_offset ||= OutboxRelay::ConsumerOffset.find_or_initialize_for(
      consumer_group: consumer_group_with_partition,
      topic: topic,
    ).tap do |offset|
      offset.consumer_instance_id = @consumer_instance_id
      # Set heartbeat on new records to indicate this consumer is active
      offset.heartbeat_at = Time.current if offset.new_record?
      # Always save to persist consumer_instance_id before locking
      offset.save!
    end
  end

  def consumer_group_with_partition
    "#{consumer_group}_p#{partition_key}"
  end

  def build_consumer_instance_id
    # Include partition in instance ID for better tracking
    "#{consumer_group}-#{Socket.gethostname}-#{::Process.pid}-p#{partition_key}"
  end

  def update_offset(event)
    current_offset.update_offset!(
      sequence: event.sequence,
      event_id: event.event_id,
    )
  end

  def should_dead_letter?(event)
    max_retries = dead_letter_config[:max_retries] || 2

    # Check if this event already has a DLQ entry for this consumer group
    dlq_entry = OutboxRelay::DeadLetterEvent.find_by(
      outbox_relay_outbox_event_id: event.id,
      consumer_group: consumer_group
    )

    return false unless dlq_entry

    # Check if we've exceeded max retries
    dlq_entry.total_retries >= max_retries
  end

  def move_to_dead_letter_queue(event, error)
    # Find or create DLQ entry for this consumer group
    dlq_entry = OutboxRelay::DeadLetterEvent.find_or_initialize_by(
      outbox_relay_outbox_event_id: event.id,
      consumer_group: consumer_group
    )

    # Increment retry count
    dlq_entry.assign_attributes(
      consumer_class: self.class.name,
      total_retries: (dlq_entry.total_retries || 0) + 1,
      error_message: error.message,
      error_backtrace: error.backtrace&.first(20)&.join("\n"),
      error_context: build_error_context(event, error),
      original_topic: event.topic,
      original_event_name: event.event_name,
      original_payload: event.payload,
      original_headers: event.headers,
      resolution_status: should_dead_letter?(event) ? "unresolved" : "retrying"
    )

    begin
      dlq_entry.save!
    rescue ActiveRecord::RecordInvalid => e
      # Validation failure - likely data integrity issue
      @logger.error(
        event_name: "dlq_save_validation_failed",
        event_id: event.event_id,
        consumer_group: consumer_group,
        validation_errors: dlq_entry.errors.full_messages.join(", "),
        error: e.message,
        error_class: e.class.name
      )

      OutboxRelay::Instrumentation::Models.error(
        e,
        model: "OutboxConsumer",
        operation: "dlq_save_validation",
        event_id: event.event_id,
        consumer_group: consumer_group,
        validation_errors: dlq_entry.errors.full_messages,
        severity: "critical"
      )

      raise OutboxRelay::Error, "Failed to save DLQ entry (validation): #{dlq_entry.errors.full_messages.join(', ')}"
    rescue ActiveRecord::RecordNotUnique => e
      # Duplicate entry - race condition between workers
      @logger.warn(
        event_name: "dlq_save_duplicate_race_condition",
        event_id: event.event_id,
        consumer_group: consumer_group,
        error: e.message
      )
      # Try to reload and update existing entry
      dlq_entry.reload
      dlq_entry.assign_attributes(
        total_retries: (dlq_entry.total_retries || 0) + 1,
        error_message: error.message,
        error_backtrace: error.backtrace&.first(20)&.join("\n"),
        error_context: build_error_context(event, error),
        resolution_status: should_dead_letter?(event) ? "unresolved" : "retrying"
      )
      dlq_entry.save!
    rescue => e
      # Database or other system error
      @logger.error(
        event_name: "dlq_save_failed",
        event_id: event.event_id,
        consumer_group: consumer_group,
        error: e.message,
        error_class: e.class.name,
        backtrace: e.backtrace&.first(10)&.join("\n")
      )

      OutboxRelay::Instrumentation::Models.error(
        e,
        model: "OutboxConsumer",
        operation: "dlq_save",
        event_id: event.event_id,
        consumer_group: consumer_group,
        phase: "dlq_save",
        severity: "critical"
      )

      raise OutboxRelay::Error, "Failed to save DLQ entry: #{e.message}"
    end

    @logger.error(
      event_name: "event_moved_to_dead_letter_queue",
      event_id: event.event_id,
      topic: topic,
      consumer_event_name: event.event_name,
      consumer_group: consumer_group,
      total_retries: dlq_entry.total_retries,
      resolution_status: dlq_entry.resolution_status,
      error: error.message,
    )
  end

  def handle_error(event, error)
    # Get current DLQ entry for this consumer group to include retry count in logs
    dlq_entry = OutboxRelay::DeadLetterEvent.find_by(
      outbox_relay_outbox_event_id: event.id,
      consumer_group: consumer_group
    )

    @logger.error(
      event_name: "error_processing_event",
      event_id: event.event_id,
      topic: topic,
      consumer_event_name: event.event_name,
      consumer_group: consumer_group,
      error: error.message,
      retry_count: dlq_entry&.total_retries || 0,
      backtrace: error.backtrace&.first(5)&.join("\n"),
    )
  end

  def build_error_context(event, error)
    {
      consumer_group: consumer_group,
      consumer_class: self.class.name,
      consumer_instance_id: @consumer_instance_id,
      event_id: event.event_id,
      sequence: event.sequence,
      error_class: error.class.name,
      timestamp: Time.current.iso8601,
    }
  end

  def report_unknown_event(event)
    @logger.warn(
      event_name: "unknown_event_name",
      consumer_event_name: event.event_name,
      topic: topic,
      consumer_group: consumer_group,
      event_id: event.event_id,
    )
  end

  # Handle event processing failure with proper error reporting and state management
  def handle_event_failure(event, error)
    # Report error to monitoring backend via ActiveSupport::Notifications
    report_processing_error(event, error)

    # Attempt to update event state for retry/DLQ
    update_failed_event_state(event, error)
  rescue => state_error
    # Critical: Failed to update event state after processing error
    log_critical_state_error(event, error, state_error)
  end

  def report_processing_error(event, error)
    # Get current DLQ entry for retry count
    dlq_entry = OutboxRelay::DeadLetterEvent.find_by(
      outbox_relay_outbox_event_id: event.id,
      consumer_group: consumer_group
    )

    OutboxRelay::Instrumentation::Models.error(
      error,
      model: "OutboxConsumer",
      operation: "event_processing",
      event_id: event.event_id,
      consumer_group: consumer_group,
      topic: topic,
      event_name: event.event_name,
      sequence: event.sequence,
      retry_count: dlq_entry&.total_retries || 0,
      partition_key: partition_key
    )
  end

  def update_failed_event_state(event, error)
    # Add or update DLQ entry for this consumer group
    # This tracks per-consumer-group failures and retry attempts
    move_to_dead_letter_queue(event, error)

    # Note: Event itself remains unchanged - failures are tracked in DLQ
    # The event will be excluded from future fetches by fetch_batch
  end

  def log_critical_state_error(event, original_error, state_error)
    @logger.error(
      event_name: "critical_failed_to_update_dlq",
      event_id: event.event_id,
      original_error: original_error.message,
      state_error: state_error.message,
      consumer_group: consumer_group,
      backtrace: state_error.backtrace&.first(10)&.join("\n"),
    )

    # Alert monitoring - this is a critical system failure
    OutboxRelay::Instrumentation::Models.error(
      state_error,
      model: "OutboxConsumer",
      operation: "update_failed_event_state",
      original_error: original_error.message,
      event_id: event.event_id,
      consumer_group: consumer_group,
      severity: "critical"
    )
  end

  def recoverable_error?(error)
    # Only business logic errors are recoverable
    # System errors should stop processing
    !error.is_a?(SystemStackError) &&
      !error.is_a?(NoMemoryError) &&
      !error.is_a?(SignalException) &&
      !error.is_a?(SystemExit) &&
      !error.message.match?(/stack level too deep|out of memory/i)
  end
  end
end
