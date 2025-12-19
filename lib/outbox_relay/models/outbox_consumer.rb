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
  #         auto_offset_reset: :latest,  # Optional: :latest (default) or :earliest
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
  #
  # Auto Offset Reset (for NEW consumer groups only):
  # - :latest (default) - Start from current position, skip historical events
  #   Safe for production deploys - new consumers won't reprocess old data
  # - :earliest - Start from sequence 0, process ALL historical events
  #   Use for backfill consumers or when you need to reprocess everything
  class OutboxConsumer
    attr_reader :consumer_group, :topic, :event_filter, :dead_letter_config, :partition_key, :auto_offset_reset

    # @param consumer_group [String] Unique name for this consumer group
    # @param topic [String] Topic to consume events from
    # @param partition_key [Integer] Partition number to process (0-based)
    # @param event_filter [Array<String>, nil] Optional list of event names to process
    # @param dead_letter_config [Hash] DLQ configuration (e.g., { max_retries: 3 })
    # @param auto_offset_reset [Symbol] Where to start for NEW consumer groups:
    #   - :latest (default) - Start from latest event (safe for production deploys)
    #   - :earliest - Start from beginning (reprocess all historical events)
    VALID_AUTO_OFFSET_RESET_VALUES = [:latest, :earliest].freeze

    def initialize(consumer_group:, topic:, partition_key:, event_filter: nil, dead_letter_config: {}, auto_offset_reset: :latest)
      validate_auto_offset_reset!(auto_offset_reset)

      @consumer_group = consumer_group
      @topic = topic
      @partition_key = partition_key
      @event_filter = Array.wrap(event_filter).compact
      @dead_letter_config = dead_letter_config
      @auto_offset_reset = auto_offset_reset
      @logger = OutboxRelay.logger
      @consumer_instance_id = build_consumer_instance_id
    end

    private

    def validate_auto_offset_reset!(value)
      return if VALID_AUTO_OFFSET_RESET_VALUES.include?(value)

      raise ArgumentError,
        "auto_offset_reset must be :latest or :earliest, got: #{value.inspect}"
    end

    public

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
    # Only exclude "unresolved" events (gave up after max retries)
    # Events with "retrying" status should be retried automatically
    OutboxRelay::DeadLetterEvent
      .where(consumer_group: consumer_group)
      .where(resolution_status: "unresolved")
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
  #   - Automatically released on connection close
  #   - Never escalate to table locks
  #   - Session or transaction-scoped
  #
  # We use **session-scoped** locks (pg_try_advisory_lock):
  #   - Lock persists across transactions until explicitly released
  #   - Must call pg_advisory_unlock() to release (or connection closes)
  #   - Allows consume_message() to run OUTSIDE transaction
  #   - Prevents idle_in_transaction_session_timeout for slow HTTP calls
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
  #   T1   | FETCH event 123            | FETCH event 123
  #   T2   | TRY_LOCK(123) → SUCCESS    | TRY_LOCK(123) → FAIL
  #   T3   | Process event (HTTP call)  | Skip event (already locked)
  #   T4   | Update offset              | Fetch next event
  #   T5   | UNLOCK(123)                |
  #
  # Result: Event processed exactly once by Worker A.
  #
  # ## Edge Cases
  #
  # 1. **Worker crashes mid-processing**: Connection closes → lock released → event retried
  # 2. **Database connection lost**: All locks auto-released
  # 3. **Hash collision** (extremely rare): CRC32 has 1 in 4 billion collision rate
  #    Impact: Different consumer groups might temporarily block each other
  # 4. **Sequence wraparound**: After 4 billion events, sequences repeat
  #    Impact: None - old events already processed and cleaned up
  # 5. **Lock not released due to exception**: ensure block guarantees release
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
    # Try to acquire SESSION-level advisory lock (pg_try_advisory_lock)
    # Returns true if lock acquired, false if already locked by another session
    #
    # Lock lifecycle:
    # - Persists across transactions until explicitly released or connection closes
    # - Must call release_advisory_lock() to release
    # - Auto-released on connection close (worker crash = lock released)
    #
    # Why session-level instead of transaction-level?
    # - Allows consume_message() to run OUTSIDE transaction (HTTP calls safe)
    # - Prevents idle_in_transaction_session_timeout in PostgreSQL/Aurora
    # - Lock still prevents duplicate processing across workers

    # Validate lock_key is a valid integer
    unless lock_key.is_a?(Integer)
      raise ArgumentError, "lock_key must be an integer, got #{lock_key.class}"
    end

    # Use parameterized query to prevent SQL injection
    result = ActiveRecord::Base.connection.execute(
      ActiveRecord::Base.sanitize_sql_array([
        "SELECT pg_try_advisory_lock(?)",
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

    lock_acquired = result["pg_try_advisory_lock"]

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

  def release_advisory_lock(lock_key)
    # Release session-level advisory lock
    # Should be called in ensure block to guarantee release
    return unless lock_key

    ActiveRecord::Base.connection.execute(
      ActiveRecord::Base.sanitize_sql_array([
        "SELECT pg_advisory_unlock(?)",
        lock_key
      ])
    )
  rescue => e
    # Log but don't raise - lock will be released when connection closes anyway
    @logger.warn(
      event_name: "advisory_lock_release_failed",
      lock_key: lock_key,
      error: e.message
    )
  end

  # Phase 1: Check if event can be processed and acquire session lock
  # Returns false if:
  #   - Event already processed (offset check)
  #   - Lock held by another worker
  def can_process_event?(event, lock_key)
    # Check if event was already processed (offset comparison)
    # This is a defensive check - shouldn't happen often due to fetch_batch filtering
    if event.sequence <= current_offset.last_consumed_sequence
      @logger.debug(
        event_name: "event_already_processed",
        event_id: event.event_id,
        sequence: event.sequence,
        current_offset: current_offset.last_consumed_sequence,
        consumer_group: consumer_group
      )
      return false
    end

    # Try to acquire session-level lock
    unless acquire_advisory_lock(lock_key)
      # Lock held by another worker - skip silently (this is normal/expected)
      return false
    end

    true
  end

  # Phase 3: Update offset after successful processing
  # Includes defensive staleness check
  def commit_event_processed(event)
    # Defensive check: verify event wasn't processed while we were working
    # This shouldn't happen due to advisory lock, but provides extra safety
    current_seq = current_offset.reload.last_consumed_sequence
    if event.sequence <= current_seq
      @logger.warn(
        event_name: "offset_already_advanced",
        event_id: event.event_id,
        sequence: event.sequence,
        current_offset: current_seq,
        consumer_group: consumer_group,
        message: "Offset already advanced past this event - skipping update"
      )
      return false
    end

    offset_updated = update_offset(event)

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

    offset_updated
  end

  def process_event(event)
    # 3-Phase Event Processing with Retriable Exception Support
    # ==========================================================
    # Separates lock acquisition, business logic, and offset update to avoid
    # holding DB transaction during potentially slow operations (HTTP calls).
    #
    # Phase 1: Acquire session lock (instant)
    # Phase 2: Process event OUTSIDE transaction (can be slow - HTTP calls OK)
    # Phase 3: Update offset (instant)
    #
    # Retriable Exceptions (e.g., rate limiting):
    # - Override retriable_exception?(e) to identify transient errors
    # - OutboxRelay will sleep and retry instead of going to DLQ
    # - After max_retriable_attempts, falls through to normal DLQ handling
    #
    # Why session-level locks instead of transaction-level?
    # - Transaction-level locks require keeping transaction open during consume_message
    # - Slow HTTP calls cause idle_in_transaction_session_timeout in PostgreSQL/Aurora
    # - Session locks persist across transactions, released explicitly or on connection close
    #
    # Safety guarantees:
    # - Advisory lock prevents duplicate processing across workers
    # - Worker crash → connection closes → lock auto-released → event retried
    # - At-least-once delivery guaranteed (consumers must be idempotent)

    lock_key = advisory_lock_key(event)

    # Phase 1: Check offset and acquire session lock
    return false unless can_process_event?(event, lock_key)

    retriable_attempt = 0

    begin
      # Phase 2: Process event OUTSIDE transaction
      # HTTP calls, API calls - can take seconds without holding DB transaction
      consume_message(event)

      # Phase 3: Update offset
      commit_event_processed(event)

      true # Successfully processed

    rescue => e
      # Check if this is a retriable exception (e.g., rate limiting)
      if retriable_exception?(e) && retriable_attempt < max_retriable_attempts
        retriable_attempt += 1
        delay = retry_delay_for(e)

        @logger.info(
          event_name: "retriable_exception_waiting",
          event_id: event.event_id,
          sequence: event.sequence,
          consumer_group: consumer_group,
          error_class: e.class.name,
          error: e.message,
          attempt: retriable_attempt,
          max_attempts: max_retriable_attempts,
          retry_delay: delay
        )

        # Sleep and retry - don't go to DLQ yet
        sleep(delay)
        retry
      end

      # Non-retriable exception OR exceeded max retriable attempts
      @logger.error(
        event_name: "consume_message_failed",
        event_id: event.event_id,
        sequence: event.sequence,
        consumer_group: consumer_group,
        error: e.message,
        error_class: e.class.name,
        retriable_attempts_exhausted: retriable_attempt >= max_retriable_attempts,
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

      raise
    ensure
      # ALWAYS release lock - even on success, failure, or unexpected exception
      release_advisory_lock(lock_key)
    end
  end

  def consume_message(_event)
    # To be implemented by subclasses
    raise NotImplementedError, "Subclasses must implement consume_message"
  end

  # Protected hooks - designed to be overridden by subclasses
  protected

  # Hook: Determine if an exception should trigger retry with backoff instead of DLQ
  #
  # Override in subclass to handle rate limiting or transient errors:
  #
  #   def retriable_exception?(exception)
  #     exception.is_a?(Prop::RateLimited) ||
  #       exception.is_a?(Faraday::TimeoutError)
  #   end
  #
  # When true, OutboxRelay will:
  #   1. Sleep for retry_delay_for(exception) seconds
  #   2. Retry the event (up to max_retriable_attempts times)
  #   3. NOT count this as a DLQ failure
  #
  # @param exception [Exception] The caught exception
  # @return [Boolean] true if should retry with backoff, false for normal DLQ handling
  def retriable_exception?(exception)
    false
  end

  # Hook: Determine how long to wait before retrying a retriable exception
  #
  # Override in subclass for custom delay logic:
  #
  #   def retry_delay_for(exception)
  #     case exception
  #     when Prop::RateLimited
  #       exception.retry_after  # Use Prop's calculated delay
  #     when Faraday::TimeoutError
  #       5  # Fixed 5 second delay for timeouts
  #     else
  #       60  # Default fallback
  #     end
  #   end
  #
  # @param exception [Exception] The retriable exception
  # @return [Numeric] Seconds to sleep before retry (default: 60, max: 300)
  def retry_delay_for(exception)
    delay = if exception.respond_to?(:retry_after)
      exception.retry_after
    else
      60
    end

    # Cap at 5 minutes to prevent excessive blocking
    [delay.to_i, 300].min
  end

  # Hook: Maximum retry attempts for retriable exceptions before giving up
  #
  # Override in subclass if needed:
  #
  #   def max_retriable_attempts
  #     10  # More attempts for rate-limited APIs
  #   end
  #
  # @return [Integer] Max attempts (default: 5)
  def max_retriable_attempts
    5
  end

  private

  def current_offset
    # Each partition has its own offset tracking
    # This allows parallel processing without conflicts
    @current_offset ||= OutboxRelay::ConsumerOffset.find_or_initialize_for(
      consumer_group: consumer_group_with_partition,
      topic: topic,
      auto_offset_reset: auto_offset_reset,
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
    # CRITICAL: Use requires_new to create independent transaction
    # This ensures DLQ entry is persisted even when the outer transaction rolls back
    #
    # Flow:
    #   1. Outer transaction: process_event starts
    #   2. consume_message fails with error
    #   3. handle_event_failure -> move_to_dead_letter_queue
    #   4. NEW independent transaction: DLQ entry saved and COMMITTED
    #   5. Outer transaction: raise causes ROLLBACK
    #   6. DLQ entry survives because it was committed in step 4
    #
    # Without requires_new, the DLQ save would be rolled back with the outer transaction
    dlq_entry = nil

    ActiveRecord::Base.transaction(requires_new: true) do
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
      rescue ActiveRecord::RecordNotUnique
        # Race condition - another worker created DLQ entry first
        # Reload and update the existing entry
        dlq_entry = OutboxRelay::DeadLetterEvent.find_by!(
          outbox_relay_outbox_event_id: event.id,
          consumer_group: consumer_group
        )
        dlq_entry.update!(
          total_retries: dlq_entry.total_retries + 1,
          error_message: error.message,
          error_backtrace: error.backtrace&.first(20)&.join("\n"),
          error_context: build_error_context(event, error),
          resolution_status: should_dead_letter?(event) ? "unresolved" : "retrying"
        )
      end
    end
    # End of requires_new transaction - DLQ entry is now committed independently

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
