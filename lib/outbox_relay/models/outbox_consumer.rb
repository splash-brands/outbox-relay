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
  #         dead_letter_config: { max_retries: 5 },  # Optional: DLQ configuration
  #         auto_offset_reset: :latest,  # Optional: :latest (default) or :earliest
  #       )
  #     end
  #
  #     def consume_message(event)
  #       # Process event
  #     end
  #   end
  #
  # Error Handling:
  # - Errors go to DLQ with exponential backoff (60s base, up to 30 min)
  # - After max_retries (default: 5), event marked as "unresolved"
  # - For rate limiting, use a helper in your consumer to wait for tokens
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
    VALID_AUTO_OFFSET_RESET_VALUES = %i[latest earliest].freeze

    def initialize(consumer_group:, topic:, partition_key:, event_filter: nil, dead_letter_config: {},
                   auto_offset_reset: :latest)
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
      rescue ActiveRecord::ConnectionNotEstablished, PG::Error => e
        # Database connectivity issue - stop processing this batch
        @logger.error(
          event_name: 'database_connectivity_error',
          event_id: event.event_id,
          error: e.message,
          backtrace: e.backtrace&.first(10)&.join("\n")
        )
        raise # Stop processing batch - let supervisor restart worker
      rescue ActiveRecord::RecordInvalid, ActiveRecord::StatementInvalid => e
        # Invalid data or constraint violation - this is a bug
        @logger.error(
          event_name: 'critical_event_validation_error',
          event_id: event.event_id,
          consumer_group: consumer_group,
          error: e.message,
          backtrace: e.backtrace&.first(10)&.join("\n")
        )

        OutboxRelay::Instrumentation::Models.error(
          e,
          model: 'OutboxConsumer',
          operation: 'consume_event_validation',
          event_id: event.event_id,
          consumer_group: consumer_group,
          severity: 'critical'
        )

        # Add to DLQ immediately - don't retry validation errors
        move_to_dead_letter_queue(event, e)
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
            event_name: 'critical_system_error_in_consumer',
            event_id: event.event_id,
            error_class: e.class.name,
            error: e.message,
            backtrace: e.backtrace&.first(10)&.join("\n")
          )

          OutboxRelay::Instrumentation::Models.error(
            e,
            model: 'OutboxConsumer',
            operation: 'consume_event_system_error',
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
      # Count actual events after the consumer's offset for this specific partition.
      # Previously used max_sequence - last_consumed_sequence, but sequence numbers
      # are global across all topics, so the gap overstates real backlog.
      OutboxRelay::OutboxEvent
        .where(topic: topic, partition_key: partition_key)
        .where('commit_seq > ?', current_offset.last_consumed_sequence)
        .count
    end

    private

    def fetch_batch(batch_size)
      # Get current offset
      offset = current_offset

      # Get IDs of events in DLQ for this consumer group (with caching)
      # These events should not be fetched again by this consumer group
      dlq_event_ids = fetch_dlq_event_ids

      # Get IDs of DLQ events ready for retry (retrying with retry_after <= now)
      # These need to be explicitly included since their sequence is before last_consumed_sequence
      retry_ready_ids = fetch_retry_ready_event_ids

      # Store retry IDs for use in process_event to identify DLQ retries
      # This is needed because DLQ retry events have sequence <= offset,
      # but should NOT be skipped by can_process_event? offset check
      @current_batch_retry_ids = retry_ready_ids.to_set

      # Build query for normal events (after current offset) + retry-ready events
      # Using OR to combine: (commit_seq > offset AND not in DLQ) OR (ready for retry)
      #
      # Ordering/cursor is commit_seq (assigned at the COMMIT edge), NOT sequence
      # (assigned at INSERT). This closes the long-transaction race where a
      # lower-sequence row that commits late lands below the high-water offset and
      # is silently skipped. See SB-2140.
      query = OutboxRelay::OutboxEvent
              .where(topic: topic)
              .where(partition_key: partition_key)
              .where(
                '(commit_seq > :offset AND id NOT IN (:dlq_ids)) OR id IN (:retry_ids)',
                offset: offset.last_consumed_sequence,
                dlq_ids: dlq_event_ids.presence || [0], # [0] for empty array to avoid SQL syntax error
                retry_ids: retry_ready_ids.presence || [0]
              )
              .not_expired
              .by_commit_seq
              .limit(batch_size)

      # Apply event filtering (matches Karafka's MarkingEventTypeFilter)
      query = query.where(event_name: event_filter) if event_filter.present?

      # Add FOR UPDATE SKIP LOCKED for efficient multi-worker operation
      # This allows workers to grab different events in parallel instead of competing
      # NOTE: This changes query semantics - events locked by other transactions are skipped
      query = query.lock('FOR UPDATE SKIP LOCKED')

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
      # Exclude events that should NOT be fetched:
      # 1. "unresolved" - gave up after max retries, won't retry
      # 2. "retrying" with retry_after > current time - still in backoff period
      #
      # Events with "retrying" AND (retry_after IS NULL OR retry_after <= current time)
      # are ready for retry and should NOT be excluded
      current_time = Time.current

      OutboxRelay::DeadLetterEvent
        .where(consumer_group: consumer_group)
        .where(
          "resolution_status = 'unresolved' OR " \
          "(resolution_status = 'retrying' AND retry_after IS NOT NULL AND retry_after > ?)",
          current_time
        )
        .pluck(:outbox_relay_outbox_event_id)
    end

    # Get IDs of events ready for retry from DLQ
    # These are events with "retrying" status and retry_after <= current time
    # They need to be explicitly included in fetch since their sequence is before last_consumed_sequence
    def fetch_retry_ready_event_ids
      @retry_ready_cache ||= begin
        @retry_ready_cache_expires_at = Time.current + 5.seconds
        load_retry_ready_event_ids
      end

      if Time.current > @retry_ready_cache_expires_at
        @retry_ready_cache = load_retry_ready_event_ids
        @retry_ready_cache_expires_at = Time.current + 5.seconds
      end

      @retry_ready_cache
    end

    def load_retry_ready_event_ids
      current_time = Time.current

      OutboxRelay::DeadLetterEvent
        .where(consumer_group: consumer_group)
        .where(resolution_status: 'retrying')
        .where('retry_after IS NULL OR retry_after <= ?', current_time)
        .pluck(:outbox_relay_outbox_event_id)
    end

    # Mark DLQ entry as successfully reprocessed
    # Called after consume_message succeeds for an event that was in DLQ
    def mark_dlq_event_reprocessed(event)
      dlq_entry = OutboxRelay::DeadLetterEvent.find_by(
        consumer_group: consumer_group,
        outbox_relay_outbox_event_id: event.id,
        resolution_status: 'retrying'
      )

      return unless dlq_entry

      dlq_entry.mark_as_reprocessed!(notes: "Successfully reprocessed after #{dlq_entry.total_retries} retries")

      # Invalidate retry cache so this event is not fetched again
      @retry_ready_cache = nil

      @logger.info(
        event_name: 'dlq_event_reprocessed',
        event_id: event.event_id,
        sequence: event.sequence,
        consumer_group: consumer_group,
        total_retries: dlq_entry.total_retries,
        message: 'DLQ event successfully reprocessed'
      )
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
      raise ArgumentError, "lock_key must be an integer, got #{lock_key.class}" unless lock_key.is_a?(Integer)

      # Use parameterized query to prevent SQL injection
      result = ActiveRecord::Base.connection.execute(
        ActiveRecord::Base.sanitize_sql_array([
                                                'SELECT pg_try_advisory_lock(?)',
                                                lock_key
                                              ])
      ).first

      if result.nil?
        @logger.error(
          event_name: 'advisory_lock_query_returned_nil',
          lock_key: lock_key
        )
        return false  # Fail safe - don't process event
      end

      lock_acquired = result['pg_try_advisory_lock']

      if lock_acquired.nil?
        @logger.error(
          event_name: 'advisory_lock_result_unexpected_format',
          lock_key: lock_key,
          result: result.inspect
        )
        return false  # Fail safe
      end

      lock_acquired == true
    rescue StandardError => e
      @logger.error(
        event_name: 'advisory_lock_failed',
        lock_key: lock_key,
        error: e.message,
        backtrace: e.backtrace&.first(10)&.join("\n")
      )

      OutboxRelay::Instrumentation::Models.error(
        e,
        model: 'OutboxConsumer',
        operation: 'advisory_lock',
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
                                                'SELECT pg_advisory_unlock(?)',
                                                lock_key
                                              ])
      )
    rescue StandardError => e
      # Log but don't raise - lock will be released when connection closes anyway
      @logger.warn(
        event_name: 'advisory_lock_release_failed',
        lock_key: lock_key,
        error: e.message
      )
    end

    # Phase 1: Check if event can be processed and acquire session lock
    # Returns false if:
    #   - Event already processed (offset check) AND not a DLQ retry
    #   - Lock held by another worker
    #
    # @param event [OutboxEvent] The event to check
    # @param lock_key [Integer] Advisory lock key for this event
    # @param is_dlq_retry [Boolean] True if this event is being retried from DLQ
    def can_process_event?(event, lock_key, is_dlq_retry: false)
      # Skip offset check for DLQ retry events - they have commit_seq <= offset by definition
      # (they were already processed and failed, so offset moved past them)
      if !is_dlq_retry && (event.commit_seq <= current_offset.last_consumed_sequence)
        # Skip if already processed by another worker (normal in multi-worker setup)
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
      if event.commit_seq <= current_seq
        @logger.warn(
          event_name: 'offset_already_advanced',
          event_id: event.event_id,
          sequence: event.sequence,
          commit_seq: event.commit_seq,
          current_offset: current_seq,
          consumer_group: consumer_group,
          message: 'Offset already advanced past this event - skipping update'
        )
        return false
      end

      offset_updated = update_offset(event)

      unless offset_updated
        @logger.debug(
          event_name: 'stale_offset_skipped',
          event_id: event.event_id,
          sequence: event.sequence,
          current_offset: current_offset.last_consumed_sequence,
          consumer_group: consumer_group,
          message: 'Event processed successfully but offset not updated (out-of-order completion)'
        )
      end

      offset_updated
    end

    def process_event(event)
      # 3-Phase Event Processing
      # ==========================================================
      # Separates lock acquisition, business logic, and offset update to avoid
      # holding DB transaction during potentially slow operations (HTTP calls).
      #
      # Phase 1: Acquire session lock (instant)
      # Phase 2: Process event OUTSIDE transaction (can be slow - HTTP calls OK)
      # Phase 3: Update offset (instant)
      #
      # Why session-level locks instead of transaction-level?
      # - Transaction-level locks require keeping transaction open during consume_message
      # - Slow HTTP calls cause idle_in_transaction_session_timeout in PostgreSQL/Aurora
      # - Session locks persist across transactions, released explicitly or on connection close
      #
      # Error handling:
      # - Errors go to DLQ with exponential backoff (configurable)
      # - For rate limiting, use with_rate_limit helper in your consumer
      #
      # Safety guarantees:
      # - Advisory lock prevents duplicate processing across workers
      # - Worker crash → connection closes → lock auto-released → event retried
      # - At-least-once delivery guaranteed (consumers must be idempotent)

      lock_key = advisory_lock_key(event)

      # Check if this event is a DLQ retry (sequence may be <= offset, but should be processed)
      is_dlq_retry = @current_batch_retry_ids&.include?(event.id) || false

      # Phase 1: Check offset and acquire session lock
      return false unless can_process_event?(event, lock_key, is_dlq_retry: is_dlq_retry)

      begin
        # Phase 2: Process event OUTSIDE transaction
        # HTTP calls, API calls - can take seconds without holding DB transaction
        consume_message(event)

        # Mark DLQ entry as reprocessed if this was a retry
        mark_dlq_event_reprocessed(event)

        # Phase 3: Update offset
        commit_event_processed(event)

        true # Successfully processed
      rescue StandardError => e
        @logger.error(
          event_name: 'consume_message_failed',
          event_id: event.event_id,
          sequence: event.sequence,
          consumer_group: consumer_group,
          error: e.message,
          error_class: e.class.name,
          backtrace: e.backtrace&.first(10)&.join("\n")
        )

        OutboxRelay::Instrumentation::Models.error(
          e,
          model: 'OutboxConsumer',
          operation: 'consume_message',
          event_id: event.event_id,
          sequence: event.sequence,
          consumer_group: consumer_group,
          phase: 'consume_message'
        )

        # Record failure in DLQ for retry with backoff
        handle_event_failure(event, e)

        raise
      ensure
        # ALWAYS release lock - even on success, failure, or unexpected exception
        release_advisory_lock(lock_key)
      end
    end

    def consume_message(_event)
      # To be implemented by subclasses
      raise NotImplementedError, 'Subclasses must implement consume_message'
    end

    # Protected hooks - designed to be overridden by subclasses
    protected

    # Hook: Base delay for DLQ-level exponential backoff (in seconds)
    #
    # Default: 60 seconds (1 minute)
    # Override in subclass or configure via dead_letter_config[:retry_base_delay]
    #
    # Formula: base_delay * (2 ^ (retry_count - 1)) + jitter
    #   Retry 1: 1 min (with 60s base)
    #   Retry 2: 2 min
    #   Retry 3: 4 min
    #   Retry 4: 8 min
    #   Retry 5: 16 min (capped at dlq_retry_max_delay)
    #
    # @return [Integer] Base delay in seconds (default: 60)
    def dlq_retry_base_delay
      dead_letter_config[:retry_base_delay] || 60
    end

    # Hook: Maximum delay for DLQ-level backoff (in seconds)
    #
    # @return [Integer] Max delay in seconds (default: 1800 = 30 minutes)
    def dlq_retry_max_delay
      dead_letter_config[:retry_max_delay] || 1800
    end

    private

    def current_offset
      # Each partition has its own offset tracking
      # This allows parallel processing without conflicts
      @current_offset ||= OutboxRelay::ConsumerOffset.find_or_initialize_for(
        consumer_group: consumer_group_with_partition,
        topic: topic,
        auto_offset_reset: auto_offset_reset
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
        commit_seq: event.commit_seq,
        event_id: event.event_id
      )
    end

    def should_dead_letter?(event)
      max_retries = dead_letter_config[:max_retries] || 5

      # Check if this event already has a DLQ entry for this consumer group
      dlq_entry = OutboxRelay::DeadLetterEvent.find_by(
        outbox_relay_outbox_event_id: event.id,
        consumer_group: consumer_group
      )

      return false unless dlq_entry

      # Check if we've exceeded max retries
      dlq_entry.total_retries >= max_retries
    end

    # Calculate retry_after timestamp with exponential backoff and jitter
    #
    # Formula: NOW() + base_delay * (2 ^ (retry_count - 1)) * jitter
    #
    # Examples with 60s base (default):
    #   retry_count=1: 60s * 2^0 = 60s (1 min)
    #   retry_count=2: 60s * 2^1 = 120s (2 min)
    #   retry_count=3: 60s * 2^2 = 240s (4 min)
    #   retry_count=4: 60s * 2^3 = 480s (8 min)
    #   retry_count=5: 60s * 2^4 = 960s (16 min)
    #
    # Jitter adds ±20% randomness to prevent thundering herd
    def calculate_retry_after(retry_count)
      base = dlq_retry_base_delay
      max_delay = dlq_retry_max_delay

      # Exponential backoff: base * 2^(n-1)
      delay = base * (2**[retry_count - 1, 0].max)

      # Cap at maximum
      delay = [delay, max_delay].min

      # Add jitter: ±20%
      jitter_factor = 0.8 + (rand * 0.4) # 0.8 to 1.2
      delay = (delay * jitter_factor).to_i

      Time.current + delay.seconds
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
        new_retry_count = (dlq_entry.total_retries || 0) + 1
        will_dead_letter = should_dead_letter?(event)
        new_status = will_dead_letter ? 'unresolved' : 'retrying'

        # Calculate retry_after with exponential backoff (only for "retrying" status)
        # Events marked "unresolved" won't be retried, so no delay needed
        new_retry_after = will_dead_letter ? nil : calculate_retry_after(new_retry_count)

        dlq_entry.assign_attributes(
          consumer_class: self.class.name,
          total_retries: new_retry_count,
          error_message: error.message,
          error_backtrace: error.backtrace&.first(20)&.join("\n"),
          error_context: build_error_context(event, error),
          original_topic: event.topic,
          original_sequence: event.sequence,
          original_event_id: event.event_id,
          original_event_name: event.event_name,
          original_payload: event.payload,
          original_headers: event.headers,
          original_partition_key: event.partition_key,
          resolution_status: new_status,
          retry_after: new_retry_after
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
          updated_retry_count = dlq_entry.total_retries + 1
          will_dead_letter_now = should_dead_letter?(event)
          updated_status = will_dead_letter_now ? 'unresolved' : 'retrying'
          updated_retry_after = will_dead_letter_now ? nil : calculate_retry_after(updated_retry_count)

          dlq_entry.update!(
            total_retries: updated_retry_count,
            error_message: error.message,
            error_backtrace: error.backtrace&.first(20)&.join("\n"),
            error_context: build_error_context(event, error),
            resolution_status: updated_status,
            retry_after: updated_retry_after
          )
        end
      end
      # End of requires_new transaction - DLQ entry is now committed independently

      # CRITICAL: Immediately add event to DLQ cache to prevent re-fetching
      # Without this, the event could be fetched again before cache expires (5s)
      # and retried immediately, burning through all retries in seconds instead of
      # respecting the exponential backoff delay.
      #
      # This is a hot-path fix: instead of invalidating the entire cache (which would
      # cause a DB query), we surgically add just this event ID to the exclusion list.
      if @dlq_event_ids_cache && dlq_entry.resolution_status == 'retrying' && !@dlq_event_ids_cache.include?(event.id)
        @dlq_event_ids_cache << event.id
      end

      @logger.error(
        event_name: 'event_moved_to_dead_letter_queue',
        event_id: event.event_id,
        topic: topic,
        consumer_event_name: event.event_name,
        consumer_group: consumer_group,
        total_retries: dlq_entry.total_retries,
        resolution_status: dlq_entry.resolution_status,
        retry_after: dlq_entry.retry_after&.iso8601,
        error: error.message
      )
    end

    def handle_error(event, error)
      # Get current DLQ entry for this consumer group to include retry count in logs
      dlq_entry = OutboxRelay::DeadLetterEvent.find_by(
        outbox_relay_outbox_event_id: event.id,
        consumer_group: consumer_group
      )

      @logger.error(
        event_name: 'error_processing_event',
        event_id: event.event_id,
        topic: topic,
        consumer_event_name: event.event_name,
        consumer_group: consumer_group,
        error: error.message,
        retry_count: dlq_entry&.total_retries || 0,
        backtrace: error.backtrace&.first(5)&.join("\n")
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
        timestamp: Time.current.iso8601
      }
    end

    def report_unknown_event(event)
      @logger.warn(
        event_name: 'unknown_event_name',
        consumer_event_name: event.event_name,
        topic: topic,
        consumer_group: consumer_group,
        event_id: event.event_id
      )
    end

    # Handle event processing failure with proper error reporting and state management
    def handle_event_failure(event, error)
      # Report error to monitoring backend via ActiveSupport::Notifications
      report_processing_error(event, error)

      # Attempt to update event state for retry/DLQ
      update_failed_event_state(event, error)
    rescue StandardError => e
      # Critical: Failed to update event state after processing error
      log_critical_state_error(event, error, e)
    end

    def report_processing_error(event, error)
      # Get current DLQ entry for retry count
      dlq_entry = OutboxRelay::DeadLetterEvent.find_by(
        outbox_relay_outbox_event_id: event.id,
        consumer_group: consumer_group
      )

      OutboxRelay::Instrumentation::Models.error(
        error,
        model: 'OutboxConsumer',
        operation: 'event_processing',
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

      # NOTE: Event itself remains unchanged - failures are tracked in DLQ
      # The event will be excluded from future fetches by fetch_batch
    end

    def log_critical_state_error(event, original_error, state_error)
      @logger.error(
        event_name: 'critical_failed_to_update_dlq',
        event_id: event.event_id,
        original_error: original_error.message,
        state_error: state_error.message,
        consumer_group: consumer_group,
        backtrace: state_error.backtrace&.first(10)&.join("\n")
      )

      # Alert monitoring - this is a critical system failure
      OutboxRelay::Instrumentation::Models.error(
        state_error,
        model: 'OutboxConsumer',
        operation: 'update_failed_event_state',
        original_error: original_error.message,
        event_id: event.event_id,
        consumer_group: consumer_group,
        severity: 'critical'
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
