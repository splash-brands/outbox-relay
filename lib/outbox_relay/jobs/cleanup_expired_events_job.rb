# frozen_string_literal: true

module OutboxRelay
  module Jobs
    # Periodically cleans up expired OutboxRelay events and resolved DLQ entries
    # from the database.
    #
    # ## What gets deleted
    #
    # **Events** (outbox_relay_outbox_events):
    # - Have expired (expires_at IS NOT NULL AND expires_at < now), AND
    # - Have been processed by ALL consumer groups (sequence < min consumed sequence per topic).
    #
    # Events without expires_at are never deleted by this job.
    #
    # **Resolved DLQ entries** (outbox_relay_dead_letter_events):
    # - Have resolution_status IN (resolved, reprocessed, ignored), AND
    # - Were resolved before (now - OutboxRelay.dlq_resolved_ttl).
    #
    # The TTL is measured from `resolved_at` (populated by mark_as_resolved! /
    # mark_as_reprocessed! / mark_as_ignored!). Entries with resolution_status in
    # a terminal state but a NULL resolved_at (e.g. rows updated by older code
    # without going through the helpers) are preserved.
    #
    # This phase runs only if `OutboxRelay.dlq_resolved_ttl` is set.
    # Unresolved / retrying DLQ entries are NEVER deleted.
    #
    # ## Configuration
    #
    # Enable cleanup in your OutboxRelay initializer:
    #
    #   OutboxRelay.cleanup_enabled    = true       # Enable cleanup (default: false)
    #   OutboxRelay.cleanup_batch_size = 10_000     # Batch per run (default: 10_000)
    #   OutboxRelay.default_event_ttl  = 14.days    # Publisher default TTL (optional)
    #   OutboxRelay.dlq_resolved_ttl   = 14.days    # Resolved DLQ TTL (optional)
    #
    # In Rails these also flow through `Rails.application.config.outbox_relay.*`.
    # Both TTL values must be `ActiveSupport::Duration` instances — bare Integers
    # are rejected to prevent the "I meant 14.days but wrote 14 (seconds)" footgun.
    #
    # ## Instrumentation
    #
    # The job emits an ActiveSupport::Notifications event after every run — success,
    # timeout, or failure:
    #
    #   ActiveSupport::Notifications.subscribe("outbox_relay.cleanup.completed") do |_, _, _, _, payload|
    #     payload # => { events_deleted:, dlq_deleted:, duration:, error_class:, timeout: }
    #   end
    #
    # `error_class` is nil on success, the exception class name on failure, or
    # the value "PG::QueryCanceled" with `timeout: true` on timeout. `duration`
    # is always populated.
    #
    # The event name follows the gem-wide `outbox_relay.<category>.<event>`
    # convention, so subscribers using the documented `/^outbox_relay\./` pattern
    # will see it.
    #
    # ## Sidekiq Integration
    #
    # If using Sidekiq Enterprise periodic jobs, register the job with cron syntax:
    #
    #   Sidekiq.configure_server do |config|
    #     config.periodic do |mgr|
    #       mgr.register("0 3 * * *", "OutboxRelay::Jobs::CleanupExpiredEventsJob")
    #     end
    #   end
    #
    # ## Alternative Schedulers
    #
    # For other schedulers (cron, whenever, etc.), call:
    #   OutboxRelay::Jobs::CleanupExpiredEventsJob.perform
    #
    # Without Sidekiq, the job does not auto-retry on database errors. Run the
    # schedule frequently enough that the next scheduled run is an acceptable retry.
    #
    class CleanupExpiredEventsJob
      include Sidekiq::Job if defined?(Sidekiq::Job)

      sidekiq_options queue: :default, retry: 3 if defined?(Sidekiq)

      EVENT_NAME = 'outbox_relay.cleanup.completed'
      TERMINAL_DLQ_STATUSES = %w[resolved reprocessed ignored].freeze

      # Sentinel that replaces the real PG classes in rescue clauses when the
      # `pg` gem is not loaded (e.g. when the host app uses a different adapter,
      # or when specs run against SQLite). `.new` is blocked, so the sentinel
      # can never be raised and its rescue clause simply never matches.
      class UnreachableError < StandardError
        private_class_method :new
      end

      PG_CONNECTION_BAD = defined?(PG::ConnectionBad) ? PG::ConnectionBad : UnreachableError
      PG_QUERY_CANCELED = defined?(PG::QueryCanceled) ? PG::QueryCanceled : UnreachableError

      class << self
        # Perform cleanup (can be called directly or via Sidekiq).
        def perform
          return unless OutboxRelay.cleanup_enabled

          new.perform
        end
      end

      def perform
        @events_deleted = 0
        @dlq_deleted = 0
        @events_iterations = 0
        @dlq_iterations = 0
        @error_class = nil
        @timeout = false
        started_at = monotonic_now

        # DLQ first: deleting resolved DLQ entries frees up FK references on
        # outbox_events, making them eligible for cleanup in the same run.
        deadline = monotonic_now + cleanup_max_runtime_seconds
        @dlq_iterations    = run_loop(deadline) { delete_resolved_dlq_chunk_and_accumulate }
        @events_iterations = run_loop(deadline) { delete_expired_events_chunk_and_accumulate }

        build_result(duration_since(started_at))
      rescue ActiveRecord::ConnectionNotEstablished, PG_CONNECTION_BAD => e
        @error_class = e.class.name
        log_error('outbox_relay_cleanup_database_error', e)
        safe_report_error(e, severity: 'warning')
        raise
      rescue PG_QUERY_CANCELED => e
        @error_class = e.class.name
        @timeout = true
        log_error('outbox_relay_cleanup_timeout', e,
                  note: 'Query timeout during cleanup - will retry in next scheduled run')
        build_result(duration_since(started_at))
      rescue StandardError => e
        @error_class = e.class.name
        log_error('outbox_relay_cleanup_unexpected_error', e)
        safe_report_error(e, severity: 'critical')
        raise
      ensure
        duration = duration_since(started_at)
        log_completion(duration)
        emit_notification(duration)
      end

      private

      # Run a deletion phase in chunks until either exhausted (chunk returned
      # fewer rows than cleanup_batch_size) or the deadline is hit. Always runs
      # at least one iteration so cleanup_max_runtime = 0 is not a no-op.
      # The block must return the chunk row count and is responsible for
      # updating its own accumulator (so partial counts survive a mid-loop
      # raise). Returns the iteration count.
      def run_loop(deadline)
        iterations = 0
        loop do
          deleted = yield
          iterations += 1
          break if deleted < OutboxRelay.cleanup_batch_size
          break if monotonic_now >= deadline
        end
        iterations
      end

      def cleanup_max_runtime_seconds
        OutboxRelay.cleanup_max_runtime.to_f
      end

      def delete_resolved_dlq_chunk_and_accumulate
        deleted = delete_resolved_dlq_chunk
        @dlq_deleted += deleted
        deleted
      end

      def delete_expired_events_chunk_and_accumulate
        deleted = delete_expired_events_chunk
        @events_deleted += deleted
        deleted
      end

      # Delete expired events that have been fully consumed.
      #
      # An event is deleted only if its sequence is strictly less than the minimum
      # last_consumed_sequence across all consumer groups for its topic. This
      # guarantees every consumer group has already processed it.
      #
      # Events still referenced by a row in outbox_relay_dead_letter_events are
      # preserved: production has a FK on outbox_relay_outbox_event_id with no
      # ON DELETE rule, so deleting a referenced event would fail with
      # PG::ForeignKeyViolation. Resolved DLQ entries are dropped first by
      # delete_resolved_dlq_entries, so once the DLQ TTL passes the event
      # becomes eligible in the next run (or the same run, since DLQ cleanup
      # runs first).
      def delete_expired_events_chunk
        # Subquery returns a single value (MIN), so `< (subquery)` is equivalent to
        # `< ALL(subquery)` and works across both PostgreSQL and SQLite (tests).
        OutboxRelay::OutboxEvent
          .where('expires_at IS NOT NULL AND expires_at < ?', Time.current)
          .where(
            "sequence < (
              SELECT COALESCE(MIN(last_consumed_sequence), 0)
              FROM outbox_relay_consumer_offsets
              WHERE topic = outbox_relay_outbox_events.topic
            )"
          )
          .where(
            "NOT EXISTS (
              SELECT 1 FROM outbox_relay_dead_letter_events
              WHERE outbox_relay_dead_letter_events.outbox_relay_outbox_event_id = outbox_relay_outbox_events.id
            )"
          )
          .limit(OutboxRelay.cleanup_batch_size)
          .delete_all
      end

      # Delete resolved DLQ entries older than the configured TTL.
      #
      # Only touches entries with terminal resolution_status (resolved, reprocessed,
      # ignored) that also have resolved_at populated. TTL is measured from
      # resolved_at, not created_at — an entry resolved today cannot be deleted
      # until `resolved_at + dlq_resolved_ttl` has passed, regardless of how
      # long it spent in `retrying` first.
      #
      # Returns 0 when dlq_resolved_ttl is not configured.
      def delete_resolved_dlq_chunk
        ttl = OutboxRelay.dlq_resolved_ttl
        return 0 if ttl.nil?

        unless ttl.is_a?(ActiveSupport::Duration)
          raise OutboxRelay::ConfigurationError,
                'OutboxRelay.dlq_resolved_ttl must be an ActiveSupport::Duration ' \
                "(e.g. `14.days`); got #{ttl.class}: #{ttl.inspect}. " \
                'Bare Integers would silently be interpreted as seconds.'
        end

        OutboxRelay::DeadLetterEvent
          .where(resolution_status: TERMINAL_DLQ_STATUSES)
          .where('resolved_at IS NOT NULL AND resolved_at < ?', ttl.ago)
          .limit(OutboxRelay.cleanup_batch_size)
          .delete_all
      end

      def build_result(duration)
        result = { events_deleted: @events_deleted, dlq_deleted: @dlq_deleted, duration: duration }
        result[:timeout] = true if @timeout
        result[:error_class] = @error_class if @error_class && !@timeout
        result
      end

      def duration_since(started_at)
        monotonic_now - started_at
      end

      def monotonic_now
        ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
      end

      def log_completion(duration)
        return unless @events_deleted.positive? || @dlq_deleted.positive?

        OutboxRelay.logger.info(
          event_name: 'outbox_relay_cleanup_completed',
          events_deleted: @events_deleted,
          dlq_deleted: @dlq_deleted,
          duration_ms: (duration * 1000).round(2),
          timestamp: Time.current.iso8601
        )
      end

      # Always emits, including on failure, so dashboards see every job run.
      # Subscriber errors are swallowed to avoid misclassifying them as a
      # cleanup failure in the outer rescue chain.
      def emit_notification(duration)
        payload = {
          events_deleted: @events_deleted,
          dlq_deleted: @dlq_deleted,
          duration: duration,
          error_class: @error_class,
          timeout: @timeout
        }

        ActiveSupport::Notifications.instrument(EVENT_NAME, payload)
      rescue StandardError => e
        OutboxRelay.logger.error(
          event_name: 'outbox_relay_cleanup_notification_error',
          error_class: e.class.name,
          error_message: e.message
        )
      end

      # Report to Sentry/Rails.error via Instrumentation, but don't let a
      # failure in the reporter itself replace the original exception in
      # the outer rescue.
      def safe_report_error(error, severity:)
        OutboxRelay::Instrumentation::Models.error(
          error,
          model: 'CleanupExpiredEventsJob',
          operation: 'cleanup',
          severity: severity
        )
      rescue StandardError => e
        OutboxRelay.logger.error(
          event_name: 'outbox_relay_cleanup_instrumentation_error',
          error_class: e.class.name,
          error_message: e.message,
          original_error_class: error.class.name
        )
      end

      def log_error(event_name, error, **context)
        OutboxRelay.logger.error(
          {
            event_name: event_name,
            error_class: error.class.name,
            error_message: error.message,
            backtrace: error.backtrace&.first(10)&.join("\n")
          }.merge(context)
        )
      end
    end
  end
end
