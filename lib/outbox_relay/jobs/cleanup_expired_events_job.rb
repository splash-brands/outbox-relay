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
    # - Are older than OutboxRelay.dlq_resolved_ttl.
    #
    # This phase runs only if `OutboxRelay.dlq_resolved_ttl` is set.
    # Unresolved / retrying DLQ entries are NEVER deleted.
    #
    # ## Configuration
    #
    # Enable cleanup in your OutboxRelay initializer:
    #
    #   Rails.application.config.outbox_relay.cleanup_enabled = true       # Enable cleanup (default: false)
    #   Rails.application.config.outbox_relay.cleanup_batch_size = 10_000  # Batch per run (default: 10_000)
    #   Rails.application.config.outbox_relay.default_event_ttl = 14.days  # Publisher default TTL (optional)
    #   Rails.application.config.outbox_relay.dlq_resolved_ttl = 14.days   # Resolved DLQ TTL (optional)
    #
    # ## Instrumentation
    #
    # After each run, the job emits an ActiveSupport::Notifications event:
    #
    #   ActiveSupport::Notifications.subscribe("cleanup_completed.outbox_relay") do |_, _, _, _, payload|
    #     puts payload.inspect
    #     # => { events_deleted: 42, dlq_deleted: 3, duration: 0.123 }
    #   end
    #
    # ## Sidekiq Integration
    #
    # If using Sidekiq Enterprise periodic jobs, register the job with cron syntax:
    #
    #   Sidekiq.configure_server do |config|
    #     config.periodic do |mgr|
    #       mgr.register("0 3 * * *", "OutboxRelay::Jobs::CleanupExpiredEventsJob")  # Daily at 3 AM UTC
    #     end
    #   end
    #
    # ## Alternative Schedulers
    #
    # For other schedulers (cron, whenever, etc.), call:
    #   OutboxRelay::Jobs::CleanupExpiredEventsJob.perform
    #
    class CleanupExpiredEventsJob
      include Sidekiq::Job if defined?(Sidekiq::Job)

      sidekiq_options queue: :default, retry: 3 if defined?(Sidekiq)

      class << self
        # Perform cleanup (can be called directly or via Sidekiq)
        def perform
          return unless OutboxRelay.cleanup_enabled

          new.perform
        end
      end

      def perform
        started_at = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)

        events_deleted = delete_expired_events
        dlq_deleted = delete_resolved_dlq_entries

        duration = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) - started_at

        if events_deleted.positive? || dlq_deleted.positive?
          OutboxRelay.logger.info(
            event_name: 'outbox_relay_cleanup_completed',
            events_deleted: events_deleted,
            dlq_deleted: dlq_deleted,
            duration_ms: (duration * 1000).round(2),
            timestamp: Time.current.iso8601
          )
        end

        ActiveSupport::Notifications.instrument(
          'cleanup_completed.outbox_relay',
          events_deleted: events_deleted,
          dlq_deleted: dlq_deleted,
          duration: duration
        )

        { events_deleted: events_deleted, dlq_deleted: dlq_deleted, duration: duration }
      rescue ActiveRecord::ConnectionNotEstablished, PG::ConnectionBad => e
        log_error('outbox_relay_cleanup_database_error', e)
        OutboxRelay::Instrumentation::Models.error(
          e,
          model: 'CleanupExpiredEventsJob',
          operation: 'cleanup',
          severity: 'warning'
        )
        raise # Let Sidekiq retry
      rescue PG::QueryCanceled => e
        OutboxRelay.logger.error(
          event_name: 'outbox_relay_cleanup_timeout',
          error_message: e.message,
          note: 'Query timeout during cleanup - will retry in next scheduled run'
        )
        { events_deleted: 0, dlq_deleted: 0, duration: 0, timeout: true }
      rescue StandardError => e
        log_error('outbox_relay_cleanup_unexpected_error', e)
        OutboxRelay::Instrumentation::Models.error(
          e,
          model: 'CleanupExpiredEventsJob',
          operation: 'cleanup',
          severity: 'critical'
        )
        raise
      end

      private

      # Delete expired events that have been fully consumed.
      #
      # An event is deleted only if its sequence is strictly less than the minimum
      # last_consumed_sequence across all consumer groups for its topic. This
      # guarantees every consumer group has already processed it.
      def delete_expired_events
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
          .limit(OutboxRelay.cleanup_batch_size)
          .delete_all
      end

      # Delete resolved DLQ entries older than the configured TTL.
      #
      # Only touches entries with terminal resolution_status (resolved, reprocessed,
      # ignored). Unresolved / retrying entries are preserved unconditionally.
      #
      # Returns 0 when dlq_resolved_ttl is not configured.
      def delete_resolved_dlq_entries
        ttl = OutboxRelay.dlq_resolved_ttl
        return 0 unless ttl

        OutboxRelay::DeadLetterEvent
          .where(resolution_status: %w[resolved reprocessed ignored])
          .where('created_at < ?', ttl.ago)
          .limit(OutboxRelay.cleanup_batch_size)
          .delete_all
      end

      def log_error(event_name, error)
        OutboxRelay.logger.error(
          event_name: event_name,
          error_class: error.class.name,
          error_message: error.message,
          backtrace: error.backtrace&.first(10)&.join("\n")
        )
      end
    end
  end
end
