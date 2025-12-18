# frozen_string_literal: true

module OutboxRelay
  module Jobs
    # Periodically cleans up expired OutboxRelay events from the database
    #
    # Events are deleted only if:
    # 1. They have expired (expires_at < current time), AND
    # 2. ALL consumer groups have processed them (sequence < min consumed sequence)
    #
    # This prevents database bloat from old events while ensuring at-least-once
    # delivery guarantee is maintained. Events without expires_at are never deleted
    # by this job (they must be cleaned up manually or via separate retention policy).
    #
    # ## Configuration
    #
    # Enable cleanup in your OutboxRelay initializer:
    #
    #   OutboxRelay.configuration.cleanup_enabled = true        # Enable/disable cleanup (default: false)
    #   OutboxRelay.configuration.cleanup_batch_size = 10_000   # Delete up to 10k events per run (default: 10_000)
    #
    # ## Sidekiq Integration
    #
    # If using Sidekiq Enterprise periodic jobs, register the job with cron syntax:
    #
    #   Sidekiq.configure_server do |config|
    #     config.periodic do |mgr|
    #       if OutboxRelay.configuration.cleanup_enabled
    #         mgr.register("*/15 * * * *", "OutboxRelay::Jobs::CleanupExpiredEventsJob")  # Every 15 minutes
    #       end
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
          return unless OutboxRelay.configuration.cleanup_enabled

          new.perform
        end
      end

      def perform
        # Only delete events that have expired AND been consumed by all consumer groups
        # This ensures we never delete events that haven't been processed yet
        batch_size = OutboxRelay.configuration.cleanup_batch_size

        deleted_count = OutboxRelay::OutboxEvent
          .where("expires_at IS NOT NULL AND expires_at < ?", Time.current)
          .where(
            "sequence < ALL(
              SELECT COALESCE(MIN(last_consumed_sequence), 0)
              FROM outbox_relay_consumer_offsets
              WHERE topic = outbox_relay_outbox_events.topic
            )"
          )
          .limit(batch_size)
          .delete_all

        if deleted_count > 0
          # INFO: This is a successful cleanup operation, not a warning
          OutboxRelay.logger.info(
            event_name: "outbox_relay_expired_events_cleaned",
            deleted_count: deleted_count,
            batch_size: batch_size,
            timestamp: Time.current.iso8601
          )
        end

        deleted_count
      rescue ActiveRecord::ConnectionNotEstablished, PG::ConnectionBad => db_error
        # Database connectivity issue - re-raise for retry (if using job queue)
        OutboxRelay.logger.error(
          event_name: "outbox_relay_cleanup_database_error",
          error_class: db_error.class.name,
          error_message: db_error.message,
          backtrace: db_error.backtrace&.first(10)&.join("\n")
        )

        # Report to monitoring backend
        OutboxRelay::Instrumentation::Models.error(
          db_error,
          model: "CleanupExpiredEventsJob",
          operation: "cleanup",
          severity: "warning"
        )

        raise # Let Sidekiq retry the job
      rescue PG::QueryCanceled => timeout_error
        # Query timeout - log and return 0 (non-critical, will retry next run)
        OutboxRelay.logger.error(
          event_name: "outbox_relay_cleanup_timeout",
          error_message: timeout_error.message,
          note: "Query timeout during cleanup - will retry in next scheduled run"
        )
        0 # Don't re-raise - timeout is expected for large datasets
      rescue StandardError => e
        # Unexpected error - log and re-raise for investigation
        OutboxRelay.logger.error(
          event_name: "outbox_relay_cleanup_unexpected_error",
          error_class: e.class.name,
          error_message: e.message,
          backtrace: e.backtrace&.first(10)&.join("\n")
        )

        OutboxRelay::Instrumentation::Models.error(
          e,
          model: "CleanupExpiredEventsJob",
          operation: "cleanup",
          severity: "critical"
        )

        raise # Let Sidekiq retry and alert on repeated failures
      end
    end
  end
end
