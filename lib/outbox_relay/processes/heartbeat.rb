# frozen_string_literal: true

module OutboxRelay
  module Processes
    # Automatic periodic heartbeat for process liveness detection
    #
    # Uses Concurrent::TimerTask to update database heartbeat timestamp every N seconds.
    # Critical for ECS deployments: Enables detection of hung/crashed processes.
    #
    # Features:
    # - Non-blocking: Runs in separate thread pool, doesn't interfere with main loop
    # - Graceful shutdown: Task is stopped before deregistration
    # - Failure handling: After max consecutive failures, process shuts down
    # - Signal-safe: No complex operations in timer callback
    module Heartbeat
      # Default heartbeat interval in seconds
      # Balance between database load and detection speed
      DEFAULT_HEARTBEAT_INTERVAL = 10

      # Maximum consecutive heartbeat failures before shutting down
      # After 3 failures (30 seconds), assume database is unreachable
      DEFAULT_MAX_HEARTBEAT_FAILURES = 3

      # Start automatic heartbeat after boot
      # Called automatically by runnable lifecycle
      def start_heartbeat
        return unless registered?  # Only start if registration succeeded

        # Lazy initialize heartbeat state if not already set
        # This handles cases where initialize chain wasn't called
        @heartbeat_interval ||= DEFAULT_HEARTBEAT_INTERVAL
        @max_heartbeat_failures ||= DEFAULT_MAX_HEARTBEAT_FAILURES
        @heartbeat_failures ||= Concurrent::AtomicFixnum.new(0)

        @heartbeat_task = Concurrent::TimerTask.new(
          execution_interval: @heartbeat_interval,
          timeout_interval: @heartbeat_interval - 1,  # Give 1 second buffer
          run_now: false  # Don't run immediately, first heartbeat after interval
        ) do
          # Timer callback - wrap in executor to manage database connections properly
          # Primary error handling in Registrable#heartbeat
          # Task-level errors handled by handle_heartbeat_task_error observer
          #
          # Note: wrap_in_app_executor is only available in Worker (via AppExecutor module)
          # Supervisor doesn't need it since it doesn't interact with application code
          if respond_to?(:wrap_in_app_executor, true)
            wrap_in_app_executor { heartbeat }
          else
            heartbeat
          end
        end

        # Add error handler for task itself
        @heartbeat_task.add_observer(self, :handle_heartbeat_task_error)

        @heartbeat_task.execute

        OutboxRelay.logger.info(
          event_name: "heartbeat_started",
          process_id: process_id,
          interval: @heartbeat_interval,
          max_failures: @max_heartbeat_failures
        )
      rescue => e
        OutboxRelay.logger.error(
          event_name: "heartbeat_start_failed",
          process_id: process_id,
          error: e.message,
          error_class: e.class.name,
          backtrace: e.backtrace&.first(5)&.join("\n")
        )

        OutboxRelay::Instrumentation::Heartbeat.start_error(
          e,
          process_id: process_id
        )

        # Don't fail the process if heartbeat can't start
        # Process can still run, just won't have liveness detection
      end

      # Stop automatic heartbeat before deregistration
      # Called automatically by runnable lifecycle
      def stop_heartbeat
        return unless @heartbeat_task

        # Shutdown the task gracefully
        # This waits for current execution to complete
        @heartbeat_task.shutdown

        # Give it a moment to finish
        unless @heartbeat_task.wait_for_termination(5)
          # Force kill if it doesn't stop gracefully
          @heartbeat_task.kill
          OutboxRelay.logger.warn(
            event_name: "heartbeat_force_killed",
            process_id: process_id,
            reason: "Task didn't stop within 5 seconds"
          )
        end

        @heartbeat_task = nil

        OutboxRelay.logger.info(
          event_name: "heartbeat_stopped",
          process_id: process_id
        )
      rescue => e
        OutboxRelay.logger.error(
          event_name: "heartbeat_stop_failed",
          process_id: process_id,
          error: e.message,
          error_class: e.class.name
        )

        # Don't raise - this is during shutdown
      end

      # Observer callback for heartbeat task errors
      # Called by Concurrent::TimerTask when task itself fails (not heartbeat())
      def handle_heartbeat_task_error(time, result, exception)
        return unless exception

        OutboxRelay.logger.error(
          event_name: "heartbeat_task_error",
          process_id: process_id,
          error: exception.message,
          error_class: exception.class.name,
          backtrace: exception.backtrace&.first(5)&.join("\n")
        )

        OutboxRelay::Instrumentation::Heartbeat.task_error(
          exception,
          process_id: process_id
        )
      end

      private

      # Override in subclasses or instances to customize interval
      attr_writer :heartbeat_interval, :max_heartbeat_failures
    end
  end
end
