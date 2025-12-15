# frozen_string_literal: true

module OutboxRelay
  module Processes
    module Runnable
      attr_writer :mode

      def start
        boot
        run
      end

      def stop
        super
        wake_up
      end

      private

      DEFAULT_MODE = :fork

      def mode
        (@mode || DEFAULT_MODE).to_s.inquiry
      end

      def boot
        OutboxRelay.instrument(:start_process, process: self) do
          run_callbacks(:boot) do
            if running_as_fork?
              # Capture supervisor PID immediately after fork
              # ::Process.ppid returns the parent (supervisor) PID
              @supervisor_pid = ::Process.ppid

              reconnect_after_fork

              # Claim partition before registration (Workers only)
              # If claim fails, worker exits gracefully and doesn't register
              claim_partition if respond_to?(:claim_partition)

              register
              start_heartbeat  # Start automatic heartbeat after registration
              register_signal_handlers
              set_procline
            end
          end
        end
      end

      def reconnect_after_fork
        # Critical: ActiveRecord connections are not fork-safe and MUST be
        # reestablished after fork to prevent crashes and connection conflicts
        max_retries = 3
        retry_count = 0

        begin
          ActiveRecord::Base.connection_handler.clear_all_connections!
          ActiveRecord::Base.establish_connection

          # Force immediate connection to ensure gssencmode setting takes effect
          # Lazy connection establishment can bypass gssencmode on macOS
          ActiveRecord::Base.connection.execute("SELECT 1")

          OutboxRelay.logger.debug(
            event_name: "activerecord_reconnected_after_fork",
            process_id: process_id,
            name: name,
            pid: ::Process.pid,
            database: ActiveRecord::Base.connection_db_config.database
          )

        rescue => e
          retry_count += 1

          if retry_count < max_retries
            backoff_seconds = retry_count ** 2  # 1s, 4s, 9s

            OutboxRelay.logger.warn(
              event_name: "activerecord_reconnection_retry",
              process_id: process_id,
              error: e.message,
              retry_count: retry_count,
              max_retries: max_retries,
              backoff_seconds: backoff_seconds
            )

            sleep(backoff_seconds)
            retry
          else
            OutboxRelay.logger.error(
              event_name: "activerecord_reconnection_failed",
              process_id: process_id,
              error: e.message,
              error_class: e.class.name,
              backtrace: e.backtrace&.first(10)&.join("\n"),
              retries_exhausted: true
            )

            OutboxRelay::Instrumentation::Runnable.reconnect_error(
              e,
              process_id: process_id,
              attempt: retry_count,
              max_attempts: max_retries
            )

            raise ConnectionError, "Failed to reconnect after #{max_retries} attempts: #{e.message}"
          end
        end
      end

      def shutting_down?
        stopped? ||
        (supervised? && supervisor_went_away?) ||
        finished? ||
        !registered?
      end

      def run
        raise NotImplementedError, "Subclasses must implement #run"
      end

      def finished?
        false
      end

      def shutdown
        # Stop heartbeat before deregistration
        # This ensures no heartbeat attempts during shutdown
        stop_heartbeat if respond_to?(:stop_heartbeat)

        # Restore original signal handlers
        # This allows Ruby to handle signals normally after shutdown
        restore_signal_handlers if respond_to?(:restore_signal_handlers)

        # Close self-pipe to prevent file descriptor leak
        close_self_pipe if respond_to?(:close_self_pipe)

        # Deregister from database
        # This removes process record and cascades to supervisees
        deregister if respond_to?(:deregister)

        # Override in subclasses for additional cleanup
      end

      def set_procline
        # Override in subclasses
      end

      def running_as_fork?
        mode.fork?
      end

      def register_signal_handlers
        # Basic signal handling
        # Subclasses can override for more sophisticated handling
        %w[TERM INT].each do |signal|
          begin
            Signal.trap(signal) do
              # Signal handlers must be kept simple
              # Don't do complex logging inside handler - just set flags
              stop
            end

            OutboxRelay.logger.debug(
              event_name: "signal_handler_registered",
              signal: signal,
              process_id: process_id
            )
          rescue => e
            OutboxRelay.logger.error(
              event_name: "signal_handler_registration_failed",
              signal: signal,
              error: e.message,
              error_class: e.class.name,
              backtrace: e.backtrace&.first(5)&.join("\n")
            )

            OutboxRelay::Instrumentation::Runnable.fork_initialization_error(
              e,
              consumer_class: "signal_handler",
              partition_key: signal
            )

            # This is critical - without signal handlers, process can't be stopped gracefully
            raise OutboxRelay::Error, "Failed to register #{signal} handler: #{e.message}"
          end
        end
      end

      def handle_thread_error(exception)
        OutboxRelay.logger.error(
          event_name: "thread_error",
          process_id: process_id,
          name: name,
          error: exception.message,
          backtrace: exception.backtrace&.first(10)&.join("\n")
        )
      end
    end
  end
end
