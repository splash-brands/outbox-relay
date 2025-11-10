# frozen_string_literal: true

module OutboxRelay
  module Processes
    class Poller < Base
      include Runnable

      attr_accessor :polling_interval

      def initialize(polling_interval:, **options)
        @polling_interval = polling_interval
        super(**options)
      end

      def metadata
        super.merge(polling_interval: polling_interval)
      end

      private

      def run
        start_polling_loop
      end

      def start_polling_loop
        loop do
          break if shutting_down?

          delay = poll_with_instrumentation
          interruptible_sleep(delay)
        end
      ensure
        OutboxRelay.instrument(:shutdown_process, process: self) do
          run_callbacks(:shutdown) { shutdown }
        end

        deregister
      end

      def poll_with_instrumentation
        begin
          OutboxRelay.instrument(:poll, process: self) do
            begin
              with_polling_volume { poll }
            rescue => e
              # Polling errors - these are expected and handled
              custom_logger.error(
                event_name: "polling_error",
                process_id: process_id,
                name: name,
                error: e.message,
                error_class: e.class.name,
                backtrace: e.backtrace&.first(10)&.join("\n")
              )

              Sentry.capture_exception(e, extra: {
                process_id: process_id,
                name: name,
                phase: "poll"
              }) if defined?(Sentry)

              # On polling error, wait full interval before retry
              return polling_interval
            end
          end
        rescue => e
          # Instrumentation errors - these should be rare and indicate system issues
          custom_logger.error(
            event_name: "instrumentation_error",
            process_id: process_id,
            name: name,
            error: e.message,
            error_class: e.class.name,
            backtrace: e.backtrace&.first(10)&.join("\n")
          )

          Sentry.capture_exception(e, extra: {
            process_id: process_id,
            name: name,
            phase: "instrumentation",
            severity: "high"
          }) if defined?(Sentry)

          # On instrumentation error, still try to poll
          begin
            with_polling_volume { poll }
          rescue => poll_error
            custom_logger.error(
              event_name: "polling_error_after_instrumentation_failure",
              process_id: process_id,
              name: name,
              error: poll_error.message,
              error_class: poll_error.class.name
            )
            polling_interval
          end
        end
      end

      def poll
        raise NotImplementedError, "Subclasses must implement #poll"
      end

      def with_polling_volume
        if OutboxRelay.silence_polling? && ActiveRecord::Base.logger
          begin
            ActiveRecord::Base.logger.silence { yield }
          rescue => e
            # Logger silencing failed - log and continue without silencing
            custom_logger.warn(
              event_name: "logger_silencing_failed",
              process_id: process_id,
              error: e.message,
              error_class: e.class.name
            )

            # Execute poll without silencing
            yield
          end
        else
          yield
        end
      rescue => e
        # Yield itself failed - this is a poll error, let it bubble up
        raise
      end
    end
  end
end
