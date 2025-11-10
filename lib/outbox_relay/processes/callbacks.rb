# frozen_string_literal: true

module OutboxRelay
  module Processes
    module Callbacks
      extend ActiveSupport::Concern

      included do
        class_attribute :_boot_callbacks, :_shutdown_callbacks, default: []
      end

      class_methods do
        def after_boot(method_name)
          self._boot_callbacks += [method_name]
        end

        def before_shutdown(method_name)
          self._shutdown_callbacks += [method_name]
        end
      end

      def run_callbacks(kind, &block)
        case kind
        when :boot
          run_boot_callbacks(&block)
        when :shutdown
          run_shutdown_callbacks(&block)
        end
      end

      private

      def run_boot_callbacks
        # Execute registered boot callbacks (e.g., log_worker_start, log_supervisor_start)
        _boot_callbacks.each do |callback|
          send(callback)
        rescue => e
          custom_logger.error(
            event_name: "boot_callback_failed",
            callback: callback,
            error: e.message,
            backtrace: e.backtrace&.first(10)&.join("\n")
          )
          Sentry.capture_exception(e, extra: { callback: callback, phase: "boot" }) if defined?(Sentry)

          # Re-raise critical errors that prevent worker from functioning
          # Most boot callbacks (like logging) are non-critical, so we continue
          # Critical callbacks like :register should handle their own re-raising
          if e.is_a?(ActiveRecord::ConnectionNotEstablished) || e.is_a?(SystemExit)
            raise
          end

          # Log and continue for non-critical callbacks
        end

        # Execute the boot block (register, register_signal_handlers, etc.)
        # These are critical and their errors should propagate
        yield if block_given?
      end

      def run_shutdown_callbacks
        # Execute the shutdown block first (deregister, cleanup, etc.)
        # Best effort - log errors but don't stop shutdown
        if block_given?
          begin
            yield
          rescue => e
            custom_logger.error(
              event_name: "shutdown_block_failed",
              error: e.message,
              backtrace: e.backtrace&.first(10)&.join("\n")
            )
            Sentry.capture_exception(e, extra: { phase: "shutdown_block" }) if defined?(Sentry)
            # Continue with registered callbacks
          end
        end

        # Execute registered shutdown callbacks (e.g., log_worker_stop, log_supervisor_stop)
        _shutdown_callbacks.each do |callback|
          send(callback)
        rescue => e
          custom_logger.error(
            event_name: "shutdown_callback_failed",
            callback: callback,
            error: e.message,
            backtrace: e.backtrace&.first(10)&.join("\n")
          )
          Sentry.capture_exception(e, extra: { callback: callback, phase: "shutdown" }) if defined?(Sentry)

          # Continue with other callbacks during shutdown - best effort cleanup
        end
      end
    end
  end
end
