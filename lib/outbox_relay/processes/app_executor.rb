# frozen_string_literal: true

module OutboxRelay
  module Processes
    # AppExecutor - Rails executor wrapper for thread-safe async operations
    #
    # ## Purpose
    #
    # The Rails executor (Rails.application.executor) manages application state
    # during asynchronous operations. It ensures proper handling of:
    #   - Database connection management (checkout/checkin)
    #   - Code reloading in development mode
    #   - Transaction boundaries and callbacks
    #   - Query caching lifecycle
    #   - Middleware stack execution
    #
    # ## Why Wrapping Is Critical
    #
    # Fork-based workers inherit parent's memory but need their own:
    #   - Database connection pool
    #   - Rails application context
    #   - Autoloading/reloading state
    #
    # Without executor wrapping:
    #   - Autoloading may fail (constant not found)
    #   - DB connections may leak or error
    #   - Development mode reloading breaks
    #   - Query cache persists incorrectly
    #
    # ## When It's Applied
    #
    # The executor wrapper is used in two places:
    #   1. Worker polling loop (poll method)
    #   2. Batch processing (process_batch method)
    #
    # This ensures every database query and event processing happens
    # within proper Rails application context.
    #
    # ## Configuration
    #
    # By default, uses Rails.application.executor (if Rails is present).
    # Can be customized via OutboxRelay.app_executor:
    #
    #   OutboxRelay.app_executor = MyCustomExecutor.new
    #
    # Set to nil to disable wrapping (not recommended in Rails apps).
    #
    # ## Error Handling
    #
    # Errors within the executor wrapper are framework-level errors
    # (OutboxRelay internal issues), NOT application errors (event handler bugs).
    #
    # Framework errors are:
    #   - Logged via OutboxRelay.logger.error
    #   - Reported via on_thread_error callback
    #   - Instrumented for monitoring
    #
    # Application errors (inside event handlers) are handled separately
    # by the consumer/handler code and are NOT caught by this wrapper.
    #
    # ## Solid Queue Compatibility
    #
    # This module implements the same pattern as Solid Queue's AppExecutor:
    #   - wrap_in_app_executor method
    #   - handle_thread_error method
    #   - Optional executor with graceful fallback
    #
    # This ensures OutboxRelay integrates seamlessly with Rails like Solid Queue.
    #
    module AppExecutor
      # Wrap a code block with Rails application executor
      #
      # If app_executor is configured, wraps execution with Rails context.
      # Otherwise, executes block directly (useful for non-Rails apps).
      #
      # @param source [String] Source identifier for instrumentation
      # @yield Block to execute within Rails context
      # @return Result of block execution
      #
      # @example Basic usage
      #   wrap_in_app_executor do
      #     # Database queries and event processing
      #     consumer.consume_batch(batch_size: 100)
      #   end
      #
      # @example With error handling
      #   wrap_in_app_executor do
      #     process_events
      #   rescue => e
      #     handle_thread_error(e)
      #   end
      #
      def wrap_in_app_executor(source: "outbox_relay", &block)
        if OutboxRelay.app_executor
          # Rails executor configured - wrap with Rails context
          # This ensures proper DB connection management, reloading, etc.
          OutboxRelay.app_executor.wrap(source: source, &block)
        else
          # No executor configured - execute directly
          # This is fine for non-Rails apps or when explicitly disabled
          yield
        end
      end

      # Handle thread-level errors from OutboxRelay framework
      #
      # This is for errors WITHIN OutboxRelay (polling, batch processing, etc.),
      # NOT for errors within user event handlers.
      #
      # Errors are:
      #   1. Logged to OutboxRelay.logger
      #   2. Instrumented via ActiveSupport::Notifications
      #   3. Reported to on_thread_error callback (if configured)
      #
      # @param error [Exception] Exception raised in worker thread
      #
      # @example Configure error callback
      #   OutboxRelay.on_thread_error = ->(error) do
      #     Sentry.capture_exception(error, level: :error)
      #   end
      #
      def handle_thread_error(error)
        # Log error details
        logger = OutboxRelay.logger
        logger.error(
          event_name: "outbox_relay_thread_error",
          error: error.message,
          error_class: error.class.name,
          backtrace: error.backtrace&.first(10)&.join("\n")
        )

        # Instrument for monitoring (Datadog, New Relic, etc.)
        OutboxRelay.instrument(:thread_error, error: error)

        # Call user-configured error handler
        if OutboxRelay.on_thread_error
          OutboxRelay.on_thread_error.call(error)
        end
      rescue => callback_error
        # Error handler itself failed - log but don't raise
        # This prevents error handling from causing additional failures
        logger = OutboxRelay.logger
        logger.error(
          event_name: "thread_error_handler_failed",
          original_error: error.message,
          callback_error: callback_error.message,
          message: "Error handler itself raised an exception"
        )
      end
    end
  end
end
