# frozen_string_literal: true

require "active_support/notifications"

module OutboxRelay
  # Instrumentation module for OutboxRelay observability
  #
  # This module provides a centralized interface for emitting observability events
  # using ActiveSupport::Notifications. Applications can subscribe to these events
  # and route them to their monitoring backend of choice (Sentry, DataDog, etc.).
  #
  # @example Subscribe to all OutboxRelay events
  #   ActiveSupport::Notifications.subscribe(/^outbox_relay\./) do |name, start, finish, id, payload|
  #     # Route to your monitoring backend
  #     if payload[:exception]
  #       Sentry.capture_exception(payload[:exception], extra: payload.except(:exception))
  #     end
  #   end
  #
  # @example Subscribe to specific event types
  #   ActiveSupport::Notifications.subscribe("outbox_relay.worker.poll_error") do |name, start, finish, id, payload|
  #     Sentry.capture_exception(payload[:exception], extra: payload.except(:exception))
  #   end
  module Instrumentation
    # Emit an error event with exception details
    #
    # @param event_name [String] The event name (e.g., "worker.poll_error")
    # @param exception [Exception] The exception to report
    # @param payload [Hash] Additional context (process_id, severity, phase, etc.)
    def self.error(event_name, exception, **payload)
      full_event_name = "outbox_relay.#{event_name}"

      ActiveSupport::Notifications.instrument(
        full_event_name,
        payload.merge(
          exception: exception,
          error: exception.message,
          error_class: exception.class.name,
          backtrace: exception.backtrace&.first(10)
        )
      )
    end

    # Emit a message event (for warnings or informational alerts)
    #
    # @param event_name [String] The event name (e.g., "supervisor.boot_incomplete")
    # @param message [String] The message to report
    # @param payload [Hash] Additional context
    def self.message(event_name, message, **payload)
      full_event_name = "outbox_relay.#{event_name}"

      ActiveSupport::Notifications.instrument(
        full_event_name,
        payload.merge(message: message)
      )
    end

    # Event Categories and Helper Methods
    #
    # These methods provide semantic interfaces for common event types
    # and ensure consistent payload structures across the gem.

    module Worker
      def self.poll_error(exception, process_id:, consumer_class:, partition_key:, loop_count:, total_processed:)
        Instrumentation.error(
          "worker.poll_error",
          exception,
          process_id: process_id,
          consumer_class: consumer_class,
          partition_key: partition_key,
          loop_count: loop_count,
          total_processed: total_processed,
          severity: "critical"
        )
      end

      def self.delay_calculation_error(exception, process_id:, consumer_class:, partition_key:, processed_count:)
        Instrumentation.error(
          "worker.delay_calculation_error",
          exception,
          process_id: process_id,
          consumer_class: consumer_class,
          partition_key: partition_key,
          processed_count: processed_count,
          severity: "high"
        )
      end
    end

    module Heartbeat
      def self.failure(exception, process_id:, consecutive_failures:, max_failures:)
        # Dynamic severity escalation based on failure count
        severity = consecutive_failures >= max_failures ? "critical" : "warning"

        Instrumentation.error(
          "heartbeat.failure",
          exception,
          process_id: process_id,
          consecutive_failures: consecutive_failures,
          max_failures: max_failures,
          severity: severity
        )
      end

      def self.task_error(exception, process_id:)
        Instrumentation.error(
          "heartbeat.task_error",
          exception,
          process_id: process_id,
          severity: "high",
          context: "heartbeat_timer_task"
        )
      end

      def self.start_error(exception, process_id:)
        Instrumentation.error(
          "heartbeat.start_error",
          exception,
          process_id: process_id,
          severity: "high"
        )
      end
    end

    module Process
      def self.registration_failed(exception, name:, kind:, pid:)
        Instrumentation.error(
          "process.registration_failed",
          exception,
          name: name,
          kind: kind,
          pid: pid,
          severity: "critical"
        )
      end

      def self.deregistration_failed(exception, process_id:, db_id:, name:)
        Instrumentation.error(
          "process.deregistration_failed",
          exception,
          process_id: process_id,
          db_id: db_id,
          name: name,
          phase: "shutdown",
          severity: "high"
        )
      end

      def self.heartbeat_failed(exception, process_id:, db_id:, consecutive_failures:, max_failures:)
        # Dynamic severity escalation
        severity = consecutive_failures >= max_failures ? "critical" : "warning"

        Instrumentation.error(
          "process.heartbeat_failed",
          exception,
          process_id: process_id,
          db_id: db_id,
          consecutive_failures: consecutive_failures,
          max_failures: max_failures,
          severity: severity
        )
      end

      def self.run_error(exception, name:)
        Instrumentation.message(
          "process.run_error",
          "Process run loop exited with error",
          name: name,
          error: exception.message,
          error_class: exception.class.name,
          severity: "error"
        )
      end
    end

    module Supervisor
      def self.boot_incomplete(total_expected:, running_workers:, failed_workers:, failed_details:)
        Instrumentation.message(
          "supervisor.boot_incomplete",
          "Supervisor boot completed with failures",
          total_expected: total_expected,
          running_workers: running_workers,
          failed_workers: failed_workers,
          failed_details: failed_details,
          severity: "error"
        )
      end

      def self.fork_error(exception, worker_name:, consumer_class:)
        Instrumentation.error(
          "supervisor.fork_error",
          exception,
          worker_name: worker_name,
          consumer_class: consumer_class,
          severity: "critical",
          phase: "fork"
        )
      end

      def self.restart_abandoned(worker_name:, worker_key:, exit_status:, restart_attempts:)
        Instrumentation.message(
          "supervisor.restart_abandoned",
          "Worker restart abandoned after excessive failures",
          worker_name: worker_name,
          worker_key: worker_key,
          exit_status: exit_status,
          restart_attempts: restart_attempts,
          severity: "error"
        )
      end
    end

    module Poller
      def self.poll_error(exception, process_id:, name:)
        Instrumentation.error(
          "poller.poll_error",
          exception,
          process_id: process_id,
          name: name,
          phase: "poll",
          severity: "warning"
        )
      end

      def self.instrumentation_error(exception, process_id:, name:)
        Instrumentation.error(
          "poller.instrumentation_error",
          exception,
          process_id: process_id,
          name: name,
          phase: "instrumentation",
          severity: "high"
        )
      end
    end

    module Configuration
      def self.partition_count_query_failed(exception, topic:)
        Instrumentation.error(
          "configuration.partition_count_query_failed",
          exception,
          topic: topic,
          severity: "critical"
        )
      end
    end

    module Tasks
      # Generic task error instrumentation for rake tasks
      def self.task_error(exception, task_name:, **context)
        Instrumentation.error(
          "tasks.error",
          exception,
          task_name: task_name,
          severity: "high",
          **context
        )
      end
    end

    module Models
      # Generic model error instrumentation
      def self.error(exception, model:, operation:, **context)
        Instrumentation.error(
          "models.error",
          exception,
          model: model,
          operation: operation,
          severity: "high",
          **context
        )
      end
    end

    module Callbacks
      def self.boot_failed(exception, callback:)
        Instrumentation.error(
          "callbacks.boot_failed",
          exception,
          callback: callback,
          phase: "boot",
          severity: "warning"
        )
      end

      def self.shutdown_block_failed(exception)
        Instrumentation.error(
          "callbacks.shutdown_block_failed",
          exception,
          phase: "shutdown_block",
          severity: "warning"
        )
      end

      def self.shutdown_failed(exception, callback:)
        Instrumentation.error(
          "callbacks.shutdown_failed",
          exception,
          callback: callback,
          phase: "shutdown",
          severity: "warning"
        )
      end
    end

    module Runnable
      def self.reconnect_error(exception, process_id:, attempt:, max_attempts:)
        Instrumentation.error(
          "runnable.reconnect_error",
          exception,
          process_id: process_id,
          attempt: attempt,
          max_attempts: max_attempts,
          severity: "high"
        )
      end

      def self.fork_initialization_error(exception, consumer_class:, partition_key:)
        Instrumentation.error(
          "runnable.fork_initialization_error",
          exception,
          consumer_class: consumer_class,
          partition_key: partition_key,
          severity: "critical"
        )
      end
    end

    module Signals
      def self.signal_handler_error(exception, signal_name:)
        Instrumentation.error(
          "signals.handler_error",
          exception,
          signal_name: signal_name,
          severity: "high"
        )
      end
    end

    module CLI
      def self.start_error(exception)
        Instrumentation.error(
          "cli.start_error",
          exception,
          severity: "critical"
        )
      end
    end
  end
end
