# frozen_string_literal: true

require "concurrent"

module OutboxRelay
  module Processes
    module Registrable
      # Register process in database registry for fault tolerance
      #
      # Uses PostgreSQL-backed OutboxRelay::Process model for persistence.
      # Critical for ECS deployments: Process state survives container crashes.
      #
      # Registry tracks: process ID, name, kind, hostname, PID, metadata, heartbeats
      def register
        # Get supervisor_id from the supervisor DB process object if available
        # This is set by supervisor.supervised_by() before fork
        # Avoids PID-based lookup which fails in multi-container deployments
        supervisor_id = try(:supervisor_db_process)&.id

        @db_process = OutboxRelay::Process.register(
          kind: kind,
          name: name,
          supervisor_id: supervisor_id,
          **metadata.compact
        )

        OutboxRelay.instrument(
          :process_registered,
          process_id: process_id,
          name: name,
          kind: kind,
          pid: pid,
          db_id: @db_process.id
        )
      rescue => e
        OutboxRelay.logger.error(
          event_name: "process_registration_failed",
          name: name,
          error: e.message,
          error_class: e.class.name,
          backtrace: e.backtrace&.first(10)&.join("\n")
        )

        OutboxRelay::Instrumentation::Process.registration_failed(
          e,
          name: name,
          kind: kind,
          pid: pid
        )

        # Re-raise - a process that can't register shouldn't run
        raise OutboxRelay::Error, "Failed to register process: #{e.message}"
      end

      def deregister
        unless @db_process
          OutboxRelay.logger.warn(
            event_name: "deregister_skipped_no_process",
            name: name,
            reason: "Process was never successfully registered"
          )
          return
        end

        @db_process.deregister

        OutboxRelay.instrument(
          :process_deregistered,
          process_id: process_id,
          name: name,
          kind: kind,
          db_id: @db_process.id
        )
      rescue => e
        OutboxRelay.logger.error(
          event_name: "process_deregistration_failed",
          name: name,
          process_id: process_id,
          db_id: @db_process&.id,
          error: e.message,
          error_class: e.class.name,
          backtrace: e.backtrace&.first(5)&.join("\n")
        )

        OutboxRelay::Instrumentation::Process.deregistration_failed(
          e,
          process_id: process_id,
          db_id: @db_process&.id,
          name: name
        )

        # Don't re-raise during shutdown - this is best-effort cleanup
        # Log for manual cleanup if needed
      end

      def process_id
        @db_process&.id
      end

      def registered?
        @db_process && @db_process.persisted? && !@db_process.destroyed?
      end

      def heartbeat
        return unless @db_process

        # Defensive initialization - ensure counter exists even if initialize didn't run
        @heartbeat_failures ||= Concurrent::AtomicFixnum.new(0)

        # Get current metadata from the process
        # This allows processes to update their state during heartbeat
        # For example, supervisor updates workers_count as forks change
        current_metadata = metadata

        success = @db_process.heartbeat(metadata: current_metadata)

        if success
          @heartbeat_failures.value = 0  # Reset on success
        else
          # Process was deregistered or record deleted
          failures = @heartbeat_failures.increment

          # Severity escalation: INFO → WARN → ERROR
          # First failure is expected during graceful shutdown/deployment
          # Multiple failures indicate a real problem
          severity = case failures
                     when 1 then :info
                     when 2 then :warn
                     else :error
                     end

          OutboxRelay.logger.public_send(
            severity,
            event_name: "heartbeat_process_not_found",
            process_id: process_id,
            db_id: @db_process.id,
            consecutive_failures: failures,
            severity: severity
          )
        end
      rescue => e
        failures = @heartbeat_failures.increment

        # Severity escalation for database errors
        # First failure could be transient (network blip, connection pool exhausted)
        # Multiple failures indicate persistent database issues
        severity = case failures
                   when 1 then :warn
                   when 2 then :error
                   else :error
                   end

        OutboxRelay.logger.public_send(
          severity,
          event_name: "process_heartbeat_failed",
          process_id: process_id,
          db_id: @db_process&.id,
          error: e.message,
          consecutive_failures: failures,
          max_failures: @max_heartbeat_failures,
          severity: severity
        )

        OutboxRelay::Instrumentation::Process.heartbeat_failed(
          e,
          process_id: process_id,
          db_id: @db_process&.id,
          consecutive_failures: failures,
          max_failures: @max_heartbeat_failures
        )

        # After max consecutive failures, assume database is broken and shut down
        if failures >= @max_heartbeat_failures
          OutboxRelay.logger.error(
            event_name: "heartbeat_failures_exceeded",
            process_id: process_id,
            db_id: @db_process&.id,
            consecutive_failures: failures,
            action: "shutting_down",
            reason: "Database appears unreachable - stopping to prevent duplicate processing"
          )
          stop
        end
      end

    end
  end
end
