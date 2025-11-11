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
        # Prepare metadata for database storage
        # Include supervisor_id if this is a supervised worker
        registration_metadata = metadata.merge(
          supervisor_id: supervisor_id_for_registration
        ).compact

        @db_process = OutboxRelay::Process.register(
          kind: kind,
          name: name,
          supervisor_id: registration_metadata.delete(:supervisor_id),
          **registration_metadata
        )

        OutboxRelay.logger.info(
          event_name: "process_registered",
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

      # Get supervisor database ID for worker registration
      # Workers need to link to their supervisor's database record
      def supervisor_id_for_registration
        return nil unless supervised?

        # For workers, find supervisor's database record by PID
        # @supervisor_pid was captured after fork in boot
        supervisor_record = OutboxRelay::Process.find_by(pid: @supervisor_pid)
        supervisor_record&.id
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

        OutboxRelay.logger.info(
          event_name: "process_deregistered",
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

        success = @db_process.heartbeat

        if success
          @heartbeat_failures.value = 0  # Reset on success
        else
          # Process was deregistered or record deleted
          failures = @heartbeat_failures.increment

          OutboxRelay.logger.warn(
            event_name: "heartbeat_process_not_found",
            process_id: process_id,
            db_id: @db_process.id,
            consecutive_failures: failures
          )
        end
      rescue => e
        failures = @heartbeat_failures.increment

        OutboxRelay.logger.error(
          event_name: "process_heartbeat_failed",
          process_id: process_id,
          db_id: @db_process&.id,
          error: e.message,
          consecutive_failures: failures,
          max_failures: @max_heartbeat_failures
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
