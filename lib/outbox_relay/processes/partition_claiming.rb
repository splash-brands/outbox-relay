# frozen_string_literal: true

module OutboxRelay
  module Processes
    # PartitionClaiming - Database-backed partition claiming for distributed workers
    #
    # This module implements exclusive partition ownership using database-backed claims
    # with TTL-based leases. It ensures only ONE worker processes each partition at any
    # time, even across multiple ECS instances or deployments.
    #
    # ## Problem
    #
    # Without partition claiming, multiple ECS instances spawn workers for ALL partitions:
    #
    #   ECS Task 1: [worker p0, worker p1, worker p2, worker p3]
    #   ECS Task 2: [worker p0, worker p1, worker p2, worker p3]  ← DUPLICATES!
    #
    # This causes:
    #   - Wasted CPU (duplicate polling)
    #   - Lock contention (multiple workers compete for same events)
    #   - Potential duplicate processing
    #
    # ## Solution
    #
    # Workers claim partitions on boot. Only the claim holder processes events:
    #
    #   ECS Task 1: [worker p0 ✅, worker p1 ✅, worker p2 exit, worker p3 exit]
    #   ECS Task 2: [worker p0 exit, worker p1 exit, worker p2 ✅, worker p3 ✅]
    #
    # ## Claim Lifecycle
    #
    #   1. Boot:     Worker calls claim_partition → try_claim!
    #   2. Success:  Worker registers and starts polling
    #   3. Failure:  Worker exits gracefully (code 0, supervisor backoff)
    #   4. Run:      Heartbeat calls renew_partition_claim every 10s
    #   5. Shutdown: Worker calls release_partition_claim → release_claim!
    #   6. Crash:    Claim expires after TTL (30s) → another worker claims
    #
    # ## Failover
    #
    # If a worker dies without releasing its claim:
    #   - Claim TTL expires after 30 seconds
    #   - Another worker can then claim the partition
    #   - Events are processed with at-most 30s delay
    #
    # ## Usage
    #
    #   class Worker < Processes::Poller
    #     include Processes::PartitionClaiming
    #
    #     before_shutdown :release_partition_claim
    #   end
    #
    module PartitionClaiming
      CLAIM_TTL = 30.seconds

      # Claim partition for exclusive processing.
      # Called during worker boot, after reconnect_after_fork, before register.
      #
      # If partition is already claimed by another active worker, this worker
      # will exit gracefully (code 0) to avoid duplicate processing.
      #
      # @return [void]
      # @raise [SystemExit] if partition already claimed (exit code 0)
      def claim_partition
        offset = get_or_create_consumer_offset

        claimed = offset.try_claim!(
          consumer_instance_id: build_consumer_instance_id,
          ttl: CLAIM_TTL
        )

        unless claimed
          log_claim_failed(offset)
          exit_gracefully_for_claim_failure
        end

        @partition_claimed = true
        @partition_offset = offset

        log_claim_success(offset)
      end

      # Renew partition claim during heartbeat.
      # Called periodically (every 10s) to extend the claim TTL.
      #
      # If renewal fails (claim was stolen), the worker will stop.
      #
      # @return [void]
      def renew_partition_claim
        return unless @partition_claimed && @partition_offset

        renewed = @partition_offset.renew_claim!(
          consumer_instance_id: build_consumer_instance_id,
          ttl: CLAIM_TTL
        )

        unless renewed
          log_claim_lost
          stop  # Stop worker - supervisor will restart and it can try to reclaim
        end
      end

      # Release partition claim during shutdown.
      # Called as before_shutdown callback for graceful cleanup.
      #
      # This allows another worker to immediately claim the partition
      # instead of waiting for TTL expiration.
      #
      # @return [void]
      def release_partition_claim
        return unless @partition_claimed && @partition_offset

        released = @partition_offset.release_claim!(
          consumer_instance_id: build_consumer_instance_id
        )

        if released
          OutboxRelay.logger.info(
            event_name: "partition_claim_released",
            consumer_group: consumer_group,
            topic: topic,
            partition_key: partition_key,
            consumer_instance_id: build_consumer_instance_id
          )
        end

        @partition_claimed = false
      rescue => e
        # Best-effort cleanup - don't raise during shutdown
        OutboxRelay.logger.warn(
          event_name: "partition_claim_release_failed",
          consumer_group: consumer_group,
          topic: topic,
          partition_key: partition_key,
          error: e.message
        )
      end

      private

      def get_or_create_consumer_offset
        OutboxRelay::ConsumerOffset.find_or_create_by!(
          consumer_group: consumer_group_with_partition,
          topic: topic
        ) do |offset|
          offset.last_consumed_sequence = 0
        end
      end

      def consumer_group_with_partition
        "#{consumer_group}_p#{partition_key}"
      end

      def build_consumer_instance_id
        @consumer_instance_id ||= "#{consumer_group}-#{Socket.gethostname}-#{::Process.pid}-p#{partition_key}"
      end

      def exit_gracefully_for_claim_failure
        OutboxRelay.logger.info(
          event_name: "worker_exiting_claim_unavailable",
          consumer_group: consumer_group,
          topic: topic,
          partition_key: partition_key,
          message: "Partition already claimed by another worker. Exiting gracefully."
        )

        # Exit with code 0 (success) so supervisor doesn't immediately restart.
        # Supervisor will restart with backoff, giving time for claims to settle.
        exit(0)
      end

      def log_claim_failed(offset)
        OutboxRelay.logger.warn(
          event_name: "partition_claim_failed",
          consumer_group: consumer_group,
          topic: topic,
          partition_key: partition_key,
          claimed_by: offset.claimed_by,
          claimed_until: offset.claimed_until&.iso8601,
          my_instance_id: build_consumer_instance_id
        )
      end

      def log_claim_success(offset)
        OutboxRelay.logger.info(
          event_name: "partition_claimed",
          consumer_group: consumer_group,
          topic: topic,
          partition_key: partition_key,
          consumer_instance_id: build_consumer_instance_id,
          claimed_until: offset.claimed_until.iso8601
        )
      end

      def log_claim_lost
        OutboxRelay.logger.error(
          event_name: "partition_claim_lost",
          consumer_group: consumer_group,
          topic: topic,
          partition_key: partition_key,
          consumer_instance_id: build_consumer_instance_id,
          current_claimer: @partition_offset.reload.claimed_by,
          message: "Lost partition claim! Another worker took over. Stopping."
        )
      end
    end
  end
end
