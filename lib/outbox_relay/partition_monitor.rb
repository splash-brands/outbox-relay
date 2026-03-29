# frozen_string_literal: true

module OutboxRelay
  # PartitionMonitor - Detects orphaned partitions and monitors partition health
  #
  # This class provides monitoring capabilities to detect when partitions have
  # no active workers, helping prevent situations where events stop being processed.
  #
  # ## Problem Solved
  #
  # Without partition monitoring, a worker can die and its partition can remain
  # unprocessed for hours without anyone noticing. This class:
  #   1. Detects orphaned partitions (no active claim)
  #   2. Detects high-lag partitions (falling behind)
  #   3. Provides health reports for dashboards and alerts
  #
  # ## Usage
  #
  #   monitor = OutboxRelay::PartitionMonitor.new
  #
  #   # Get all orphaned partitions
  #   monitor.orphaned_partitions
  #   # => [{ consumer_group: "shipstation", topic: "orders", partition_key: 0, claimed_until: nil }]
  #
  #   # Get health status for all partitions
  #   monitor.partition_health
  #   # => [{ consumer_group: "shipstation", topic: "orders", partition_key: 0, status: :orphaned, lag: 150 }]
  #
  #   # Get comprehensive health report
  #   monitor.health_report
  #   # => { total: 18, active: 17, orphaned: [...], high_lag: [...] }
  #
  class PartitionMonitor
    def initialize(configuration = nil)
      @configuration = configuration || OutboxRelay.configuration
    end

    # Returns Array of orphaned partition info
    # Orphaned = no active claim (claimed_by is nil or claimed_until expired)
    #
    # IMPORTANT: Only returns partitions that are in the current configuration.
    # This filters out stale records from removed consumer groups that may
    # still exist in the database.
    #
    # @return [Array<Hash>] List of orphaned partitions with metadata
    def orphaned_partitions
      # Build set of expected consumer_group values (with partition suffix)
      expected_consumer_groups = expected_partitions.map do |p|
        "#{p[:consumer_group]}_p#{p[:partition_key]}"
      end.to_set

      # Filter orphaned records to only include expected partitions
      ConsumerOffset.orphaned.select do |offset|
        expected_consumer_groups.include?(offset.consumer_group)
      end.map do |offset|
        {
          consumer_group: base_consumer_group(offset.consumer_group),
          topic: offset.topic,
          partition_key: extract_partition_key(offset.consumer_group),
          claimed_until: offset.claimed_until,
          last_consumed_at: offset.last_consumed_at,
          lag: calculate_lag(offset)
        }
      end
    end

    # Returns health status for all expected partitions from configuration
    #
    # @return [Array<Hash>] List of all partitions with their health status
    def partition_health
      expected_partitions.map do |partition|
        offset = find_offset(partition)
        partition.merge(
          status: determine_status(offset),
          lag: offset ? calculate_lag(offset) : 0,
          claimed_by: offset&.claimed_by,
          claimed_until: offset&.claimed_until,
          last_consumed_at: offset&.last_consumed_at,
          heartbeat_at: offset&.heartbeat_at
        )
      end
    end

    # Returns comprehensive health report suitable for dashboards and alerts
    #
    # @return [Hash] Health report with totals and problem partitions
    def health_report
      health = partition_health
      threshold = lag_alert_threshold

      {
        total: health.size,
        active: health.count { |p| p[:status] == :active },
        stale: health.count { |p| p[:status] == :stale },
        orphaned: health.select { |p| p[:status] == :orphaned },
        high_lag: health.select { |p| p[:lag] > threshold },
        timestamp: Time.current.iso8601
      }
    end

    # Returns ALL unclaimed partitions, including stale records from removed consumer groups.
    # Use this for cleanup operations to identify records that should be deleted.
    #
    # Unlike `orphaned_partitions`, this method does NOT filter by configuration.
    # Use with caution - these records may be from intentionally removed consumer groups.
    #
    # @return [Array<Hash>] List of all unclaimed partitions with metadata
    def all_unclaimed_partitions
      ConsumerOffset.orphaned.map do |offset|
        {
          consumer_group: base_consumer_group(offset.consumer_group),
          full_consumer_group: offset.consumer_group,
          topic: offset.topic,
          partition_key: extract_partition_key(offset.consumer_group),
          claimed_until: offset.claimed_until,
          last_consumed_at: offset.last_consumed_at,
          in_config: in_expected_partitions?(offset)
        }
      end
    end

    # Returns consumer groups that exist in DB but not in configuration.
    # These are candidates for cleanup after removing consumer groups from config.
    #
    # @return [Array<String>] List of stale consumer group base names
    def stale_consumer_groups
      expected_groups = expected_partitions.map { |p| p[:consumer_group] }.uniq.to_set

      ConsumerOffset.pluck(:consumer_group)
                    .map { |cg| base_consumer_group(cg) }
                    .uniq
                    .reject { |cg| expected_groups.include?(cg) }
    end

    # Returns lag for a specific partition
    #
    # @param consumer_group [String] Base consumer group name (without _pN suffix)
    # @param topic [String] Topic name
    # @param partition_key [Integer] Partition number
    # @return [Integer] Number of unprocessed events
    def partition_lag(consumer_group:, topic:, partition_key:)
      offset = ConsumerOffset.find_by(
        topic: topic,
        consumer_group: "#{consumer_group}_p#{partition_key}"
      )
      return 0 unless offset

      calculate_lag(offset)
    end

    private

    def expected_partitions
      return [] unless @configuration&.workers

      @configuration.workers.flat_map do |worker_config|
        worker_config.partitions.map do |partition_key|
          {
            consumer_group: worker_config.consumer_group,
            topic: worker_config.topic,
            partition_key: partition_key
          }
        end
      end
    end

    def find_offset(partition)
      ConsumerOffset.find_by(
        topic: partition[:topic],
        consumer_group: "#{partition[:consumer_group]}_p#{partition[:partition_key]}"
      )
    end

    def determine_status(offset)
      return :orphaned unless offset

      if offset.claimed?
        # Check if heartbeat is recent (within 60 seconds)
        if offset.heartbeat_at && offset.heartbeat_at > stale_timeout.ago
          :active
        else
          :stale
        end
      else
        :orphaned
      end
    end

    def calculate_lag(offset)
      return 0 unless offset

      # Count actual events after the consumer's offset for this topic and partition.
      # Previously used max_sequence - last_consumed_sequence, but sequence numbers
      # are global across all topics, so the gap wildly overstates real backlog.
      OutboxEvent
        .where(topic: offset.topic, partition_key: extract_partition_key(offset.consumer_group))
        .where('sequence > ?', offset.last_consumed_sequence || 0)
        .count
    end

    def extract_partition_key(consumer_group)
      match = consumer_group.to_s.match(/_p(\d+)$/)
      match ? match[1].to_i : 0
    end

    def base_consumer_group(consumer_group)
      consumer_group.to_s.sub(/_p\d+$/, '')
    end

    def lag_alert_threshold
      @configuration&.lag_alert_threshold || Configuration::DEFAULT_LAG_ALERT_THRESHOLD
    end

    def stale_timeout
      (@configuration&.stale_worker_timeout || Configuration::DEFAULT_STALE_WORKER_TIMEOUT).seconds
    end

    def in_expected_partitions?(offset)
      @expected_consumer_groups_set ||= expected_partitions.map do |p|
        "#{p[:consumer_group]}_p#{p[:partition_key]}"
      end.to_set

      @expected_consumer_groups_set.include?(offset.consumer_group)
    end
  end
end
