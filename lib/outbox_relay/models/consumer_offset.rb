# frozen_string_literal: true

module OutboxRelay
  class ConsumerOffset < ApplicationRecord
    # Constants
    ACTIVE_TIMEOUT = 5.minutes
    VALID_AUTO_OFFSET_RESET_VALUES = %i[latest earliest].freeze
    CLAIM_TTL = 30.seconds

    # Validations
    validates :consumer_group, presence: true
    validates :topic, presence: true
    validates :last_consumed_sequence, presence: true, numericality: { only_integer: true, greater_than_or_equal_to: 0 }
    # NOTE: sequence_must_not_decrease validation removed - handled by conditional update in update_offset!

    # Scopes
    scope :for_group, ->(group) { where(consumer_group: group) }
    scope :for_topic, ->(topic) { where(topic: topic) }
    scope :active, -> { where('heartbeat_at > ?', ACTIVE_TIMEOUT.ago) }
    scope :stale, -> { where('heartbeat_at IS NULL OR heartbeat_at <= ?', ACTIVE_TIMEOUT.ago) }
    # Partition claiming scopes
    scope :claimed, -> { where.not(claimed_by: nil).where('claimed_until > ?', Time.current) }
    scope :unclaimed, lambda {
      where(claimed_by: nil).or(where('claimed_until IS NULL OR claimed_until <= ?', Time.current))
    }
    # Aliases for monitoring
    scope :orphaned, -> { unclaimed }
    scope :actively_claimed, -> { claimed }

    # Class methods
    #
    # Find or initialize a consumer offset record for the given consumer group and topic.
    #
    # @param consumer_group [String] Consumer group name
    # @param topic [String] Topic name
    # @param auto_offset_reset [Symbol] Where to start consuming for NEW consumer groups:
    #   - :latest (default) - Start from the latest event (skip historical events)
    #   - :earliest - Start from the beginning (process all historical events)
    #
    # @return [ConsumerOffset] Found or initialized offset record
    #
    # @example New consumer with default :latest (safe for production deploys)
    #   ConsumerOffset.find_or_initialize_for(
    #     consumer_group: "my_new_consumer",
    #     topic: "orders"
    #   )
    #   # => Starts from current max sequence, skips historical events
    #
    # @example New consumer with :earliest (reprocess all events)
    #   ConsumerOffset.find_or_initialize_for(
    #     consumer_group: "backfill_consumer",
    #     topic: "orders",
    #     auto_offset_reset: :earliest
    #   )
    #   # => Starts from sequence 0, processes all historical events
    #
    def self.find_or_initialize_for(consumer_group:, topic:, auto_offset_reset: :latest)
      unless VALID_AUTO_OFFSET_RESET_VALUES.include?(auto_offset_reset)
        raise ArgumentError,
              "auto_offset_reset must be :latest or :earliest, got: #{auto_offset_reset.inspect}"
      end

      find_or_initialize_by(
        consumer_group: consumer_group,
        topic: topic
      ) do |offset|
        offset.last_consumed_sequence = case auto_offset_reset
                                        when :earliest
                                          0
                                        else # :latest (default)
                                          # Seed from commit_seq (the consumer cursor), not sequence. See SB-2140.
                                          OutboxRelay::OutboxEvent.where(topic: topic).maximum(:commit_seq) || 0
                                        end
      end
    end

    # Instance methods
    #
    # NOTE (SB-2140): the cursor is now the event's `commit_seq` (assigned at the
    # COMMIT edge), not `sequence` (assigned at INSERT). The stored column is still
    # named `last_consumed_sequence` to avoid an offset-row migration — historical
    # rows have commit_seq == sequence and new commit_seq values continue above the
    # global sequence high-water, so any pre-cutover offset value remains a valid
    # commit_seq threshold.
    def update_offset!(commit_seq:, event_id:)
      # Kafka-style conditional offset update to handle out-of-order completion
      # in concurrent worker environments.
      #
      # ## Problem: Race Condition in Concurrent Processing
      #
      # When multiple workers process events in parallel:
      #   Worker A: fetches event seq=100
      #   Worker B: fetches event seq=101
      #   Worker B: completes first → offset=101 ✓
      #   Worker A: completes later → tries offset=100 → CONFLICT!
      #
      # ## Solution: Conditional Update (Kafka Pattern)
      #
      # Only update offset if new sequence is GREATER than current:
      #   UPDATE consumer_offsets
      #   SET last_consumed_sequence = 100
      #   WHERE id = X AND last_consumed_sequence < 100
      #
      # This prevents:
      #   1. ValidationError when workers complete out-of-order
      #   2. False-positive DLQ entries for successfully processed events
      #   3. Offset going backwards (data integrity)
      #
      # ## Safety Guarantees (At-Least-Once Delivery)
      #
      # If Worker A crashes BEFORE commit:
      #   - Transaction rolls back
      #   - Advisory lock released
      #   - Offset NOT updated
      #   - Event seq=100 will be re-processed ✓
      #
      # If Worker A commits AFTER Worker B:
      #   - Offset stays at 101 (conditional update skips)
      #   - Event seq=100 was successfully processed
      #   - No data loss ✓
      #
      # ## Return Value
      #
      # Returns:
      #   - true if offset was updated (sequence > current)
      #   - false if offset was stale (sequence <= current)
      #   - Raises on other errors (DB connectivity, constraint violations)
      #
      # Note: Uses explicit reload + lock! pattern instead of with_lock to be compatible
      # with Rails 7.1+ which raises error on with_lock if record has pending changes.
      #
      ActiveRecord::Base.transaction do
        reload # Clear any in-memory changes and ensure we have latest offset value
        lock!

        # Check if this is a stale offset (event processed out-of-order)
        if commit_seq <= last_consumed_sequence
          # This is expected in concurrent processing - not an error!
          # Worker completed processing but another worker already advanced offset
          return false
        end

        # Offset is fresh - update it
        update!(
          last_consumed_sequence: commit_seq,
          last_consumed_event_id: event_id,
          last_consumed_at: Time.current,
          heartbeat_at: Time.current
        )

        true
      end
    end

    def heartbeat!
      update_column(:heartbeat_at, Time.current)
    end

    def lag
      # Count actual events after the consumer's offset across ALL partitions for this topic.
      # Previously used max_sequence - last_consumed_sequence, but sequence numbers
      # are global across all topics, so the gap overstates real backlog.
      #
      # For partition-specific lag, use OutboxConsumer#lag instead.
      OutboxRelay::OutboxEvent
        .where(topic: topic)
        .where('commit_seq > ?', last_consumed_sequence)
        .count
    end

    def active?
      heartbeat_at.present? && heartbeat_at > ACTIVE_TIMEOUT.ago
    end

    # ============================================================================
    # Partition Claiming Methods
    # ============================================================================
    # These methods implement database-backed partition claiming to ensure only
    # one worker processes each partition at a time across multiple ECS instances.
    #
    # Flow:
    #   1. Worker boots → try_claim! → acquires exclusive claim
    #   2. Heartbeat → renew_claim! → extends TTL
    #   3. Shutdown → release_claim! → frees partition
    #   4. Crash → claim expires after TTL → another worker can claim

    # Try to claim this partition for exclusive processing.
    #
    # @param consumer_instance_id [String] Unique ID of the claiming worker
    # @param ttl [ActiveSupport::Duration] How long the claim is valid (default: 30 seconds)
    # @return [Boolean] true if claim acquired, false if already claimed by another worker
    #
    # Note: Uses explicit reload + lock! pattern instead of with_lock to be compatible
    # with Rails 7.1+ which raises error on with_lock if record has pending changes.
    def try_claim!(consumer_instance_id:, ttl: CLAIM_TTL)
      ActiveRecord::Base.transaction do
        reload # Clear any in-memory changes before locking
        lock!

        # Check if already claimed by another worker (claim still active)
        return false if claimed? && claimed_by != consumer_instance_id

        # Acquire or renew claim
        # Also set heartbeat_at to prove worker liveness from the start
        update!(
          claimed_by: consumer_instance_id,
          claimed_until: Time.current + ttl,
          heartbeat_at: Time.current
        )

        true
      end
    end

    # Renew an existing claim (extend TTL).
    # Called during heartbeat to prevent claim expiration.
    #
    # @param consumer_instance_id [String] Unique ID of the claiming worker
    # @param ttl [ActiveSupport::Duration] New TTL from now
    # @return [Boolean] true if renewal successful, false if claim lost to another worker
    #
    # Note: Uses explicit reload + lock! pattern instead of with_lock to be compatible
    # with Rails 7.1+ which raises error on with_lock if record has pending changes.
    #
    # IMPORTANT: Also updates heartbeat_at to prove worker liveness. This is critical
    # for PartitionMonitor to correctly identify active vs stale workers, especially
    # when a partition has no new events to process (heartbeat_at would otherwise
    # only update in update_offset! when processing events).
    def renew_claim!(consumer_instance_id:, ttl: CLAIM_TTL)
      ActiveRecord::Base.transaction do
        reload # Clear any in-memory changes before locking
        lock!

        # Only renew if we still hold the claim
        return false unless claimed_by == consumer_instance_id

        update!(
          claimed_until: Time.current + ttl,
          heartbeat_at: Time.current
        )
        true
      end
    end

    # Release claim explicitly (graceful shutdown).
    #
    # @param consumer_instance_id [String] Unique ID of the releasing worker
    # @return [Boolean] true if release successful, false if we don't hold the claim
    #
    # Note: Uses explicit reload + lock! pattern instead of with_lock to be compatible
    # with Rails 7.1+ which raises error on with_lock if record has pending changes.
    def release_claim!(consumer_instance_id:)
      ActiveRecord::Base.transaction do
        reload # Clear any in-memory changes before locking
        lock!

        # Only release if we hold the claim
        return false unless claimed_by == consumer_instance_id

        update!(claimed_by: nil, claimed_until: nil)
        true
      end
    end

    # Check if partition is currently claimed by an active worker.
    # A claim is active if claimed_by is set AND claimed_until is in the future.
    #
    # @return [Boolean] true if actively claimed, false if unclaimed or expired
    def claimed?
      claimed_by.present? && claimed_until.present? && claimed_until > Time.current
    end
  end
end
