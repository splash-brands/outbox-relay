# frozen_string_literal: true

module OutboxRelay
  class ConsumerOffset < ApplicationRecord
  # Constants
  ACTIVE_TIMEOUT = 5.minutes
  CLAIM_TTL = 30.seconds

  # Validations
  validates :consumer_group, presence: true
  validates :topic, presence: true
  validates :last_consumed_sequence, presence: true, numericality: { only_integer: true, greater_than_or_equal_to: 0 }
  # Note: sequence_must_not_decrease validation removed - handled by conditional update in update_offset!

  # Scopes
  scope :for_group, ->(group) { where(consumer_group: group) }
  scope :for_topic, ->(topic) { where(topic: topic) }
  scope :active, -> { where("heartbeat_at > ?", ACTIVE_TIMEOUT.ago) }
  scope :stale, -> { where("heartbeat_at IS NULL OR heartbeat_at <= ?", ACTIVE_TIMEOUT.ago) }
  # Partition claiming scopes
  scope :claimed, -> { where.not(claimed_by: nil).where("claimed_until > ?", Time.current) }
  scope :unclaimed, -> { where(claimed_by: nil).or(where("claimed_until IS NULL OR claimed_until <= ?", Time.current)) }

  # Class methods
  def self.find_or_initialize_for(consumer_group:, topic:)
    find_or_initialize_by(
      consumer_group: consumer_group,
      topic: topic,
    ) do |offset|
      offset.last_consumed_sequence = 0
    end
  end

  # Instance methods
  def update_offset!(sequence:, event_id:)
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
    with_lock do
      reload # Ensure we have latest offset value

      # Check if this is a stale offset (event processed out-of-order)
      if sequence <= last_consumed_sequence
        # This is expected in concurrent processing - not an error!
        # Worker completed processing but another worker already advanced offset
        return false
      end

      # Offset is fresh - update it
      update!(
        last_consumed_sequence: sequence,
        last_consumed_event_id: event_id,
        last_consumed_at: Time.current,
        heartbeat_at: Time.current,
      )

      true
    end
  end

  def heartbeat!
    update_column(:heartbeat_at, Time.current)
  end

  def lag
    # IMPORTANT: Calculates GLOBAL lag across all partitions for this topic
    # This may not accurately reflect lag for partition-specific consumers
    #
    # For accurate partition-specific lag, use OutboxConsumer#lag instead
    # (lib/outbox_relay/models/outbox_consumer.rb:137-141), which filters by partition_key
    #
    # Example:
    #   consumer = MyConsumer.new(partition_key: 0)
    #   partition_lag = consumer.lag  # Lag for partition 0 only
    #
    # This method is primarily for monitoring overall topic health
    latest_sequence = OutboxRelay::OutboxEvent.where(topic: topic).maximum(:sequence) || 0
    latest_sequence - last_consumed_sequence
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
  def try_claim!(consumer_instance_id:, ttl: CLAIM_TTL)
    with_lock do
      reload

      # Check if already claimed by another worker (claim still active)
      if claimed? && claimed_by != consumer_instance_id
        return false
      end

      # Acquire or renew claim
      update!(
        claimed_by: consumer_instance_id,
        claimed_until: Time.current + ttl
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
  def renew_claim!(consumer_instance_id:, ttl: CLAIM_TTL)
    with_lock do
      reload

      # Only renew if we still hold the claim
      return false unless claimed_by == consumer_instance_id

      update!(claimed_until: Time.current + ttl)
      true
    end
  end

  # Release claim explicitly (graceful shutdown).
  #
  # @param consumer_instance_id [String] Unique ID of the releasing worker
  # @return [Boolean] true if release successful, false if we don't hold the claim
  def release_claim!(consumer_instance_id:)
    with_lock do
      reload

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
