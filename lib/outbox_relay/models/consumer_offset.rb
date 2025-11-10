# frozen_string_literal: true

module OutboxRelay
  class ConsumerOffset < ApplicationRecord
  # Constants
  ACTIVE_TIMEOUT = 5.minutes

  # Validations
  validates :consumer_group, presence: true
  validates :topic, presence: true
  validates :last_consumed_sequence, presence: true, numericality: { only_integer: true, greater_than_or_equal_to: 0 }
  validate :sequence_must_not_decrease, on: :update, if: :last_consumed_sequence_changed?

  # Scopes
  scope :for_group, ->(group) { where(consumer_group: group) }
  scope :for_topic, ->(topic) { where(topic: topic) }
  scope :active, -> { where("heartbeat_at > ?", ACTIVE_TIMEOUT.ago) }
  scope :stale, -> { where("heartbeat_at IS NULL OR heartbeat_at <= ?", ACTIVE_TIMEOUT.ago) }

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
    with_lock do
      update!(
        last_consumed_sequence: sequence,
        last_consumed_event_id: event_id,
        last_consumed_at: Time.current,
        heartbeat_at: Time.current,
      )
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

  private

  def sequence_must_not_decrease
    # Prevents sequence from going backwards, but ALLOWS same sequence to be
    # written multiple times (e.g., after worker restart with in-progress event).
    #
    # Database uniqueness constraint on (consumer_group, topic) prevents actual
    # duplicate offset records. This validation only catches application bugs
    # where sequence counter decrements.
    return unless last_consumed_sequence_changed?
    return unless last_consumed_sequence_was.present?

    if last_consumed_sequence < last_consumed_sequence_was
      errors.add(
        :last_consumed_sequence,
        "cannot decrease (was #{last_consumed_sequence_was}, got #{last_consumed_sequence})",
      )
    end
  end
  end
end
