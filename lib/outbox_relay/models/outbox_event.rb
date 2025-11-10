# frozen_string_literal: true

module OutboxRelay
  class OutboxEvent < ApplicationRecord
  # ============================================================================
  # IMMUTABLE EVENT LOG
  # ============================================================================
  # OutboxEvents are immutable facts - they don't have states or lifecycles.
  # Think of this as a Kafka log: events exist and are ordered by sequence.
  #
  # Consumer groups track their own progress via:
  # - ConsumerOffset: Last successfully processed sequence per consumer group
  # - DeadLetterEvent: Per-consumer-group failure tracking
  #
  # Advisory locks prevent duplicate processing within the same consumer group.
  # ============================================================================

  # Make critical fields immutable after creation to prevent accidental modification
  attr_readonly :sequence, :topic, :event_id, :event_name, :partition_key

  # Callbacks
  before_validation :set_sequence, on: :create
  before_validation :set_default_partition_key, on: :create

  # Validations
  validates :topic, presence: true
  validates :sequence, presence: true, uniqueness: true
  validates :partition_key, presence: true, numericality: { only_integer: true, greater_than_or_equal_to: 0 }
  validate :payload_must_be_hash_or_array

  # Scopes
  scope :for_topic, ->(topic) { where(topic: topic) }
  scope :for_event_name, ->(event_name) { where(event_name: event_name) }
  scope :not_expired, -> { where("expires_at IS NULL OR expires_at > ?", Time.current) }
  scope :by_sequence, -> { order(:sequence) }

  # Associations
  # One event can have multiple DLQ entries (one per consumer group that failed)
  has_many :dead_letter_events, dependent: :destroy

  # Class methods
  def self.next_sequence
    result = connection.execute("SELECT nextval('outbox_relay_outbox_events_sequence')").first

    if result.nil?
      raise OutboxRelay::Error, "Failed to generate sequence: query returned nil. " \
                                "Ensure sequence 'outbox_relay_outbox_events_sequence' exists in database."
    end

    sequence = result["nextval"]

    if sequence.nil?
      raise OutboxRelay::Error, "Failed to generate sequence: nextval column not found in result. " \
                                "Result keys: #{result.keys.join(', ')}"
    end

    sequence.to_i
  rescue ActiveRecord::StatementInvalid => e
    # Sequence doesn't exist or database error
    raise OutboxRelay::Error, "Failed to generate sequence (database error): #{e.message}. " \
                              "Run migrations: rails outbox_relay:install:migrations && rails db:migrate"
  rescue => e
    raise OutboxRelay::Error, "Failed to generate sequence: #{e.message}"
  end

  # Instance methods
  def expired?
    expires_at.present? && expires_at < Time.current
  end

  private

  def set_sequence
    return if sequence.present? # Already set

    begin
      self.sequence = self.class.next_sequence
    rescue => e
      # Log sequence generation failure
      OutboxRelay.logger.error(
        event_name: "sequence_generation_failed",
        topic: topic,
        event_name_attr: event_name,
        error: e.message,
        error_class: e.class.name,
        backtrace: e.backtrace&.first(10)&.join("\n")
      )

      Sentry.capture_exception(e, extra: {
        topic: topic,
        event_name: event_name,
        severity: "critical"
      }) if defined?(Sentry)

      # Re-raise - record cannot be saved without sequence
      raise
    end
  end

  def set_default_partition_key
    self.partition_key ||= 0
  end

  def payload_must_be_hash_or_array
    return if payload.nil?
    return if payload.is_a?(Hash) || payload.is_a?(Array)

    errors.add(:payload, "must be a Hash or Array, got #{payload.class.name}. " \
      "Do not pass JSON strings - pass Ruby Hash/Array instead. " \
      "If you have a JSON string, parse it first: JSON.parse(json_string)")
  end
  end
end
