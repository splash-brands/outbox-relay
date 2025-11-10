# frozen_string_literal: true

module OutboxRelay
  class DeadLetterEvent < ApplicationRecord
  # Resolution statuses
  RESOLUTION_STATUSES = ["retrying", "unresolved", "resolved", "reprocessed", "ignored"].freeze

  # Validations
  validates :consumer_group, presence: true
  validates :total_retries, presence: true, numericality: { only_integer: true, greater_than: 0 }
  validates :error_message, presence: true
  validates :resolution_status, inclusion: { in: RESOLUTION_STATUSES }

  # Associations
  belongs_to :outbox_event,
             class_name: "OutboxRelay::OutboxEvent",
             foreign_key: "outbox_relay_outbox_event_id",
             optional: true

  # Scopes
  scope :unresolved, -> { where(resolution_status: "unresolved") }
  scope :resolved, -> { where(resolution_status: "resolved") }
  scope :reprocessed, -> { where(resolution_status: "reprocessed") }
  scope :ignored, -> { where(resolution_status: "ignored") }
  scope :for_group, ->(group) { where(consumer_group: group) }
  scope :for_topic, ->(topic) { where(original_topic: topic) }
  scope :recent, -> { where("created_at > ?", 7.days.ago) }
  scope :by_error_type, -> { group(:error_message).count }

  # Instance methods
  def mark_as_resolved!(notes: nil)
    update!(
      resolution_status: "resolved",
      resolution_notes: notes,
      resolved_at: Time.current,
    )
  end

  def mark_as_reprocessed!(notes: nil)
    update!(
      resolution_status: "reprocessed",
      resolution_notes: notes,
      resolved_at: Time.current,
    )
  end

  def mark_as_ignored!(notes: nil)
    update!(
      resolution_status: "ignored",
      resolution_notes: notes,
      resolved_at: Time.current,
    )
  end

  def error_summary(max_length: 100)
    return if error_message.blank?

    # Get first line of error message
    first_line = error_message.lines.first&.strip || ""

    # Truncate if needed
    if first_line.length > max_length
      first_line[0...max_length - 3] + "..."
    else
      first_line
    end
  end
  end
end
