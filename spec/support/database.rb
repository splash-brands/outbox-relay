# frozen_string_literal: true

require "active_record"

# Configure test database connection (in-memory SQLite for speed)
ActiveRecord::Base.establish_connection(
  adapter: "sqlite3",
  database: ":memory:"
)

# Silence ActiveRecord logs in tests
ActiveRecord::Base.logger = Logger.new(nil) if ENV["VERBOSE_TESTS"] != "true"

ActiveRecord::Schema.define do
  create_table :outbox_relay_outbox_events, force: true do |t|
    t.integer :sequence, null: false
    t.string :topic, null: false
    t.string :event_id, null: false
    t.string :event_name
    t.text :payload, null: false
    t.text :headers
    t.integer :partition_key, null: false, default: 0
    t.datetime :expires_at
    t.timestamps

    t.index :sequence, unique: true
    t.index :topic
    t.index :event_name
    t.index :partition_key
    t.index [:topic, :partition_key]
    t.index :created_at
  end

  create_table :outbox_relay_consumer_offsets, force: true do |t|
    t.string :consumer_group, null: false
    t.string :topic, null: false
    t.integer :last_consumed_sequence, null: false, default: 0
    t.string :last_consumed_event_id
    t.string :consumer_instance_id
    t.datetime :last_consumed_at
    t.datetime :heartbeat_at
    # Partition claiming columns
    t.string :claimed_by
    t.datetime :claimed_until
    t.timestamps

    # Unique constraint: one offset per (consumer_group, topic)
    # Note: partition is encoded in consumer_group name (e.g., "notifications_p0")
    t.index [:consumer_group, :topic], unique: true, name: "index_consumer_offsets_unique"
    t.index :heartbeat_at
    t.index [:claimed_by, :claimed_until], name: "idx_consumer_offset_claim"
  end

  create_table :outbox_relay_dead_letter_events, force: true do |t|
    t.string :consumer_group, null: false
    t.string :original_topic, null: false
    t.integer :original_sequence, null: false
    t.string :original_event_id, null: false
    t.string :original_event_name
    t.text :original_payload, null: false
    t.text :original_headers
    t.integer :original_partition_key, null: false, default: 0
    t.text :error_message
    t.text :error_backtrace
    t.integer :total_retries, null: false, default: 0
    t.string :resolution_status, null: false, default: "retrying"
    t.datetime :last_retry_at
    t.timestamps

    t.index [:consumer_group, :original_sequence], unique: true, name: "index_dead_letter_events_unique"
    t.index :resolution_status
    t.index :created_at
  end

  create_table :outbox_relay_processes, force: true do |t|
    t.string :kind, null: false
    t.string :name, null: false
    t.integer :pid, null: false
    t.string :hostname, null: false
    t.integer :supervisor_id
    t.datetime :last_heartbeat_at, null: false
    t.text :metadata
    t.timestamps

    t.index :kind
    t.index :pid
    t.index :supervisor_id
    t.index :last_heartbeat_at
  end
end

# Configure model table names and SQLite-specific behavior
module OutboxRelay
  class OutboxEvent < ApplicationRecord
    self.table_name = "outbox_relay_outbox_events"

    class << self
      # Mock next_sequence for tests (SQLite doesn't have sequences)
      def next_sequence
        (maximum(:sequence) || 0) + 1
      end
    end

    # Serialize JSON columns for SQLite
    serialize :payload, coder: JSON
    serialize :headers, coder: JSON

    # Override event_id default for SQLite (no gen_random_uuid())
    before_validation :set_event_id, on: :create

    private

    def set_event_id
      self.event_id ||= SecureRandom.uuid
    end
  end

  class ConsumerOffset < ApplicationRecord
    self.table_name = "outbox_relay_consumer_offsets"
  end

  class DeadLetterEvent < ApplicationRecord
    self.table_name = "outbox_relay_dead_letter_events"

    serialize :original_payload, coder: JSON
    serialize :original_headers, coder: JSON
  end
end

# Configure Process model for SQLite tests
# Note: Process already exists and inherits from ActiveRecord::Base
# We just need to add JSON serialization for SQLite
OutboxRelay::Process.class_eval do
  # Serialize metadata as JSON for SQLite (PostgreSQL uses jsonb natively)
  serialize :metadata, coder: JSON
end

# Clean database before each test
RSpec.configure do |config|
  config.before(:each) do
    OutboxRelay::OutboxEvent.delete_all
    OutboxRelay::ConsumerOffset.delete_all
    OutboxRelay::DeadLetterEvent.delete_all
    OutboxRelay::Process.delete_all
  end
end
