# frozen_string_literal: true

require 'spec_helper'
require 'outbox_relay/jobs/cleanup_expired_events_job'

RSpec.describe OutboxRelay::Jobs::CleanupExpiredEventsJob do
  around do |example|
    original_enabled = OutboxRelay.cleanup_enabled
    original_batch = OutboxRelay.cleanup_batch_size
    original_dlq_ttl = OutboxRelay.dlq_resolved_ttl
    OutboxRelay.cleanup_enabled = true
    OutboxRelay.cleanup_batch_size = 10_000
    OutboxRelay.dlq_resolved_ttl = nil
    example.run
  ensure
    OutboxRelay.cleanup_enabled = original_enabled
    OutboxRelay.cleanup_batch_size = original_batch
    OutboxRelay.dlq_resolved_ttl = original_dlq_ttl
  end

  def create_event(sequence:, topic: 'orders', expires_at: nil)
    OutboxRelay::OutboxEvent.create!(
      sequence: sequence,
      topic: topic,
      event_name: 'created',
      event_id: SecureRandom.uuid,
      partition_key: 0,
      payload: { 'id' => sequence },
      headers: {},
      expires_at: expires_at
    )
  end

  def set_consumer_offset(topic:, last_consumed_sequence:, consumer_group: 'group_a')
    OutboxRelay::ConsumerOffset.create!(
      consumer_group: consumer_group,
      topic: topic,
      last_consumed_sequence: last_consumed_sequence
    )
  end

  def create_dlq(status:, created_at:, consumer_group: 'g', topic: 'orders')
    OutboxRelay::DeadLetterEvent.create!(
      consumer_group: consumer_group,
      original_topic: topic,
      original_sequence: SecureRandom.random_number(1_000_000),
      original_event_id: SecureRandom.uuid,
      original_payload: {},
      total_retries: 1,
      error_message: 'boom',
      resolution_status: status,
      created_at: created_at,
      updated_at: created_at
    )
  end

  describe '.perform (class method)' do
    it 'is a no-op when cleanup_enabled is false' do
      OutboxRelay.cleanup_enabled = false
      expect_any_instance_of(described_class).not_to receive(:perform)

      described_class.perform
    end

    it 'runs the job when cleanup_enabled is true' do
      expect_any_instance_of(described_class).to receive(:perform).and_return({ events_deleted: 0, dlq_deleted: 0,
                                                                                duration: 0 })

      described_class.perform
    end
  end

  describe '#perform — event cleanup' do
    it 'deletes events that are expired AND consumed by all groups' do
      create_event(sequence: 1, expires_at: 1.hour.ago)
      set_consumer_offset(topic: 'orders', last_consumed_sequence: 5)

      result = described_class.new.perform

      expect(result[:events_deleted]).to eq(1)
      expect(OutboxRelay::OutboxEvent.count).to eq(0)
    end

    it 'does NOT delete events that are expired but NOT yet consumed by all groups' do
      create_event(sequence: 10, expires_at: 1.hour.ago)
      set_consumer_offset(topic: 'orders', last_consumed_sequence: 5) # offset behind sequence

      result = described_class.new.perform

      expect(result[:events_deleted]).to eq(0)
      expect(OutboxRelay::OutboxEvent.count).to eq(1)
    end

    it 'does NOT delete events with expires_at = nil' do
      create_event(sequence: 1, expires_at: nil)
      set_consumer_offset(topic: 'orders', last_consumed_sequence: 5)

      result = described_class.new.perform

      expect(result[:events_deleted]).to eq(0)
      expect(OutboxRelay::OutboxEvent.count).to eq(1)
    end

    it 'does NOT delete events that have not yet expired' do
      create_event(sequence: 1, expires_at: 1.hour.from_now)
      set_consumer_offset(topic: 'orders', last_consumed_sequence: 5)

      result = described_class.new.perform

      expect(result[:events_deleted]).to eq(0)
      expect(OutboxRelay::OutboxEvent.count).to eq(1)
    end

    it 'respects MIN(offset) across multiple consumer groups' do
      create_event(sequence: 3, expires_at: 1.hour.ago)
      # group_a fully caught up, group_b lagging — event must NOT be deleted
      set_consumer_offset(topic: 'orders', last_consumed_sequence: 10, consumer_group: 'group_a')
      set_consumer_offset(topic: 'orders', last_consumed_sequence: 2,  consumer_group: 'group_b')

      result = described_class.new.perform

      expect(result[:events_deleted]).to eq(0)
      expect(OutboxRelay::OutboxEvent.count).to eq(1)
    end

    it 'respects cleanup_batch_size' do
      OutboxRelay.cleanup_batch_size = 2
      3.times { |i| create_event(sequence: i + 1, expires_at: 1.hour.ago) }
      set_consumer_offset(topic: 'orders', last_consumed_sequence: 100)

      result = described_class.new.perform

      expect(result[:events_deleted]).to eq(2)
      expect(OutboxRelay::OutboxEvent.count).to eq(1)
    end
  end

  describe '#perform — DLQ cleanup' do
    before { OutboxRelay.dlq_resolved_ttl = 14.days }

    it 'deletes old resolved DLQ entries' do
      create_dlq(status: 'resolved', created_at: 20.days.ago)
      create_dlq(status: 'reprocessed', created_at: 20.days.ago)
      create_dlq(status: 'ignored', created_at: 20.days.ago)

      result = described_class.new.perform

      expect(result[:dlq_deleted]).to eq(3)
      expect(OutboxRelay::DeadLetterEvent.count).to eq(0)
    end

    it 'does NOT delete resolved DLQ entries newer than TTL' do
      create_dlq(status: 'resolved', created_at: 1.day.ago)

      result = described_class.new.perform

      expect(result[:dlq_deleted]).to eq(0)
      expect(OutboxRelay::DeadLetterEvent.count).to eq(1)
    end

    it 'does NOT delete unresolved or retrying DLQ entries regardless of age' do
      create_dlq(status: 'unresolved', created_at: 100.days.ago)
      create_dlq(status: 'retrying', created_at: 100.days.ago)

      result = described_class.new.perform

      expect(result[:dlq_deleted]).to eq(0)
      expect(OutboxRelay::DeadLetterEvent.count).to eq(2)
    end

    it 'skips DLQ cleanup entirely when dlq_resolved_ttl is nil' do
      OutboxRelay.dlq_resolved_ttl = nil
      create_dlq(status: 'resolved', created_at: 100.days.ago)

      result = described_class.new.perform

      expect(result[:dlq_deleted]).to eq(0)
      expect(OutboxRelay::DeadLetterEvent.count).to eq(1)
    end
  end

  describe '#perform — instrumentation' do
    it 'emits cleanup_completed.outbox_relay with stats' do
      OutboxRelay.dlq_resolved_ttl = 14.days
      create_event(sequence: 1, expires_at: 1.hour.ago)
      set_consumer_offset(topic: 'orders', last_consumed_sequence: 100)
      create_dlq(status: 'resolved', created_at: 30.days.ago)

      captured = nil
      subscription = ActiveSupport::Notifications.subscribe('cleanup_completed.outbox_relay') do |_, _, _, _, payload|
        captured = payload
      end

      described_class.new.perform

      expect(captured).to include(events_deleted: 1, dlq_deleted: 1)
      expect(captured[:duration]).to be_a(Numeric)
    ensure
      ActiveSupport::Notifications.unsubscribe(subscription) if subscription
    end
  end
end
