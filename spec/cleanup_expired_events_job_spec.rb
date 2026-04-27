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

  def create_dlq(status:, resolved_at:, created_at: nil, consumer_group: 'g', topic: 'orders')
    OutboxRelay::DeadLetterEvent.create!(
      consumer_group: consumer_group,
      original_topic: topic,
      original_sequence: SecureRandom.random_number(1_000_000),
      original_event_id: SecureRandom.uuid,
      original_payload: {},
      total_retries: 1,
      error_message: 'boom',
      resolution_status: status,
      resolved_at: resolved_at,
      created_at: created_at || resolved_at || Time.current,
      updated_at: created_at || resolved_at || Time.current
    )
  end

  describe '.perform (class method)' do
    it 'is a no-op when cleanup_enabled is false, leaving data untouched' do
      OutboxRelay.cleanup_enabled = false
      create_event(sequence: 1, expires_at: 1.hour.ago)
      set_consumer_offset(topic: 'orders', last_consumed_sequence: 100)

      expect { described_class.perform }.not_to(change { OutboxRelay::OutboxEvent.count })
    end

    it 'runs real cleanup end-to-end and returns the stats hash' do
      create_event(sequence: 1, expires_at: 1.hour.ago)
      set_consumer_offset(topic: 'orders', last_consumed_sequence: 100)

      result = described_class.perform

      expect(result).to include(events_deleted: 1, dlq_deleted: 0)
      expect(result[:duration]).to be_a(Numeric)
      expect(OutboxRelay::OutboxEvent.count).to eq(0)
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

  describe '#perform — DLQ FK protection' do
    # Production has FK outbox_relay_dead_letter_events.outbox_relay_outbox_event_id
    # → outbox_relay_outbox_events(id) with NO ACTION on delete. Tests run on
    # SQLite without FK enforcement (see spec/support/database.rb), so these
    # specs assert the LOGIC that prevents the FK violation, regardless of
    # whether the DB enforces it.

    it 'does NOT delete an expired+consumed event still referenced by a DLQ entry' do
      event = create_event(sequence: 1, expires_at: 1.hour.ago)
      set_consumer_offset(topic: 'orders', last_consumed_sequence: 100)
      OutboxRelay::DeadLetterEvent.create!(
        outbox_relay_outbox_event_id: event.id,
        consumer_group: 'g',
        original_topic: 'orders',
        original_sequence: 1,
        original_event_id: SecureRandom.uuid,
        original_payload: {},
        total_retries: 1,
        error_message: 'boom',
        resolution_status: 'unresolved',
        resolved_at: nil
      )

      result = described_class.new.perform

      expect(result[:events_deleted]).to eq(0)
      expect(OutboxRelay::OutboxEvent.count).to eq(1)
    end

    it 'deletes resolved DLQ first, then deletes its formerly-referenced event in the same run' do
      OutboxRelay.dlq_resolved_ttl = 14.days
      event = create_event(sequence: 1, expires_at: 1.hour.ago)
      set_consumer_offset(topic: 'orders', last_consumed_sequence: 100)
      OutboxRelay::DeadLetterEvent.create!(
        outbox_relay_outbox_event_id: event.id,
        consumer_group: 'g',
        original_topic: 'orders',
        original_sequence: 1,
        original_event_id: SecureRandom.uuid,
        original_payload: {},
        total_retries: 1,
        error_message: 'boom',
        resolution_status: 'resolved',
        resolved_at: 30.days.ago
      )

      result = described_class.new.perform

      expect(result).to include(events_deleted: 1, dlq_deleted: 1)
      expect(OutboxRelay::OutboxEvent.count).to eq(0)
      expect(OutboxRelay::DeadLetterEvent.count).to eq(0)
    end
  end

  describe '#perform — DLQ cleanup' do
    before { OutboxRelay.dlq_resolved_ttl = 14.days }

    it 'deletes terminal DLQ entries whose resolved_at is older than TTL' do
      create_dlq(status: 'resolved',    resolved_at: 20.days.ago)
      create_dlq(status: 'reprocessed', resolved_at: 20.days.ago)
      create_dlq(status: 'ignored',     resolved_at: 20.days.ago)

      result = described_class.new.perform

      expect(result[:dlq_deleted]).to eq(3)
      expect(OutboxRelay::DeadLetterEvent.count).to eq(0)
    end

    it 'measures TTL from resolved_at, not created_at' do
      # Entry created 100 days ago but resolved yesterday → must be preserved.
      create_dlq(status: 'resolved', created_at: 100.days.ago, resolved_at: 1.day.ago)

      result = described_class.new.perform

      expect(result[:dlq_deleted]).to eq(0)
      expect(OutboxRelay::DeadLetterEvent.count).to eq(1)
    end

    it 'does NOT delete resolved DLQ entries newer than TTL' do
      create_dlq(status: 'resolved', resolved_at: 1.day.ago)

      result = described_class.new.perform

      expect(result[:dlq_deleted]).to eq(0)
      expect(OutboxRelay::DeadLetterEvent.count).to eq(1)
    end

    it 'does NOT delete unresolved or retrying DLQ entries regardless of age' do
      create_dlq(status: 'unresolved', resolved_at: nil, created_at: 100.days.ago)
      create_dlq(status: 'retrying',   resolved_at: nil, created_at: 100.days.ago)

      result = described_class.new.perform

      expect(result[:dlq_deleted]).to eq(0)
      expect(OutboxRelay::DeadLetterEvent.count).to eq(2)
    end

    it 'preserves terminal entries with NULL resolved_at (never went through mark_as_* helpers)' do
      create_dlq(status: 'resolved', resolved_at: nil, created_at: 100.days.ago)

      result = described_class.new.perform

      expect(result[:dlq_deleted]).to eq(0)
      expect(OutboxRelay::DeadLetterEvent.count).to eq(1)
    end

    it 'skips DLQ cleanup entirely when dlq_resolved_ttl is nil' do
      OutboxRelay.dlq_resolved_ttl = nil
      create_dlq(status: 'resolved', resolved_at: 100.days.ago)

      result = described_class.new.perform

      expect(result[:dlq_deleted]).to eq(0)
      expect(OutboxRelay::DeadLetterEvent.count).to eq(1)
    end

    it 'respects cleanup_batch_size for DLQ deletes' do
      OutboxRelay.cleanup_batch_size = 2
      3.times { create_dlq(status: 'resolved', resolved_at: 30.days.ago) }

      result = described_class.new.perform

      expect(result[:dlq_deleted]).to eq(2)
      expect(OutboxRelay::DeadLetterEvent.count).to eq(1)
    end

    it 'uses strict < inequality on the resolved_at boundary' do
      # Exactly at ttl.ago → must be preserved (not older than TTL).
      create_dlq(status: 'resolved', resolved_at: 14.days.ago + 1.second)

      result = described_class.new.perform

      expect(result[:dlq_deleted]).to eq(0)
      expect(OutboxRelay::DeadLetterEvent.count).to eq(1)
    end

    it 'raises ConfigurationError when dlq_resolved_ttl is not a Duration' do
      OutboxRelay.dlq_resolved_ttl = 14 # forgot `.days`
      create_dlq(status: 'resolved', resolved_at: 30.days.ago)

      expect { described_class.new.perform }
        .to raise_error(OutboxRelay::ConfigurationError, /ActiveSupport::Duration/)
    end
  end

  describe '#perform — instrumentation' do
    # Subscribes to the cleanup event for the duration of the block. `captured`
    # is populated as events arrive and remains accessible after the block,
    # even if the block raises.
    def with_cleanup_subscription
      captured = []
      subscription = ActiveSupport::Notifications.subscribe('outbox_relay.cleanup.completed') do |_, _, _, _, payload|
        captured << payload
      end
      yield captured
      captured
    ensure
      ActiveSupport::Notifications.unsubscribe(subscription) if subscription
    end

    it 'emits outbox_relay.cleanup.completed with success payload' do
      OutboxRelay.dlq_resolved_ttl = 14.days
      create_event(sequence: 1, expires_at: 1.hour.ago)
      set_consumer_offset(topic: 'orders', last_consumed_sequence: 100)
      create_dlq(status: 'resolved', resolved_at: 30.days.ago)

      payloads = with_cleanup_subscription { described_class.new.perform }

      expect(payloads.size).to eq(1)
      expect(payloads.first).to include(
        events_deleted: 1,
        dlq_deleted: 1,
        error_class: nil,
        timeout: false
      )
      expect(payloads.first[:duration]).to be_a(Numeric)
    end

    it 'emits notification even on timeout, with preserved phase-1 count and timeout: true' do
      create_event(sequence: 1, expires_at: 1.hour.ago)
      set_consumer_offset(topic: 'orders', last_consumed_sequence: 100)
      OutboxRelay.dlq_resolved_ttl = 14.days
      create_dlq(status: 'resolved', resolved_at: 30.days.ago)

      fake_timeout = Class.new(StandardError) do
        def self.name
          'PG::QueryCanceled'
        end
      end
      stub_const('OutboxRelay::Jobs::CleanupExpiredEventsJob::PG_QUERY_CANCELED', fake_timeout)

      allow_any_instance_of(described_class)
        .to receive(:delete_expired_events).and_raise(fake_timeout, 'statement timeout')

      result = nil
      payloads = with_cleanup_subscription do
        expect { result = described_class.new.perform }.not_to raise_error
      end

      expect(result).to include(events_deleted: 0, dlq_deleted: 1, timeout: true)
      expect(payloads.size).to eq(1)
      expect(payloads.first).to include(
        events_deleted: 0,
        dlq_deleted: 1,
        timeout: true,
        error_class: 'PG::QueryCanceled'
      )
    end

    it 'emits notification and re-raises on unexpected errors' do
      create_event(sequence: 1, expires_at: 1.hour.ago)
      set_consumer_offset(topic: 'orders', last_consumed_sequence: 100)

      allow_any_instance_of(described_class)
        .to receive(:delete_expired_events).and_raise(StandardError, 'kaboom')
      allow(OutboxRelay::Instrumentation::Models).to receive(:error)

      payloads = with_cleanup_subscription do
        expect { described_class.new.perform }.to raise_error(StandardError, 'kaboom')
      end

      expect(payloads.size).to eq(1)
      expect(payloads.first).to include(
        events_deleted: 0,
        dlq_deleted: 0,
        timeout: false,
        error_class: 'StandardError'
      )
    end

    it 'does not let a buggy notification subscriber mask the cleanup outcome' do
      create_event(sequence: 1, expires_at: 1.hour.ago)
      set_consumer_offset(topic: 'orders', last_consumed_sequence: 100)

      subscription = ActiveSupport::Notifications.subscribe('outbox_relay.cleanup.completed') do
        raise 'subscriber blew up'
      end

      expect { described_class.new.perform }.not_to raise_error
      expect(OutboxRelay::OutboxEvent.count).to eq(0)
    ensure
      ActiveSupport::Notifications.unsubscribe(subscription) if subscription
    end
  end
end
