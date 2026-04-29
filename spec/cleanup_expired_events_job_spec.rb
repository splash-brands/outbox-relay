# frozen_string_literal: true

require 'spec_helper'
require 'outbox_relay/jobs/cleanup_expired_events_job'

RSpec.describe OutboxRelay::Jobs::CleanupExpiredEventsJob do
  around do |example|
    original_enabled = OutboxRelay.cleanup_enabled
    original_batch = OutboxRelay.cleanup_batch_size
    original_dlq_ttl = OutboxRelay.dlq_resolved_ttl
    original_max_runtime = OutboxRelay.cleanup_max_runtime
    OutboxRelay.cleanup_enabled = true
    OutboxRelay.cleanup_batch_size = 10_000
    OutboxRelay.dlq_resolved_ttl = nil
    OutboxRelay.cleanup_max_runtime = 30
    example.run
  ensure
    OutboxRelay.cleanup_enabled = original_enabled
    OutboxRelay.cleanup_batch_size = original_batch
    OutboxRelay.dlq_resolved_ttl = original_dlq_ttl
    OutboxRelay.cleanup_max_runtime = original_max_runtime
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

    it 'loops across multiple chunks until the qualifying set is exhausted' do
      OutboxRelay.cleanup_batch_size = 2
      3.times { |i| create_event(sequence: i + 1, expires_at: 1.hour.ago) }
      set_consumer_offset(topic: 'orders', last_consumed_sequence: 100)

      result = described_class.new.perform

      # batch_size=2 + 3 rows → iter1 deletes 2, iter2 deletes 1 (<batch_size, exhausted, exit).
      expect(result[:events_deleted]).to eq(3)
      expect(OutboxRelay::OutboxEvent.count).to eq(0)
    end

    it 'exits the events loop when the deadline is hit even if rows remain' do
      OutboxRelay.cleanup_batch_size = 2
      6.times { |i| create_event(sequence: i + 1, expires_at: 1.hour.ago) }
      set_consumer_offset(topic: 'orders', last_consumed_sequence: 100)

      job = described_class.new
      # monotonic_now calls in order:
      #   1: started_at (perform top)
      #   2: deadline base (deadline = monotonic_now + budget → 0.0 + 30 = 30)
      #   (DLQ loop: dlq_resolved_ttl is nil → delete_resolved_dlq_chunk returns 0
      #    immediately; 0 < batch_size=2 → breaks BEFORE the monotonic_now deadline check)
      #   3: events iter 1 deadline check (0.0 < 30 → continue)
      #   4: events iter 2 deadline check (100.0 >= 30 → break)
      #   5: duration_since(started_at) in build_result
      #   6: duration_since(started_at) in ensure block
      allow(job).to receive(:monotonic_now).and_return(0.0, 0.0, 0.0, 100.0, 100.0, 100.0)

      result = job.perform

      # 2 events iterations × batch_size 2 = 4 deleted (out of 6 qualifying).
      expect(result[:events_deleted]).to eq(4)
      expect(OutboxRelay::OutboxEvent.count).to eq(2)
    end

    it 'always runs at least one chunk per phase even with zero runtime budget' do
      OutboxRelay.cleanup_max_runtime = 0
      OutboxRelay.cleanup_batch_size = 2
      4.times { |i| create_event(sequence: i + 1, expires_at: 1.hour.ago) }
      set_consumer_offset(topic: 'orders', last_consumed_sequence: 100)

      result = described_class.new.perform

      # First chunk runs (deletes 2), then deadline check fails (now >= now+0), break.
      # Second chunk does NOT run, leaving 2 events.
      expect(result[:events_deleted]).to eq(2)
      expect(OutboxRelay::OutboxEvent.count).to eq(2)
    end

    it 'accepts ActiveSupport::Duration for cleanup_max_runtime' do
      OutboxRelay.cleanup_max_runtime = 1.minute
      create_event(sequence: 1, expires_at: 1.hour.ago)
      set_consumer_offset(topic: 'orders', last_consumed_sequence: 100)

      result = described_class.new.perform

      expect(result[:events_deleted]).to eq(1)
    end

    it 'raises ConfigurationError when cleanup_max_runtime is not Numeric or Duration' do
      OutboxRelay.cleanup_max_runtime = '30'
      create_event(sequence: 1, expires_at: 1.hour.ago)
      set_consumer_offset(topic: 'orders', last_consumed_sequence: 100)

      expect { described_class.new.perform }
        .to raise_error(OutboxRelay::ConfigurationError, /cleanup_max_runtime/)
    end

    it 'raises ConfigurationError when cleanup_max_runtime is negative' do
      OutboxRelay.cleanup_max_runtime = -1
      create_event(sequence: 1, expires_at: 1.hour.ago)
      set_consumer_offset(topic: 'orders', last_consumed_sequence: 100)

      expect { described_class.new.perform }
        .to raise_error(OutboxRelay::ConfigurationError, /non-negative/)
    end

    it 'raises ConfigurationError when cleanup_batch_size is not a positive Integer' do
      create_event(sequence: 1, expires_at: 1.hour.ago)
      set_consumer_offset(topic: 'orders', last_consumed_sequence: 100)

      [0, -1, '10', nil, 1.5].each do |bad|
        OutboxRelay.cleanup_batch_size = bad
        expect { described_class.new.perform }
          .to raise_error(OutboxRelay::ConfigurationError, /cleanup_batch_size/),
              "expected ConfigurationError for cleanup_batch_size = #{bad.inspect}"
      end
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
      # Phase is genuinely skipped, not run as a no-op chunk: iterations[:dlq] == 0.
      expect(result[:iterations]).to eq(dlq: 0, events: 1)
      expect(OutboxRelay::DeadLetterEvent.count).to eq(1)
    end

    it 'loops DLQ deletes across multiple chunks until exhausted' do
      OutboxRelay.cleanup_batch_size = 2
      3.times { create_dlq(status: 'resolved', resolved_at: 30.days.ago) }

      result = described_class.new.perform

      expect(result[:dlq_deleted]).to eq(3)
      expect(OutboxRelay::DeadLetterEvent.count).to eq(0)
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

    it 'still runs one events chunk even when DLQ phase consumes the entire budget' do
      OutboxRelay.cleanup_batch_size = 2
      # 6 DLQ rows + 4 event rows. We stub the clock to push past deadline after
      # the 2nd DLQ chunk so the events phase enters with no remaining budget,
      # but the min-one rule still runs one events chunk.
      6.times { create_dlq(status: 'resolved', resolved_at: 30.days.ago) }
      4.times { |i| create_event(sequence: i + 1, expires_at: 1.hour.ago) }
      set_consumer_offset(topic: 'orders', last_consumed_sequence: 100)

      job = described_class.new
      # monotonic_now call sequence (deadline = 0 + 30 = 30; values >= 30 trip the break):
      #   1: started_at
      #   2: deadline base (returns 0 → deadline = 30)
      #   3: DLQ iter 1 deadline check (0 < 30, continue)
      #   4: DLQ iter 2 deadline check (100 >= 30, break)
      #   5: events iter 1 deadline check (100 >= 30, break — but iter 1 already ran per min-one rule)
      #   6: duration_since in build_result
      #   7: duration_since in ensure
      allow(job).to receive(:monotonic_now).and_return(0.0, 0.0, 0.0, 100.0, 100.0, 100.0, 100.0)

      result = job.perform

      expect(result[:dlq_deleted]).to eq(4)      # 2 DLQ iterations × 2
      expect(result[:events_deleted]).to eq(2)   # 1 events iteration × 2 (min-one)
      expect(OutboxRelay::DeadLetterEvent.count).to eq(2)
      expect(OutboxRelay::OutboxEvent.count).to eq(2)
    end

    it 'exits the DLQ loop when the deadline is hit even if rows remain' do
      OutboxRelay.cleanup_batch_size = 2
      6.times { create_dlq(status: 'resolved', resolved_at: 30.days.ago) }

      job = described_class.new
      # monotonic_now sequence:
      #   1: started_at
      #   2: deadline base (deadline = 30)
      #   3: DLQ iter 1 deadline check (0 < 30, continue)
      #   4: DLQ iter 2 deadline check (100 >= 30, break)
      #   5: events iter 1 — chunk returns 0 (no qualifying events), exits on
      #      exhaustion check before deadline check, so no monotonic_now call here
      #   5: duration_since in build_result
      #   6: duration_since in ensure
      allow(job).to receive(:monotonic_now).and_return(0.0, 0.0, 0.0, 100.0, 100.0, 100.0)

      result = job.perform

      expect(result[:dlq_deleted]).to eq(4)      # 2 DLQ iterations × 2
      expect(result[:iterations][:dlq]).to eq(2)
      expect(OutboxRelay::DeadLetterEvent.count).to eq(2)
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
        .to receive(:delete_expired_events_chunk).and_raise(fake_timeout, 'statement timeout')

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

    it 'preserves accumulated event counts when PG::QueryCanceled fires mid-loop' do
      OutboxRelay.cleanup_batch_size = 2
      4.times { |i| create_event(sequence: i + 1, expires_at: 1.hour.ago) }
      set_consumer_offset(topic: 'orders', last_consumed_sequence: 100)

      fake_timeout = Class.new(StandardError) do
        def self.name
          'PG::QueryCanceled'
        end
      end
      stub_const('OutboxRelay::Jobs::CleanupExpiredEventsJob::PG_QUERY_CANCELED', fake_timeout)

      call_count = 0
      allow_any_instance_of(described_class)
        .to receive(:delete_expired_events_chunk).and_wrap_original do |orig, *args|
          call_count += 1
          raise fake_timeout, 'statement timeout' if call_count == 2

          orig.call(*args)
        end

      result = nil
      payloads = with_cleanup_subscription { result = described_class.new.perform }

      # Iter 1 deleted 2 rows; iter 2 raised. Accumulated count = 2.
      expect(result).to include(events_deleted: 2, timeout: true)
      # Iteration counter must also reflect the completed iteration. The mid-loop
      # raise happens before the wrapper increments @events_iterations, so it
      # stays at the count from completed chunks (1 here).
      expect(result[:iterations]).to eq(dlq: 0, events: 1)
      expect(payloads.first).to include(
        events_deleted: 2,
        timeout: true,
        error_class: 'PG::QueryCanceled'
      )
    end

    it 'preserves accumulated DLQ counts when PG::QueryCanceled fires mid-DLQ-loop' do
      OutboxRelay.dlq_resolved_ttl = 14.days
      OutboxRelay.cleanup_batch_size = 2
      4.times { create_dlq(status: 'resolved', resolved_at: 30.days.ago) }

      fake_timeout = Class.new(StandardError) do
        def self.name
          'PG::QueryCanceled'
        end
      end
      stub_const('OutboxRelay::Jobs::CleanupExpiredEventsJob::PG_QUERY_CANCELED', fake_timeout)

      call_count = 0
      allow_any_instance_of(described_class)
        .to receive(:delete_resolved_dlq_chunk).and_wrap_original do |orig, *args|
          call_count += 1
          raise fake_timeout, 'statement timeout' if call_count == 2

          orig.call(*args)
        end

      result = nil
      payloads = with_cleanup_subscription { result = described_class.new.perform }

      # Iter 1 deleted 2 rows; iter 2 raised before events phase even started.
      expect(result).to include(dlq_deleted: 2, events_deleted: 0, timeout: true)
      expect(result[:iterations]).to eq(dlq: 1, events: 0)
      expect(payloads.first).to include(
        dlq_deleted: 2,
        timeout: true,
        error_class: 'PG::QueryCanceled'
      )
    end

    it 'emits notification and re-raises on unexpected errors' do
      create_event(sequence: 1, expires_at: 1.hour.ago)
      set_consumer_offset(topic: 'orders', last_consumed_sequence: 100)

      allow_any_instance_of(described_class)
        .to receive(:delete_expired_events_chunk).and_raise(StandardError, 'kaboom')
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

    it 'includes per-phase iteration counts in the notification payload' do
      OutboxRelay.dlq_resolved_ttl = 14.days
      OutboxRelay.cleanup_batch_size = 2
      4.times { |i| create_event(sequence: i + 1, expires_at: 1.hour.ago) }
      set_consumer_offset(topic: 'orders', last_consumed_sequence: 100)
      3.times { create_dlq(status: 'resolved', resolved_at: 30.days.ago) }

      payloads = with_cleanup_subscription { described_class.new.perform }

      expect(payloads.size).to eq(1)
      # DLQ: 3 rows / batch 2 → iter1=2 deleted, iter2=1 deleted (<batch, exhausted) → 2 iterations
      # events: 4 rows / batch 2 → iter1=2, iter2=2, iter3=0 (<batch, exhausted) → 3 iterations
      expect(payloads.first[:iterations]).to eq(dlq: 2, events: 3)
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
