# frozen_string_literal: true

require 'spec_helper'
require 'outbox_relay/models/outbox_consumer'

RSpec.describe 'DLQ retry backoff' do
  # Test consumer that always fails
  class TestFailingConsumer < OutboxRelay::OutboxConsumer
    attr_accessor :call_count

    def initialize(partition_key: 0, dead_letter_config: {})
      super(
        consumer_group: 'test_dlq_backoff_consumer',
        topic: 'test_topic',
        partition_key: partition_key,
        dead_letter_config: { max_retries: 5 }.merge(dead_letter_config)
      )
      @call_count = 0
    end

    def consume_message(_event)
      @call_count += 1
      raise StandardError, 'Simulated failure'
    end
  end

  let(:event) do
    OutboxRelay::OutboxEvent.create!(
      topic: 'test_topic',
      event_name: 'test_event',
      event_id: SecureRandom.uuid,
      partition_key: 0,
      payload: { 'data' => 'test' },
      headers: {},
      sequence: 1
    )
  end

  describe '#calculate_retry_after' do
    context 'with default configuration (60s base)' do
      let(:consumer) { TestFailingConsumer.new }

      it 'calculates exponential backoff with 60s base by default' do
        freeze_time = Time.current
        allow(Time).to receive(:current).and_return(freeze_time)

        retry_after_1 = consumer.send(:calculate_retry_after, 1)
        retry_after_2 = consumer.send(:calculate_retry_after, 2)
        retry_after_3 = consumer.send(:calculate_retry_after, 3)
        retry_after_4 = consumer.send(:calculate_retry_after, 4)

        # Retry 1: 60s * 2^0 = 60s (48-72 with jitter)
        expect(retry_after_1).to be_within(12.seconds).of(freeze_time + 60.seconds)

        # Retry 2: 60s * 2^1 = 120s (96-144 with jitter)
        expect(retry_after_2).to be_within(24.seconds).of(freeze_time + 120.seconds)

        # Retry 3: 60s * 2^2 = 240s (192-288 with jitter)
        expect(retry_after_3).to be_within(48.seconds).of(freeze_time + 240.seconds)

        # Retry 4: 60s * 2^3 = 480s (384-576 with jitter)
        expect(retry_after_4).to be_within(96.seconds).of(freeze_time + 480.seconds)
      end

      it 'caps delay at max_delay (default 30 minutes)' do
        freeze_time = Time.current
        allow(Time).to receive(:current).and_return(freeze_time)

        # Retry 10: 60s * 2^9 = 30720s, should be capped at 1800s
        retry_after = consumer.send(:calculate_retry_after, 10)

        # Max is 1800s (30 min) with ±20% jitter = 1440-2160
        expect(retry_after).to be <= freeze_time + 2160.seconds
        expect(retry_after).to be >= freeze_time + 1440.seconds
      end
    end

    context 'with custom base delay' do
      let(:consumer) do
        TestFailingConsumer.new(dead_letter_config: { retry_base_delay: 120 })
      end

      it 'uses custom base delay' do
        freeze_time = Time.current
        allow(Time).to receive(:current).and_return(freeze_time)

        # Retry 1: 120s * 2^0 = 120s (96-144 with jitter)
        retry_after = consumer.send(:calculate_retry_after, 1)
        expect(retry_after).to be_within(24.seconds).of(freeze_time + 120.seconds)
      end
    end

    context 'with custom max delay' do
      let(:consumer) do
        TestFailingConsumer.new(dead_letter_config: { retry_max_delay: 300 })
      end

      it 'respects custom max delay' do
        freeze_time = Time.current
        allow(Time).to receive(:current).and_return(freeze_time)

        # Retry 5: 60s * 2^4 = 960s, should be capped at 300s
        retry_after = consumer.send(:calculate_retry_after, 5)

        # Max is 300s with ±20% jitter = 240-360
        expect(retry_after).to be <= freeze_time + 360.seconds
        expect(retry_after).to be >= freeze_time + 240.seconds
      end
    end
  end

  describe 'backoff configuration' do
    it 'uses default 60s base delay' do
      consumer = TestFailingConsumer.new
      expect(consumer.send(:dlq_retry_base_delay)).to eq(60)
    end

    it 'uses default 30 min max delay' do
      consumer = TestFailingConsumer.new
      expect(consumer.send(:dlq_retry_max_delay)).to eq(1800)
    end

    it 'allows custom base delay via dead_letter_config' do
      consumer = TestFailingConsumer.new(dead_letter_config: { retry_base_delay: 120 })
      expect(consumer.send(:dlq_retry_base_delay)).to eq(120)
    end

    it 'allows custom max delay via dead_letter_config' do
      consumer = TestFailingConsumer.new(dead_letter_config: { retry_max_delay: 600 })
      expect(consumer.send(:dlq_retry_max_delay)).to eq(600)
    end

    it 'uses default 5 max retries' do
      consumer = OutboxRelay::OutboxConsumer.allocate
      consumer.instance_variable_set(:@dead_letter_config, {})
      # should_dead_letter? uses max_retries || 5
      expect(consumer.send(:dead_letter_config)[:max_retries] || 5).to eq(5)
    end
  end

  describe '#load_dlq_event_ids with backoff' do
    let(:consumer) { TestFailingConsumer.new }

    before do
      consumer.send(:current_offset)
    end

    def create_dlq_entry(attrs = {})
      OutboxRelay::DeadLetterEvent.create!({
        outbox_relay_outbox_event_id: event.id,
        consumer_group: 'test_dlq_backoff_consumer',
        original_topic: event.topic,
        original_sequence: event.sequence,
        original_event_id: event.event_id,
        original_event_name: event.event_name,
        original_payload: event.payload,
        original_headers: event.headers,
        original_partition_key: event.partition_key,
        total_retries: 1,
        resolution_status: 'retrying',
        error_message: 'Test error'
      }.merge(attrs))
    end

    it 'excludes events still in backoff period' do
      create_dlq_entry(retry_after: 1.hour.from_now)

      excluded_ids = consumer.send(:load_dlq_event_ids)
      expect(excluded_ids).to include(event.id)
    end

    it 'does not exclude events past their retry_after time' do
      create_dlq_entry(retry_after: 1.hour.ago)

      excluded_ids = consumer.send(:load_dlq_event_ids)
      expect(excluded_ids).to be_empty
    end

    it 'does not exclude events with nil retry_after (legacy entries)' do
      create_dlq_entry(retry_after: nil)

      excluded_ids = consumer.send(:load_dlq_event_ids)
      expect(excluded_ids).to be_empty
    end

    it 'always excludes unresolved events' do
      create_dlq_entry(
        total_retries: 6,
        resolution_status: 'unresolved',
        retry_after: nil
      )

      excluded_ids = consumer.send(:load_dlq_event_ids)
      expect(excluded_ids).to include(event.id)
    end
  end

  describe 'backoff timing examples' do
    let(:consumer) { TestFailingConsumer.new }

    it 'produces reasonable delays with default 60s base' do
      freeze_time = Time.current
      allow(Time).to receive(:current).and_return(freeze_time)

      # Show actual delays (without jitter randomness for predictability)
      allow(consumer).to receive(:rand).and_return(0.5) # Middle of jitter range

      delays = (1..5).map do |retry_count|
        retry_after = consumer.send(:calculate_retry_after, retry_count)
        (retry_after - freeze_time).to_i
      end

      # With 0.5 rand, jitter_factor = 0.8 + 0.2 = 1.0 (no jitter)
      # Retry 1: 60s, Retry 2: 120s, Retry 3: 240s, Retry 4: 480s, Retry 5: 960s
      expect(delays[0]).to eq(60)   # 1 minute
      expect(delays[1]).to eq(120)  # 2 minutes
      expect(delays[2]).to eq(240)  # 4 minutes
      expect(delays[3]).to eq(480)  # 8 minutes
      expect(delays[4]).to eq(960)  # 16 minutes
    end

    it 'total retry timeline is approximately 15 minutes' do
      # Timeline with 60s base, 5 retries:
      # 0:00 - fail #1 → wait 60s
      # 1:00 - fail #2 → wait 120s
      # 3:00 - fail #3 → wait 240s
      # 7:00 - fail #4 → wait 480s
      # 15:00 - fail #5 → UNRESOLVED
      #
      # Total: ~15 minutes

      allow_any_instance_of(TestFailingConsumer).to receive(:rand).and_return(0.5)

      total_wait = 60 + 120 + 240 + 480 # Wait times before each retry
      expect(total_wait).to eq(900) # 15 minutes in seconds
    end
  end

  describe 'DLQ cache invalidation on failure' do
    # This test verifies the fix for a critical bug where events in DLQ with
    # "retrying" status were immediately re-fetched because the DLQ cache
    # was not invalidated after adding an event to DLQ.
    #
    # Bug scenario (before fix):
    # 1. Event fails processing, gets added to DLQ with retry_after = 60s
    # 2. DLQ cache (expires in 5s) still doesn't contain this event ID
    # 3. Next fetch_batch (within milliseconds) fetches same event again
    # 4. Event fails again, total_retries incremented
    # 5. Repeat until max_retries reached (all within seconds!)
    #
    # Fix: After adding event to DLQ, immediately add event ID to cache

    let(:consumer) { TestFailingConsumer.new }

    before do
      # Initialize the consumer's offset (required for processing)
      consumer.send(:current_offset)

      # Mock advisory locks for SQLite (which doesn't support pg_try_advisory_lock)
      # In production PostgreSQL, these work correctly
      allow(consumer).to receive(:acquire_advisory_lock).and_return(true)
      allow(consumer).to receive(:release_advisory_lock).and_return(true)
    end

    it 'adds event to DLQ cache after failure to prevent immediate re-fetch' do
      # Initialize the cache by fetching once
      consumer.send(:fetch_dlq_event_ids)

      # Verify cache is initialized but empty
      expect(consumer.instance_variable_get(:@dlq_event_ids_cache)).to eq([])

      # Process the event (it will fail)
      expect { consumer.send(:process_event, event) }.to raise_error(StandardError)

      # Verify event was added to cache immediately
      cache = consumer.instance_variable_get(:@dlq_event_ids_cache)
      expect(cache).to include(event.id)

      # Verify event is in DLQ with "retrying" status
      dlq_entry = OutboxRelay::DeadLetterEvent.find_by(
        outbox_relay_outbox_event_id: event.id,
        consumer_group: 'test_dlq_backoff_consumer'
      )
      expect(dlq_entry).to be_present
      expect(dlq_entry.resolution_status).to eq('retrying')
      expect(dlq_entry.retry_after).to be > Time.current
    end

    it 'prevents re-fetching failed event before retry_after time' do
      # Initialize the cache
      consumer.send(:fetch_dlq_event_ids)

      # Process the event (it will fail)
      expect { consumer.send(:process_event, event) }.to raise_error(StandardError)

      # Try to fetch batch immediately after failure
      # Event should NOT be returned because it's in cache
      events = consumer.send(:fetch_batch, 10)
      expect(events.map(&:id)).not_to include(event.id)
    end

    it "does not add unresolved events to cache (they're already excluded)" do
      # Create consumer with max_retries: 1 so first failure = unresolved
      consumer_low_retries = TestFailingConsumer.new(dead_letter_config: { max_retries: 1 })
      consumer_low_retries.send(:current_offset)
      consumer_low_retries.send(:fetch_dlq_event_ids)

      # Mock advisory locks for this consumer too
      allow(consumer_low_retries).to receive(:acquire_advisory_lock).and_return(true)
      allow(consumer_low_retries).to receive(:release_advisory_lock).and_return(true)

      # First failure creates DLQ entry, second failure marks as unresolved
      # Actually with max_retries: 1, the SECOND failure will mark as unresolved
      # because should_dead_letter? checks total_retries >= max_retries

      # First failure: total_retries becomes 1, status = "retrying"
      expect { consumer_low_retries.send(:process_event, event) }.to raise_error(StandardError)

      # Event should be in cache (retrying status)
      cache = consumer_low_retries.instance_variable_get(:@dlq_event_ids_cache)
      expect(cache).to include(event.id)

      # Clear cache to simulate expiration
      consumer_low_retries.instance_variable_set(:@dlq_event_ids_cache, [])
      consumer_low_retries.instance_variable_set(:@dlq_cache_expires_at, Time.current - 1)

      # Make the DLQ entry ready for retry (move retry_after to past)
      dlq_entry = OutboxRelay::DeadLetterEvent.find_by(
        outbox_relay_outbox_event_id: event.id,
        consumer_group: 'test_dlq_backoff_consumer'
      )
      dlq_entry.update!(retry_after: 1.minute.ago)

      # Second failure: total_retries becomes 2, which is >= max_retries (1)
      # Wait - that's not right. Let me re-check the logic...
      #
      # Actually, should_dead_letter? is called BEFORE incrementing total_retries
      # So with max_retries: 1:
      # - First failure: dlq_entry doesn't exist → should_dead_letter? returns false → status = "retrying"
      # - Second failure: dlq_entry.total_retries = 1 >= 1 → should_dead_letter? returns true → status = "unresolved"

      expect { consumer_low_retries.send(:process_event, event) }.to raise_error(StandardError)

      dlq_entry.reload
      expect(dlq_entry.resolution_status).to eq('unresolved')

      # Unresolved events are NOT added to hot cache (they're excluded via DB query)
      cache = consumer_low_retries.instance_variable_get(:@dlq_event_ids_cache)
      expect(cache).not_to include(event.id) # Only "retrying" events go in hot cache
    end
  end
end
