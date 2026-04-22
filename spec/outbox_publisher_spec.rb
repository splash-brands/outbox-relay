# frozen_string_literal: true

require 'spec_helper'
require 'outbox_relay/outbox_publisher'

RSpec.describe OutboxRelay::OutboxPublisher do
  describe '.publish' do
    context 'with valid parameters' do
      it 'creates an outbox event' do
        expect do
          described_class.publish(
            topic: 'order_updates',
            payload: { order_id: 123 },
            headers: { event_name: 'created' }
          )
        end.to change { OutboxRelay::OutboxEvent.count }.by(1)
      end

      it 'sets the topic correctly' do
        event = described_class.publish(
          topic: 'order_updates',
          payload: { order_id: 123 },
          headers: { event_name: 'created' }
        )

        expect(event.topic).to eq('order_updates')
      end

      it 'sets the payload correctly' do
        payload = { order_id: 123, total: 99.99 }
        event = described_class.publish(
          topic: 'order_updates',
          payload: payload,
          headers: { event_name: 'created' }
        )

        expect(event.payload).to eq(payload.stringify_keys)
      end

      it 'extracts event_name from headers' do
        event = described_class.publish(
          topic: 'order_updates',
          payload: { order_id: 123 },
          headers: { event_name: 'created' }
        )

        expect(event.event_name).to eq('created')
      end

      it 'supports string keys in headers' do
        event = described_class.publish(
          topic: 'order_updates',
          payload: { order_id: 123 },
          headers: { 'event_name' => 'created' }
        )

        expect(event.event_name).to eq('created')
      end

      it 'stores remaining headers excluding event_name and partition_key' do
        event = described_class.publish(
          topic: 'order_updates',
          payload: { order_id: 123 },
          headers: {
            event_name: 'created',
            partition_key: 'order-123',
            correlation_id: 'abc-123',
            source: 'api'
          }
        )

        expect(event.headers).to eq({
                                      'correlation_id' => 'abc-123',
                                      'source' => 'api'
                                    })
        expect(event.headers).not_to have_key('event_name')
        expect(event.headers).not_to have_key('partition_key')
      end

      it 'supports expires_at parameter' do
        expires_at = 1.hour.from_now
        event = described_class.publish(
          topic: 'temporary_events',
          payload: { session_id: 'abc' },
          headers: { event_name: 'heartbeat' },
          expires_at: expires_at
        )

        expect(event.expires_at).to be_within(1.second).of(expires_at)
      end

      it 'defaults to partition 0 when no partition_key provided' do
        event = described_class.publish(
          topic: 'order_updates',
          payload: { order_id: 123 },
          headers: { event_name: 'created' }
        )

        expect(event.partition_key).to eq(0)
      end
    end

    context 'with partition_key' do
      before do
        # Mock configuration partitions
        allow(OutboxRelay.configuration).to receive(:partitions).and_return({
                                                                              'order_updates' => 4,
                                                                              'notifications' => 1
                                                                            })
      end

      it 'calculates partition from string key using CRC32' do
        event = described_class.publish(
          topic: 'order_updates',
          payload: { order_id: 123 },
          headers: {
            event_name: 'created',
            partition_key: 'customer-456'
          }
        )

        # Verify partition is within valid range
        expect(event.partition_key).to be >= 0
        expect(event.partition_key).to be < 4
      end

      it 'assigns same partition for same partition_key (deterministic)' do
        partition_key = 'customer-123'

        event1 = described_class.publish(
          topic: 'order_updates',
          payload: { order_id: 1 },
          headers: { event_name: 'created', partition_key: partition_key }
        )

        event2 = described_class.publish(
          topic: 'order_updates',
          payload: { order_id: 2 },
          headers: { event_name: 'created', partition_key: partition_key }
        )

        expect(event1.partition_key).to eq(event2.partition_key)
      end

      it 'distributes different keys across partitions' do
        partitions = 10.times.map do |i|
          event = described_class.publish(
            topic: 'order_updates',
            payload: { order_id: i },
            headers: { event_name: 'created', partition_key: "key-#{i}" }
          )
          event.partition_key
        end

        # Should use multiple partitions (not all the same)
        expect(partitions.uniq.size).to be > 1
      end

      it 'defaults to partition 0 for topics without partitioning configured' do
        event = described_class.publish(
          topic: 'notifications',
          payload: { message: 'hello' },
          headers: {
            event_name: 'sent',
            partition_key: 'user-123'
          }
        )

        expect(event.partition_key).to eq(0)
      end

      it 'supports string keys in headers for partition_key' do
        event = described_class.publish(
          topic: 'order_updates',
          payload: { order_id: 123 },
          headers: {
            'event_name' => 'created',
            'partition_key' => 'customer-456'
          }
        )

        expect(event.partition_key).to be >= 0
        expect(event.partition_key).to be < 4
      end
    end

    context 'with invalid parameters' do
      it 'raises error when topic is blank' do
        expect do
          described_class.publish(
            topic: '',
            payload: { order_id: 123 },
            headers: {}
          )
        end.to raise_error(ArgumentError, /topic must be present/)
      end

      it 'raises error when topic is nil' do
        expect do
          described_class.publish(
            topic: nil,
            payload: { order_id: 123 },
            headers: {}
          )
        end.to raise_error(ArgumentError, /topic must be present/)
      end

      it 'raises error when payload is not Hash or Array' do
        expect do
          described_class.publish(
            topic: 'order_updates',
            payload: 'invalid',
            headers: {}
          )
        end.to raise_error(ArgumentError, /payload must be a Hash or Array/)
      end

      it 'raises error when payload is nil' do
        expect do
          described_class.publish(
            topic: 'order_updates',
            payload: nil,
            headers: {}
          )
        end.to raise_error(ArgumentError, /payload must be a Hash or Array/)
      end

      it 'raises error when headers is not a Hash' do
        expect do
          described_class.publish(
            topic: 'order_updates',
            payload: { order_id: 123 },
            headers: 'invalid'
          )
        end.to raise_error(ArgumentError, /headers must be a Hash/)
      end

      it 'raises PublishError when event creation fails' do
        allow(OutboxRelay::OutboxEvent).to receive(:create!).and_raise(
          ActiveRecord::RecordInvalid.new(OutboxRelay::OutboxEvent.new)
        )

        expect do
          described_class.publish(
            topic: 'order_updates',
            payload: { order_id: 123 },
            headers: {}
          )
        end.to raise_error(OutboxRelay::OutboxPublisher::PublishError, /Failed to publish event/)
      end
    end

    context 'with Array payload' do
      it 'accepts Array payload' do
        payload = [{ id: 1 }, { id: 2 }, { id: 3 }]
        event = described_class.publish(
          topic: 'batch_updates',
          payload: payload,
          headers: { event_name: 'batch_created' }
        )

        expect(event.payload).to eq(payload.map(&:stringify_keys))
      end
    end

    context 'without headers' do
      it 'creates event with empty headers when not provided' do
        event = described_class.publish(
          topic: 'simple_events',
          payload: { data: 'test' }
        )

        expect(event.event_name).to be_nil
        expect(event.partition_key).to eq(0)
        expect(event.headers).to eq({})
      end
    end

    context 'with OutboxRelay.default_event_ttl configured' do
      let(:ttl) { 14.days }

      around do |example|
        original = OutboxRelay.default_event_ttl
        OutboxRelay.default_event_ttl = ttl
        example.run
      ensure
        OutboxRelay.default_event_ttl = original
      end

      it 'applies default TTL when expires_at is not provided' do
        event = described_class.publish(
          topic: 'order_updates',
          payload: { order_id: 123 },
          headers: { event_name: 'created' }
        )

        expect(event.expires_at).to be_within(5.seconds).of(ttl.from_now)
      end

      it 'uses explicit expires_at over the default TTL' do
        explicit = 1.hour.from_now

        event = described_class.publish(
          topic: 'order_updates',
          payload: { order_id: 123 },
          headers: { event_name: 'created' },
          expires_at: explicit
        )

        expect(event.expires_at).to be_within(1.second).of(explicit)
      end

      it 'treats explicit expires_at: nil as opt-out (never expires)' do
        event = described_class.publish(
          topic: 'audit_log',
          payload: { action: 'user_deleted' },
          headers: { event_name: 'deleted' },
          expires_at: nil
        )

        expect(event.expires_at).to be_nil
      end
    end

    context 'without OutboxRelay.default_event_ttl configured' do
      it 'leaves expires_at nil when nothing is provided' do
        event = described_class.publish(
          topic: 'order_updates',
          payload: { order_id: 123 },
          headers: { event_name: 'created' }
        )

        expect(event.expires_at).to be_nil
      end
    end
  end

  describe '.calculate_partition_key' do
    it 'is private' do
      expect(described_class).not_to respond_to(:calculate_partition_key)
    end
  end

  describe '.fetch_partition_count' do
    it 'is private' do
      expect(described_class).not_to respond_to(:fetch_partition_count)
    end
  end

  describe '.validate_parameters!' do
    it 'is private' do
      expect(described_class).not_to respond_to(:validate_parameters!)
    end
  end
end
