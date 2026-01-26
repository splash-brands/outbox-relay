# frozen_string_literal: true

require 'spec_helper'

RSpec.describe OutboxRelay::Processes::PartitionClaiming do
  # Create a test class that includes the module
  let(:test_class) do
    Class.new do
      include OutboxRelay::Processes::PartitionClaiming

      attr_accessor :consumer_group, :topic, :partition_key

      def initialize(consumer_group:, topic:, partition_key:)
        @consumer_group = consumer_group
        @topic = topic
        @partition_key = partition_key
        @stopped = false
      end

      def stop
        @stopped = true
      end

      def stopped?
        @stopped
      end
    end
  end

  let(:worker) do
    test_class.new(
      consumer_group: 'test-group',
      topic: 'test-topic',
      partition_key: 0
    )
  end

  describe 'CLAIM_TTL constant' do
    it 'is set to 30 seconds' do
      expect(described_class::CLAIM_TTL).to eq(30.seconds)
    end
  end

  describe '#claim_partition' do
    context 'when partition is unclaimed' do
      it 'claims successfully' do
        worker.claim_partition

        expect(worker.instance_variable_get(:@partition_claimed)).to be true
      end

      it 'creates consumer offset record' do
        worker.claim_partition

        offset = OutboxRelay::ConsumerOffset.find_by(
          consumer_group: 'test-group_p0',
          topic: 'test-topic'
        )
        expect(offset).to be_present
        expect(offset.claimed?).to be true
      end

      it 'stores offset reference' do
        worker.claim_partition

        offset = worker.instance_variable_get(:@partition_offset)
        expect(offset).to be_a(OutboxRelay::ConsumerOffset)
        expect(offset.consumer_group).to eq('test-group_p0')
      end

      it 'logs claim success' do
        expect(OutboxRelay.logger).to receive(:info).with(
          hash_including(event_name: 'partition_claimed')
        )

        worker.claim_partition
      end
    end

    context 'when partition is already claimed by another worker' do
      before do
        OutboxRelay::ConsumerOffset.create!(
          consumer_group: 'test-group_p0',
          topic: 'test-topic',
          last_consumed_sequence: 0,
          claimed_by: 'other-worker',
          claimed_until: 1.minute.from_now
        )
      end

      it 'exits gracefully with claim unavailable exit code' do
        expect do
          worker.claim_partition
        end.to raise_error(SystemExit) { |e|
          expect(e.status).to eq(described_class::CLAIM_UNAVAILABLE_EXIT_STATUS)
        }
      end

      it 'logs claim failure' do
        expect(OutboxRelay.logger).to receive(:debug).with(
          hash_including(event_name: 'partition_claim_failed')
        )
        # DEBUG level: This is expected behavior in multi-container deployments
        expect(OutboxRelay.logger).to receive(:debug).with(
          hash_including(event_name: 'worker_exiting_claim_unavailable')
        )

        expect { worker.claim_partition }.to raise_error(SystemExit)
      end

      it 'does not set partition_claimed flag' do
        expect { worker.claim_partition }.to raise_error(SystemExit)

        expect(worker.instance_variable_get(:@partition_claimed)).to be_nil
      end
    end

    context 'when claiming expired partition' do
      before do
        OutboxRelay::ConsumerOffset.create!(
          consumer_group: 'test-group_p0',
          topic: 'test-topic',
          last_consumed_sequence: 100,
          claimed_by: 'dead-worker',
          claimed_until: 1.minute.ago
        )
      end

      it 'claims successfully (takes over expired claim)' do
        worker.claim_partition

        expect(worker.instance_variable_get(:@partition_claimed)).to be true

        offset = OutboxRelay::ConsumerOffset.find_by(
          consumer_group: 'test-group_p0',
          topic: 'test-topic'
        )
        expect(offset.claimed?).to be true
        expect(offset.claimed_by).to include('test-group')
      end

      it 'preserves existing offset sequence' do
        worker.claim_partition

        offset = OutboxRelay::ConsumerOffset.find_by(
          consumer_group: 'test-group_p0',
          topic: 'test-topic'
        )
        expect(offset.last_consumed_sequence).to eq(100)
      end
    end
  end

  describe '#renew_partition_claim' do
    context 'when holding claim' do
      before do
        worker.claim_partition
      end

      it 'extends claim TTL' do
        offset = worker.instance_variable_get(:@partition_offset)
        original_until = offset.claimed_until

        sleep 0.1
        worker.renew_partition_claim

        expect(offset.reload.claimed_until).to be > original_until
      end

      it 'does not stop worker' do
        worker.renew_partition_claim

        expect(worker.stopped?).to be false
      end
    end

    context 'when claim was stolen' do
      before do
        worker.claim_partition
        offset = worker.instance_variable_get(:@partition_offset)

        # Simulate another worker stealing the claim
        offset.update!(
          claimed_by: 'thief-worker',
          claimed_until: 1.minute.from_now
        )
      end

      it 'stops the worker' do
        worker.renew_partition_claim

        expect(worker.stopped?).to be true
      end

      it 'logs claim lost' do
        expect(OutboxRelay.logger).to receive(:error).with(
          hash_including(event_name: 'partition_claim_lost')
        )

        worker.renew_partition_claim
      end
    end

    context 'when partition not claimed' do
      it 'does nothing' do
        expect do
          worker.renew_partition_claim
        end.not_to raise_error

        expect(worker.stopped?).to be false
      end
    end
  end

  describe '#release_partition_claim' do
    context 'when holding claim' do
      before do
        worker.claim_partition
      end

      it 'releases the claim' do
        worker.release_partition_claim

        offset = OutboxRelay::ConsumerOffset.find_by(
          consumer_group: 'test-group_p0',
          topic: 'test-topic'
        )
        expect(offset.claimed?).to be false
        expect(offset.claimed_by).to be_nil
      end

      it 'clears partition_claimed flag' do
        worker.release_partition_claim

        expect(worker.instance_variable_get(:@partition_claimed)).to be false
      end

      it 'logs release' do
        expect(OutboxRelay.logger).to receive(:info).with(
          hash_including(event_name: 'partition_claim_released')
        )

        worker.release_partition_claim
      end
    end

    context 'when partition not claimed' do
      it 'does nothing' do
        expect do
          worker.release_partition_claim
        end.not_to raise_error
      end
    end

    context 'when release fails' do
      before do
        worker.claim_partition
        allow(worker.instance_variable_get(:@partition_offset)).to receive(:release_claim!)
          .and_raise(StandardError, 'DB connection lost')
      end

      it 'does not raise (best-effort cleanup)' do
        expect do
          worker.release_partition_claim
        end.not_to raise_error
      end

      it 'logs at debug level' do
        expect(OutboxRelay.logger).to receive(:debug).with(
          hash_including(event_name: 'partition_claim_release_failed')
        )

        worker.release_partition_claim
      end
    end
  end

  describe 'consumer_group_with_partition' do
    it 'appends partition key to consumer group' do
      expect(worker.send(:consumer_group_with_partition)).to eq('test-group_p0')
    end

    it 'handles different partition keys' do
      worker2 = test_class.new(
        consumer_group: 'test-group',
        topic: 'test-topic',
        partition_key: 3
      )

      expect(worker2.send(:consumer_group_with_partition)).to eq('test-group_p3')
    end
  end

  describe 'build_consumer_instance_id' do
    it 'includes consumer group, hostname, pid, and partition' do
      instance_id = worker.send(:build_consumer_instance_id)

      expect(instance_id).to include('test-group')
      expect(instance_id).to include(Socket.gethostname)
      expect(instance_id).to include(Process.pid.to_s)
      expect(instance_id).to include('p0')
    end

    it 'is memoized' do
      id1 = worker.send(:build_consumer_instance_id)
      id2 = worker.send(:build_consumer_instance_id)

      expect(id1).to eq(id2)
      expect(id1.object_id).to eq(id2.object_id)
    end
  end

  describe 'resolve_auto_offset_reset' do
    context 'when consumer_class_name is not available' do
      it 'falls back to :latest' do
        expect(worker.send(:resolve_auto_offset_reset)).to eq(:latest)
      end
    end

    context 'when consumer_class_name is available' do
      # Create a test consumer class with custom auto_offset_reset
      let(:consumer_class) do
        Class.new(OutboxRelay::OutboxConsumer) do
          def initialize(partition_key:)
            super(
              consumer_group: 'test-group',
              topic: 'test-topic',
              partition_key: partition_key,
              auto_offset_reset: :earliest
            )
          end

          def consume_message(_event)
            true
          end
        end
      end

      let(:worker_with_consumer_class) do
        consumer_klass = consumer_class
        Class.new do
          include OutboxRelay::Processes::PartitionClaiming

          attr_accessor :consumer_group, :topic, :partition_key, :consumer_class_name

          def initialize(consumer_group:, topic:, partition_key:, consumer_class_name:)
            @consumer_group = consumer_group
            @topic = topic
            @partition_key = partition_key
            @consumer_class_name = consumer_class_name
            @stopped = false
          end

          def stop
            @stopped = true
          end
        end.new(
          consumer_group: 'test-group',
          topic: 'test-topic',
          partition_key: 0,
          consumer_class_name: consumer_klass.name
        )
      end

      before do
        # Register the consumer class so constantize can find it
        stub_const('TestEarliestConsumer', consumer_class)
        worker_with_consumer_class.instance_variable_set(:@consumer_class_name, 'TestEarliestConsumer')
      end

      it 'uses auto_offset_reset from consumer class' do
        expect(worker_with_consumer_class.send(:resolve_auto_offset_reset)).to eq(:earliest)
      end

      it 'creates offset with correct initial sequence when claiming partition' do
        # Create existing events to verify :earliest vs :latest behavior
        OutboxRelay::OutboxEvent.create!(
          topic: 'test-topic',
          event_name: 'test.event',
          payload: {},
          sequence: 1000,
          partition_key: 0
        )

        worker_with_consumer_class.claim_partition

        offset = OutboxRelay::ConsumerOffset.find_by(
          consumer_group: 'test-group_p0',
          topic: 'test-topic'
        )

        # With :earliest, should start from 0 (process all historical events)
        expect(offset.last_consumed_sequence).to eq(0)
      end
    end

    context 'when consumer_class_name is invalid' do
      let(:worker_with_invalid_class) do
        Class.new do
          include OutboxRelay::Processes::PartitionClaiming

          attr_accessor :consumer_group, :topic, :partition_key, :consumer_class_name

          def initialize
            @consumer_group = 'test-group'
            @topic = 'test-topic'
            @partition_key = 0
            @consumer_class_name = 'NonExistentConsumer'
          end

          def stop; end
        end.new
      end

      it 'falls back to :latest and logs warning' do
        expect(OutboxRelay.logger).to receive(:warn).with(
          hash_including(
            event_name: 'auto_offset_reset_resolution_failed',
            fallback: :latest
          )
        )

        expect(worker_with_invalid_class.send(:resolve_auto_offset_reset)).to eq(:latest)
      end
    end
  end
end
