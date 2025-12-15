# frozen_string_literal: true

require "spec_helper"

RSpec.describe OutboxRelay::Worker, "partition claiming integration" do
  let(:worker) do
    described_class.new(
      consumer_class: "TestConsumer",
      consumer_group: "test-group",
      topic: "test-topic",
      partition_key: 0,
      batch_size: 10,
      polling_interval: 1.0
    )
  end

  describe "module inclusion" do
    it "includes PartitionClaiming module" do
      expect(worker.class.included_modules).to include(OutboxRelay::Processes::PartitionClaiming)
    end

    it "responds to claim_partition" do
      expect(worker).to respond_to(:claim_partition)
    end

    it "responds to renew_partition_claim" do
      expect(worker).to respond_to(:renew_partition_claim)
    end

    it "responds to release_partition_claim" do
      expect(worker).to respond_to(:release_partition_claim)
    end
  end

  describe "before_shutdown callback" do
    it "has release_partition_claim registered" do
      # Check that the callback is registered by verifying the method is called on shutdown
      expect(worker).to respond_to(:release_partition_claim)

      # Verify callback is in the shutdown chain by checking callbacks contain :release_partition_claim
      shutdown_callbacks = worker.class._shutdown_callbacks.to_a
      callback_methods = shutdown_callbacks.map do |cb|
        # Handle both Symbol filters and Callback objects
        cb.is_a?(Symbol) ? cb : (cb.instance_variable_get(:@key) || cb.to_s.to_sym)
      end
      expect(callback_methods).to include(:release_partition_claim)
    end
  end

  describe "partition claiming flow" do
    describe "#claim_partition" do
      it "creates consumer offset with claim" do
        worker.claim_partition

        offset = OutboxRelay::ConsumerOffset.find_by(
          consumer_group: "test-group_p0",
          topic: "test-topic"
        )

        expect(offset).to be_present
        expect(offset.claimed?).to be true
        expect(offset.claimed_by).to be_present
      end

      it "uses correct consumer_group_with_partition" do
        worker.claim_partition

        offset = OutboxRelay::ConsumerOffset.find_by(
          consumer_group: "test-group_p0",
          topic: "test-topic"
        )

        expect(offset).to be_present
      end
    end

    describe "failover scenario" do
      it "allows new worker to claim after TTL expiration" do
        # Worker 1 claims with short TTL
        offset = OutboxRelay::ConsumerOffset.create!(
          consumer_group: "test-group_p0",
          topic: "test-topic",
          last_consumed_sequence: 50,
          claimed_by: "dead-worker-123",
          claimed_until: 0.5.seconds.from_now
        )

        # Worker 2 should fail immediately (claim active)
        worker2 = described_class.new(
          consumer_class: "TestConsumer",
          consumer_group: "test-group",
          topic: "test-topic",
          partition_key: 0,
          batch_size: 10
        )

        expect {
          worker2.claim_partition
        }.to raise_error(SystemExit)

        # Wait for TTL to expire
        sleep 0.6

        # Worker 3 should succeed
        worker3 = described_class.new(
          consumer_class: "TestConsumer",
          consumer_group: "test-group",
          topic: "test-topic",
          partition_key: 0,
          batch_size: 10
        )

        expect {
          worker3.claim_partition
        }.not_to raise_error

        offset.reload
        expect(offset.claimed?).to be true
        expect(offset.claimed_by).not_to eq("dead-worker-123")

        # Verify offset sequence preserved (no data loss)
        expect(offset.last_consumed_sequence).to eq(50)
      end
    end

    describe "multiple partitions" do
      it "allows claiming different partitions simultaneously" do
        workers = 4.times.map do |partition|
          described_class.new(
            consumer_class: "TestConsumer",
            consumer_group: "test-group",
            topic: "test-topic",
            partition_key: partition,
            batch_size: 10
          )
        end

        # All workers should claim successfully (different partitions)
        workers.each do |w|
          expect { w.claim_partition }.not_to raise_error
        end

        # Verify all partitions are claimed
        4.times do |partition|
          offset = OutboxRelay::ConsumerOffset.find_by(
            consumer_group: "test-group_p#{partition}",
            topic: "test-topic"
          )
          expect(offset).to be_present
          expect(offset.claimed?).to be true
        end
      end

      it "prevents duplicate claims on same partition" do
        # Simulate external worker already holding claim (different PID/host)
        OutboxRelay::ConsumerOffset.create!(
          consumer_group: "test-group_p2",
          topic: "test-topic",
          last_consumed_sequence: 0,
          claimed_by: "different-worker-on-other-host-99999-p2",
          claimed_until: 1.minute.from_now
        )

        worker2 = described_class.new(
          consumer_class: "TestConsumer",
          consumer_group: "test-group",
          topic: "test-topic",
          partition_key: 2,
          batch_size: 10
        )

        # Worker should exit because partition is claimed by different worker
        expect {
          worker2.claim_partition
        }.to raise_error(SystemExit) { |e| expect(e.status).to eq(0) }

        # External worker still holds claim
        offset = OutboxRelay::ConsumerOffset.find_by(
          consumer_group: "test-group_p2",
          topic: "test-topic"
        )
        expect(offset.claimed_by).to eq("different-worker-on-other-host-99999-p2")
      end
    end

    describe "claim renewal" do
      it "extends claim on renew" do
        worker.claim_partition
        offset = worker.instance_variable_get(:@partition_offset)
        original_until = offset.claimed_until

        sleep 0.1
        worker.renew_partition_claim

        offset.reload
        expect(offset.claimed_until).to be > original_until
      end
    end

    describe "claim release" do
      it "allows immediate reclaim after release" do
        worker.claim_partition
        worker.release_partition_claim

        # Another worker should be able to claim immediately
        worker2 = described_class.new(
          consumer_class: "TestConsumer",
          consumer_group: "test-group",
          topic: "test-topic",
          partition_key: 0,
          batch_size: 10
        )

        expect { worker2.claim_partition }.not_to raise_error

        offset = OutboxRelay::ConsumerOffset.find_by(
          consumer_group: "test-group_p0",
          topic: "test-topic"
        )
        expect(offset.claimed?).to be true
      end
    end
  end
end
