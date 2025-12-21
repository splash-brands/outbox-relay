# frozen_string_literal: true

require "spec_helper"

RSpec.describe OutboxRelay::ConsumerOffset, "partition claiming" do
  let(:consumer_group) { "test-group_p0" }
  let(:topic) { "test-topic" }
  let(:offset) do
    described_class.create!(
      consumer_group: consumer_group,
      topic: topic,
      last_consumed_sequence: 0
    )
  end

  describe "CLAIM_TTL constant" do
    it "is set to 30 seconds" do
      expect(described_class::CLAIM_TTL).to eq(30.seconds)
    end
  end

  describe ".claimed scope" do
    it "returns only actively claimed offsets" do
      # Unclaimed offset
      unclaimed = described_class.create!(
        consumer_group: "unclaimed_p0",
        topic: topic,
        last_consumed_sequence: 0
      )

      # Claimed and active
      claimed_active = described_class.create!(
        consumer_group: "claimed_active_p0",
        topic: topic,
        last_consumed_sequence: 0,
        claimed_by: "worker-1",
        claimed_until: 1.minute.from_now
      )

      # Claimed but expired
      claimed_expired = described_class.create!(
        consumer_group: "claimed_expired_p0",
        topic: topic,
        last_consumed_sequence: 0,
        claimed_by: "worker-2",
        claimed_until: 1.minute.ago
      )

      results = described_class.claimed

      expect(results).to include(claimed_active)
      expect(results).not_to include(unclaimed)
      expect(results).not_to include(claimed_expired)
    end
  end

  describe ".unclaimed scope" do
    it "returns offsets that are unclaimed or expired" do
      # Unclaimed offset
      unclaimed = described_class.create!(
        consumer_group: "unclaimed_p0",
        topic: topic,
        last_consumed_sequence: 0
      )

      # Claimed and active
      claimed_active = described_class.create!(
        consumer_group: "claimed_active_p0",
        topic: topic,
        last_consumed_sequence: 0,
        claimed_by: "worker-1",
        claimed_until: 1.minute.from_now
      )

      # Claimed but expired
      claimed_expired = described_class.create!(
        consumer_group: "claimed_expired_p0",
        topic: topic,
        last_consumed_sequence: 0,
        claimed_by: "worker-2",
        claimed_until: 1.minute.ago
      )

      results = described_class.unclaimed

      expect(results).to include(unclaimed)
      expect(results).to include(claimed_expired)
      expect(results).not_to include(claimed_active)
    end
  end

  describe "#try_claim!" do
    context "when partition is unclaimed" do
      it "successfully claims the partition" do
        result = offset.try_claim!(consumer_instance_id: "worker-1")

        expect(result).to be true
        offset.reload
        expect(offset.claimed_by).to eq("worker-1")
        expect(offset.claimed_until).to be > Time.current
      end

      it "sets claimed_until based on TTL" do
        freeze_time = Time.current
        allow(Time).to receive(:current).and_return(freeze_time)

        offset.try_claim!(consumer_instance_id: "worker-1", ttl: 30.seconds)

        expect(offset.reload.claimed_until).to be_within(1.second).of(freeze_time + 30.seconds)
      end

      it "uses default TTL when not specified" do
        freeze_time = Time.current
        allow(Time).to receive(:current).and_return(freeze_time)

        offset.try_claim!(consumer_instance_id: "worker-1")

        expect(offset.reload.claimed_until).to be_within(1.second).of(freeze_time + described_class::CLAIM_TTL)
      end
    end

    context "when partition is already claimed by another worker" do
      before do
        offset.update!(
          claimed_by: "worker-1",
          claimed_until: 1.minute.from_now
        )
      end

      it "fails to claim" do
        result = offset.try_claim!(consumer_instance_id: "worker-2")

        expect(result).to be false
        offset.reload
        expect(offset.claimed_by).to eq("worker-1") # unchanged
      end

      it "does not modify the existing claim" do
        original_until = offset.claimed_until

        offset.try_claim!(consumer_instance_id: "worker-2")

        offset.reload
        expect(offset.claimed_until).to eq(original_until)
      end
    end

    context "when claim has expired" do
      before do
        offset.update!(
          claimed_by: "dead-worker",
          claimed_until: 1.minute.ago
        )
      end

      it "allows new worker to claim" do
        result = offset.try_claim!(consumer_instance_id: "new-worker")

        expect(result).to be true
        offset.reload
        expect(offset.claimed_by).to eq("new-worker")
        expect(offset.claimed_until).to be > Time.current
      end
    end

    context "when same worker re-claims" do
      before do
        offset.update!(
          claimed_by: "worker-1",
          claimed_until: 30.seconds.from_now
        )
      end

      it "renews the claim" do
        original_until = offset.claimed_until

        result = offset.try_claim!(consumer_instance_id: "worker-1")

        expect(result).to be true
        offset.reload
        expect(offset.claimed_by).to eq("worker-1")
        expect(offset.claimed_until).to be > original_until
      end
    end
  end

  describe "#renew_claim!" do
    context "when holding the claim" do
      before do
        offset.update!(
          claimed_by: "worker-1",
          claimed_until: 10.seconds.from_now
        )
      end

      it "extends the claim TTL" do
        original_until = offset.claimed_until

        result = offset.renew_claim!(consumer_instance_id: "worker-1")

        expect(result).to be true
        offset.reload
        expect(offset.claimed_until).to be > original_until
      end

      it "uses custom TTL when specified" do
        freeze_time = Time.current
        allow(Time).to receive(:current).and_return(freeze_time)

        offset.renew_claim!(consumer_instance_id: "worker-1", ttl: 60.seconds)

        expect(offset.reload.claimed_until).to be_within(1.second).of(freeze_time + 60.seconds)
      end
    end

    context "when claim was stolen by another worker" do
      before do
        offset.update!(
          claimed_by: "worker-2",
          claimed_until: 1.minute.from_now
        )
      end

      it "fails to renew" do
        result = offset.renew_claim!(consumer_instance_id: "worker-1")

        expect(result).to be false
      end

      it "does not modify the thief's claim" do
        original_until = offset.claimed_until

        offset.renew_claim!(consumer_instance_id: "worker-1")

        offset.reload
        expect(offset.claimed_by).to eq("worker-2")
        expect(offset.claimed_until).to eq(original_until)
      end
    end

    context "when claim has expired" do
      before do
        offset.update!(
          claimed_by: "worker-1",
          claimed_until: 1.minute.ago
        )
      end

      it "renews expired claim when still owned by the same worker" do
        # Renewal only checks that claimed_by matches the caller; it does not consider
        # whether the existing claim has expired. Since claimed_by still matches, renewal
        # should succeed and extend claimed_until.
        result = offset.renew_claim!(consumer_instance_id: "worker-1")

        expect(result).to be true
        offset.reload
        expect(offset.claimed_until).to be > Time.current
      end
    end
  end

  describe "#release_claim!" do
    context "when holding the claim" do
      before do
        offset.update!(
          claimed_by: "worker-1",
          claimed_until: 1.minute.from_now
        )
      end

      it "releases the claim" do
        result = offset.release_claim!(consumer_instance_id: "worker-1")

        expect(result).to be true
        offset.reload
        expect(offset.claimed_by).to be_nil
        expect(offset.claimed_until).to be_nil
      end
    end

    context "when not holding the claim" do
      before do
        offset.update!(
          claimed_by: "worker-2",
          claimed_until: 1.minute.from_now
        )
      end

      it "does not release another worker's claim" do
        result = offset.release_claim!(consumer_instance_id: "worker-1")

        expect(result).to be false
        offset.reload
        expect(offset.claimed_by).to eq("worker-2")
      end
    end

    context "when partition is unclaimed" do
      it "returns false (nothing to release)" do
        result = offset.release_claim!(consumer_instance_id: "worker-1")

        expect(result).to be false
      end
    end
  end

  describe "#claimed?" do
    it "returns false when not claimed" do
      expect(offset.claimed?).to be false
    end

    it "returns true when actively claimed" do
      offset.update!(claimed_by: "worker-1", claimed_until: 1.minute.from_now)

      expect(offset.claimed?).to be true
    end

    it "returns false when claim has expired" do
      offset.update!(claimed_by: "worker-1", claimed_until: 1.minute.ago)

      expect(offset.claimed?).to be false
    end

    it "returns false when claimed_by is nil but claimed_until is set" do
      offset.update!(claimed_by: nil, claimed_until: 1.minute.from_now)

      expect(offset.claimed?).to be false
    end

    it "returns false when claimed_by is set but claimed_until is nil" do
      offset.update!(claimed_by: "worker-1", claimed_until: nil)

      expect(offset.claimed?).to be false
    end
  end

  describe "failover scenario" do
    it "allows new worker to claim after TTL expiration" do
      # Worker 1 claims partition
      offset.try_claim!(consumer_instance_id: "worker-1", ttl: 1.second)

      # First claim attempt by worker 2 should fail (claim still active)
      result1 = offset.try_claim!(consumer_instance_id: "worker-2")
      expect(result1).to be false

      # Wait for TTL to expire
      sleep 1.1

      # Second claim attempt should succeed
      result2 = offset.try_claim!(consumer_instance_id: "worker-2")
      expect(result2).to be true
      expect(offset.reload.claimed_by).to eq("worker-2")
    end
  end

  # Rails 7.1+ compatibility tests
  # In Rails 7.1+, with_lock raises error if record has unsaved changes.
  # These tests verify our reload + lock! pattern handles this correctly.
  describe "Rails 7.1+ compatibility (pending changes before locking)" do
    it "try_claim! works when record has pending changes" do
      # Simulate pending changes without saving
      offset.claimed_until = 1.hour.from_now
      offset.heartbeat_at = Time.current

      # Verify record is dirty
      expect(offset.changed?).to be true

      # Should NOT raise "Locking a record with unpersisted changes is not supported"
      result = offset.try_claim!(consumer_instance_id: "worker-1")

      expect(result).to be true
      expect(offset.reload.claimed_by).to eq("worker-1")
    end

    it "renew_claim! works when record has pending changes from previous claim" do
      # First, claim the partition
      offset.try_claim!(consumer_instance_id: "worker-1")

      # Simulate what happens in production:
      # The cached @partition_offset has in-memory state from try_claim!
      # but then someone modifies it or time passes
      offset.claimed_until = 1.hour.from_now  # Pending change

      # Verify record is dirty
      expect(offset.changed?).to be true

      # Should NOT raise "Locking a record with unpersisted changes is not supported"
      result = offset.renew_claim!(consumer_instance_id: "worker-1")

      expect(result).to be true
    end

    it "release_claim! works when record has pending changes" do
      # First, claim the partition
      offset.try_claim!(consumer_instance_id: "worker-1")

      # Add pending changes
      offset.heartbeat_at = Time.current

      # Verify record is dirty
      expect(offset.changed?).to be true

      # Should NOT raise error
      result = offset.release_claim!(consumer_instance_id: "worker-1")

      expect(result).to be true
      expect(offset.reload.claimed_by).to be_nil
    end

    it "handles multiple consecutive renew_claim! calls (heartbeat simulation)" do
      # Simulate the heartbeat loop that caused the production issue
      offset.try_claim!(consumer_instance_id: "worker-1")

      # First heartbeat - renew claim
      result1 = offset.renew_claim!(consumer_instance_id: "worker-1")
      expect(result1).to be true

      # Don't reload - keep using the same object (like production)
      # This leaves stale claimed_until in memory

      # Second heartbeat - should still work
      result2 = offset.renew_claim!(consumer_instance_id: "worker-1")
      expect(result2).to be true

      # Third heartbeat - should still work
      result3 = offset.renew_claim!(consumer_instance_id: "worker-1")
      expect(result3).to be true
    end
  end
end
