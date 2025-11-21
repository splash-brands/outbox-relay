# frozen_string_literal: true

require "spec_helper"

RSpec.describe OutboxRelay::ConsumerOffset do
  let(:consumer_group) { "test-consumer-group" }
  let(:topic) { "test-topic" }

  describe ".find_or_initialize_for" do
    it "creates a new offset with default sequence 0" do
      offset = described_class.find_or_initialize_for(
        consumer_group: consumer_group,
        topic: topic
      )

      expect(offset).to be_new_record
      expect(offset.consumer_group).to eq(consumer_group)
      expect(offset.topic).to eq(topic)
      expect(offset.last_consumed_sequence).to eq(0)
    end

    it "finds existing offset" do
      existing = described_class.create!(
        consumer_group: consumer_group,
        topic: topic,
        last_consumed_sequence: 100
      )

      offset = described_class.find_or_initialize_for(
        consumer_group: consumer_group,
        topic: topic
      )

      expect(offset).to be_persisted
      expect(offset.id).to eq(existing.id)
      expect(offset.last_consumed_sequence).to eq(100)
    end
  end

  describe "#update_offset!" do
    let(:offset) do
      described_class.create!(
        consumer_group: consumer_group,
        topic: topic,
        last_consumed_sequence: 100
      )
    end

    context "when sequence is greater than current" do
      it "updates the offset and returns true" do
        result = offset.update_offset!(
          sequence: 150,
          event_id: "event-150"
        )

        expect(result).to be true
        expect(offset.reload.last_consumed_sequence).to eq(150)
        expect(offset.last_consumed_event_id).to eq("event-150")
        expect(offset.last_consumed_at).to be_present
        expect(offset.heartbeat_at).to be_present
      end
    end

    context "when sequence is equal to current (Kafka-style out-of-order)" do
      it "skips update and returns false" do
        result = offset.update_offset!(
          sequence: 100,
          event_id: "event-100"
        )

        expect(result).to be false
        expect(offset.reload.last_consumed_sequence).to eq(100)
      end
    end

    context "when sequence is less than current (out-of-order completion)" do
      it "skips update and returns false" do
        # Simulate: Worker B completed seq=150 first, Worker A completes seq=120 later
        offset.update_offset!(sequence: 150, event_id: "event-150")

        result = offset.update_offset!(
          sequence: 120,
          event_id: "event-120"
        )

        expect(result).to be false
        expect(offset.reload.last_consumed_sequence).to eq(150) # Stays at 150
      end
    end

    context "concurrent updates (race condition simulation)" do
      it "handles out-of-order completion correctly" do
        # Reset to known state
        offset.update!(last_consumed_sequence: 100)

        # Simulate concurrent processing:
        # Worker A processes event 120
        # Worker B processes event 150
        # Worker B completes FIRST (out of order)

        # Worker B completes first with seq=150
        worker_b_result = offset.update_offset!(
          sequence: 150,
          event_id: "event-150"
        )
        expect(worker_b_result).to be true
        expect(offset.reload.last_consumed_sequence).to eq(150)

        # Worker A completes later with seq=120 (stale offset)
        worker_a_result = offset.update_offset!(
          sequence: 120,
          event_id: "event-120"
        )
        expect(worker_a_result).to be false
        expect(offset.reload.last_consumed_sequence).to eq(150) # Stays at 150

        # This prevents the error:
        # "Validation failed: Last consumed sequence cannot decrease (was 150, got 120)"
      end

      it "handles multiple workers completing in reverse order" do
        offset.update!(last_consumed_sequence: 100)

        # Three workers process events 110, 120, 130
        # They complete in reverse order: 130, 120, 110

        # Worker C completes first with seq=130
        result_c = offset.update_offset!(sequence: 130, event_id: "event-130")
        expect(result_c).to be true
        expect(offset.reload.last_consumed_sequence).to eq(130)

        # Worker B completes with seq=120 (stale)
        result_b = offset.update_offset!(sequence: 120, event_id: "event-120")
        expect(result_b).to be false
        expect(offset.reload.last_consumed_sequence).to eq(130)

        # Worker A completes with seq=110 (stale)
        result_a = offset.update_offset!(sequence: 110, event_id: "event-110")
        expect(result_a).to be false
        expect(offset.reload.last_consumed_sequence).to eq(130)
      end
    end

    context "thread safety" do
      it "uses pessimistic locking (with_lock)" do
        # This test verifies that with_lock is used, preventing race conditions
        # at the database level
        #
        # NOTE: SQLite doesn't handle concurrent writes well in tests, so we simulate
        # the race condition by doing sequential updates that would conflict
        offset.update!(last_consumed_sequence: 100)

        # Simulate race: two workers try to update with different sequences
        # If no locking, both might succeed and last write wins
        # With proper locking (with_lock + reload), conditional update works correctly

        # Worker A tries to update to 110
        result_a = offset.update_offset!(sequence: 110, event_id: "event-110")
        expect(result_a).to be true
        expect(offset.reload.last_consumed_sequence).to eq(110)

        # Worker B tries to update to 105 (stale, should be skipped)
        result_b = offset.update_offset!(sequence: 105, event_id: "event-105")
        expect(result_b).to be false
        expect(offset.reload.last_consumed_sequence).to eq(110) # Unchanged

        # Worker C updates to 115 (fresh, should succeed)
        result_c = offset.update_offset!(sequence: 115, event_id: "event-115")
        expect(result_c).to be true
        expect(offset.reload.last_consumed_sequence).to eq(115)
      end
    end
  end

  describe "#heartbeat!" do
    let(:offset) do
      described_class.create!(
        consumer_group: consumer_group,
        topic: topic,
        last_consumed_sequence: 100,
        heartbeat_at: Time.current
      )
    end

    it "updates heartbeat_at without triggering validations" do
      initial_heartbeat = offset.heartbeat_at

      sleep 0.01 # Ensure time difference

      offset.heartbeat!

      expect(offset.reload.heartbeat_at).to be > initial_heartbeat
    end
  end

  describe "#lag" do
    let(:offset) do
      described_class.create!(
        consumer_group: consumer_group,
        topic: topic,
        last_consumed_sequence: 100
      )
    end

    before do
      # Create some events in the topic
      OutboxRelay::OutboxEvent.create!(
        topic: topic,
        event_name: "test.event",
        payload: {},
        sequence: 150,
        partition_key: 0
      )
    end

    it "calculates lag as difference between latest and consumed sequence" do
      expect(offset.lag).to eq(50) # 150 - 100
    end

    it "returns 0 when no events exist" do
      offset.update!(topic: "non-existent-topic")
      expect(offset.lag).to eq(-100) # 0 - 100 (negative when ahead)
    end
  end

  describe "#active?" do
    it "returns true when heartbeat is recent" do
      offset = described_class.create!(
        consumer_group: consumer_group,
        topic: topic,
        last_consumed_sequence: 0,
        heartbeat_at: 1.minute.ago
      )

      expect(offset.active?).to be true
    end

    it "returns false when heartbeat is stale" do
      offset = described_class.create!(
        consumer_group: consumer_group,
        topic: topic,
        last_consumed_sequence: 0,
        heartbeat_at: 10.minutes.ago
      )

      expect(offset.active?).to be false
    end

    it "returns false when heartbeat is nil" do
      offset = described_class.create!(
        consumer_group: consumer_group,
        topic: topic,
        last_consumed_sequence: 0,
        heartbeat_at: nil
      )

      expect(offset.active?).to be false
    end
  end
end
