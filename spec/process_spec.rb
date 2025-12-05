# frozen_string_literal: true

require "spec_helper"

RSpec.describe OutboxRelay::Process do
  describe ".register" do
    it "creates a process record with metadata" do
      metadata = { workers_count: 4, uptime: 0 }

      process = described_class.register(
        kind: "supervisor",
        name: "test-supervisor",
        **metadata
      )

      expect(process).to be_persisted
      expect(process.kind).to eq("supervisor")
      expect(process.name).to eq("test-supervisor")
      expect(process.metadata["workers_count"]).to eq(4)
      expect(process.metadata["uptime"]).to eq(0)
    end
  end

  describe "#heartbeat" do
    let(:process) do
      described_class.register(
        kind: "supervisor",
        name: "test-supervisor",
        workers_count: 2,
        uptime: 0
      )
    end

    it "updates last_heartbeat_at" do
      initial_heartbeat = process.last_heartbeat_at

      sleep 0.01 # Ensure time difference

      process.heartbeat
      process.reload

      expect(process.last_heartbeat_at).to be > initial_heartbeat
    end

    it "updates metadata when provided" do
      initial_metadata = process.metadata.dup
      expect(initial_metadata["workers_count"]).to eq(2)

      # Simulate supervisor with 4 workers now
      updated_metadata = { workers_count: 4, uptime: 10.5 }

      process.heartbeat(metadata: updated_metadata)
      process.reload

      expect(process.metadata["workers_count"]).to eq(4)
      expect(process.metadata["uptime"]).to eq(10.5)
    end

    it "merges metadata with existing metadata" do
      # Initial metadata
      expect(process.metadata["workers_count"]).to eq(2)

      # Update only uptime
      process.heartbeat(metadata: { uptime: 15.0 })
      process.reload

      # workers_count should still be there
      expect(process.metadata["workers_count"]).to eq(2)
      expect(process.metadata["uptime"]).to eq(15.0)
    end

    it "does not update metadata when not provided" do
      initial_metadata = process.metadata.dup

      process.heartbeat
      process.reload

      expect(process.metadata).to eq(initial_metadata)
    end

    it "returns true on success" do
      expect(process.heartbeat).to be true
    end

    it "returns false when process is deregistered" do
      process.deregister

      expect(process.heartbeat).to be false
    end

    it "handles nil metadata gracefully" do
      # Manually set metadata to nil to simulate edge case
      process.update_column(:metadata, nil)
      process.reload

      expect(process.metadata).to be_nil

      # Should not crash even if metadata is nil
      expect {
        process.heartbeat(metadata: { test: "value" })
      }.not_to raise_error

      process.reload
      expect(process.metadata["test"]).to eq("value")
    end

    context "lock contention handling" do
      it "does not log for 1-2 consecutive lock failures (normal contention)" do
        # Force lock failures by stubbing lock! method
        allow(process).to receive(:lock!).and_raise(ActiveRecord::LockWaitTimeout, "NOWAIT lock failed")

        # First failure - should NOT log (consecutive_failures = 1)
        expect(OutboxRelay.logger).not_to receive(:debug)
        expect(OutboxRelay.logger).not_to receive(:warn)
        expect(process.heartbeat).to be false # Returns false when lock fails

        # Second failure - should NOT log (consecutive_failures = 2)
        expect(OutboxRelay.logger).not_to receive(:debug)
        expect(OutboxRelay.logger).not_to receive(:warn)
        expect(process.heartbeat).to be false
      end

      it "logs at DEBUG level for 3rd consecutive lock failure" do
        # Force 3 consecutive failures
        allow(process).to receive(:lock!).and_raise(ActiveRecord::LockWaitTimeout)

        # First two failures - no logs
        process.heartbeat
        process.heartbeat

        # Third failure - should log at DEBUG level
        expect(OutboxRelay.logger).to receive(:debug).with(hash_including(
          event_name: "heartbeat_lock_skipped",
          consecutive_failures: 3
        ))

        process.heartbeat
      end

      it "logs at WARN level for 4+ consecutive lock failures" do
        # Force 4 consecutive failures
        allow(process).to receive(:lock!).and_raise(ActiveRecord::LockWaitTimeout)

        # First 3 failures (first 2 silent, 3rd at DEBUG)
        process.heartbeat
        process.heartbeat
        allow(OutboxRelay.logger).to receive(:debug) # Allow 3rd one
        process.heartbeat

        # Fourth failure - should log at WARN level
        expect(OutboxRelay.logger).to receive(:warn).with(hash_including(
          event_name: "heartbeat_lock_skipped",
          consecutive_failures: 4
        ))

        process.heartbeat
      end

      it "resets consecutive failures counter on successful heartbeat" do
        # Force 2 failures
        allow(process).to receive(:lock!).and_raise(ActiveRecord::LockWaitTimeout)
        process.heartbeat
        process.heartbeat

        # Now allow success
        allow(process).to receive(:lock!).and_call_original

        # Successful heartbeat should reset counter
        expect(process.heartbeat).to be true

        # Next failure should be treated as first failure (no log)
        allow(process).to receive(:lock!).and_raise(ActiveRecord::LockWaitTimeout)
        expect(OutboxRelay.logger).not_to receive(:debug)
        expect(OutboxRelay.logger).not_to receive(:warn)
        process.heartbeat
      end
    end
  end

  describe "#deregister" do
    it "destroys the process record" do
      process = described_class.register(
        kind: "worker",
        name: "test-worker",
        topic: "users",
        partition_key: "0"
      )

      process_id = process.id

      expect { process.deregister }.to change { described_class.count }.by(-1)
      expect(described_class.find_by(id: process_id)).to be_nil
    end
  end
end
