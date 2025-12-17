# frozen_string_literal: true

require "spec_helper"

RSpec.describe OutboxRelay::Supervisor do
  let(:configuration) { instance_double("OutboxRelay::Configuration") }
  subject(:supervisor) { described_class.new(configuration) }

  describe "#restart_fork" do
    let(:pid) { 123 }
    let(:partition_key) { 0 }
    let(:worker_config) { instance_double("WorkerConfig", topic: "test-topic") }
    let(:worker) { instance_double("OutboxRelay::Worker", name: "TestWorker") }
    let(:worker_key) { "#{worker_config.topic}-#{partition_key}" }
    let(:fixed_time) { Time.utc(2024, 1, 1, 0, 0, 0) }

    before do
      supervisor.forks[pid] = {
        worker: worker,
        partition_key: partition_key,
        started_at: fixed_time
      }
      supervisor.worker_configs[pid] = worker_config
      supervisor.instance_variable_get(:@restart_attempts)[worker_key] = 3
    end

    context "when worker exits because partition is already claimed" do
      let(:status) do
        instance_double(
          Process::Status,
          success?: false,
          signaled?: false,
          exitstatus: OutboxRelay::Processes::PartitionClaiming::CLAIM_UNAVAILABLE_EXIT_STATUS
        )
      end

      before do
        allow(Time).to receive(:current).and_return(fixed_time)
        allow(supervisor).to receive(:start_worker)
      end

      it "delays restart using claim retry delay instead of restarting immediately" do
        supervisor.send(:restart_fork, pid, status)

        backoff = supervisor.instance_variable_get(:@restart_backoff_until)[worker_key]
        expect(backoff[:worker_config]).to eq(worker_config)
        expect(backoff[:partition_key]).to eq(partition_key)
        expect(backoff[:restart_at]).to eq(
          fixed_time + OutboxRelay::Processes::PartitionClaiming::CLAIM_UNAVAILABLE_RETRY_DELAY
        )
        expect(supervisor.instance_variable_get(:@restart_attempts)).not_to have_key(worker_key)
        expect(supervisor).not_to have_received(:start_worker)
      end
    end
  end
end
