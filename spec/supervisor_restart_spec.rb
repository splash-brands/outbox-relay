# frozen_string_literal: true

require 'spec_helper'

RSpec.describe OutboxRelay::Supervisor do
  let(:configuration) { instance_double('OutboxRelay::Configuration') }
  subject(:supervisor) { described_class.new(configuration) }

  describe '#restart_fork' do
    let(:pid) { 123 }
    let(:partition_key) { 0 }
    let(:consumer_group) { 'test-consumer-group' }
    let(:worker_config) { instance_double('WorkerConfig', topic: 'test-topic', consumer_group: consumer_group) }
    let(:worker) { instance_double('OutboxRelay::Worker', name: 'TestWorker') }
    let(:worker_key) { "#{consumer_group}-#{worker_config.topic}-#{partition_key}" }
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

    context 'when worker exits because partition is already claimed' do
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

      it 'delays restart using claim retry delay instead of restarting immediately' do
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

    context 'when multiple consumer groups consume the same topic' do
      # This test verifies the fix for orphaned partition bug where worker_key
      # was based only on topic-partition, causing collisions when multiple
      # consumer groups (e.g., shipstation_order_fulfillment and monday_order_lifecycle)
      # both consume the same topic (order_lifecycle).
      let(:pid_group_a) { 100 }
      let(:pid_group_b) { 101 }
      let(:shared_topic) { 'order_lifecycle' }
      let(:shared_partition) { 0 }

      let(:consumer_group_a) { 'shipstation_order_fulfillment' }
      let(:consumer_group_b) { 'monday_order_lifecycle' }

      let(:worker_config_a) do
        instance_double('WorkerConfig', topic: shared_topic, consumer_group: consumer_group_a)
      end
      let(:worker_config_b) do
        instance_double('WorkerConfig', topic: shared_topic, consumer_group: consumer_group_b)
      end

      let(:worker_a) { instance_double('OutboxRelay::Worker', name: 'WorkerA') }
      let(:worker_b) { instance_double('OutboxRelay::Worker', name: 'WorkerB') }

      let(:worker_key_a) { "#{consumer_group_a}-#{shared_topic}-#{shared_partition}" }
      let(:worker_key_b) { "#{consumer_group_b}-#{shared_topic}-#{shared_partition}" }

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

        # Setup both workers
        supervisor.forks[pid_group_a] = {
          worker: worker_a,
          partition_key: shared_partition,
          started_at: fixed_time
        }
        supervisor.forks[pid_group_b] = {
          worker: worker_b,
          partition_key: shared_partition,
          started_at: fixed_time
        }
        supervisor.worker_configs[pid_group_a] = worker_config_a
        supervisor.worker_configs[pid_group_b] = worker_config_b
      end

      it 'tracks backoff separately for each consumer group' do
        # Both workers fail to claim the same partition on the same topic
        supervisor.send(:restart_fork, pid_group_a, status)
        supervisor.send(:restart_fork, pid_group_b, status)

        backoff_hash = supervisor.instance_variable_get(:@restart_backoff_until)

        # Both should be tracked separately (not overwriting each other)
        expect(backoff_hash.keys).to contain_exactly(worker_key_a, worker_key_b)

        # Each backoff should reference its own worker_config
        expect(backoff_hash[worker_key_a][:worker_config]).to eq(worker_config_a)
        expect(backoff_hash[worker_key_b][:worker_config]).to eq(worker_config_b)
      end

      it 'includes consumer_group in worker_key to prevent collisions' do
        # Verify the worker keys are different even for same topic/partition
        expect(worker_key_a).not_to eq(worker_key_b)
        expect(worker_key_a).to eq('shipstation_order_fulfillment-order_lifecycle-0')
        expect(worker_key_b).to eq('monday_order_lifecycle-order_lifecycle-0')
      end
    end
  end
end
