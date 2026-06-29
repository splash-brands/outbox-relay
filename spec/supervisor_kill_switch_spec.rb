# frozen_string_literal: true

require "spec_helper"

RSpec.describe OutboxRelay::Supervisor do
  subject(:supervisor) { described_class.new(configuration) }

  let(:configuration) { instance_double("OutboxRelay::Configuration") }

  def capture_events(pattern = /^outbox_relay\.consumer_group\./)
    events = []
    subscriber = ActiveSupport::Notifications.subscribe(pattern) do |name, _start, _finish, _id, payload|
      events << { name: name, payload: payload }
    end
    yield
    events
  ensure
    ActiveSupport::Notifications.unsubscribe(subscriber) if subscriber
  end

  def worker_config_double(consumer_group:, partitions:)
    instance_double(
      "OutboxRelay::Configuration::WorkerConfig",
      consumer_group: consumer_group,
      topic: "#{consumer_group}_topic",
      partitions: partitions,
      partition_count: partitions.size
    )
  end

  describe "#start_workers" do
    let(:enabled_config) { worker_config_double(consumer_group: "enabled_cg", partitions: [0, 1]) }
    let(:disabled_config) { worker_config_double(consumer_group: "disabled_cg", partitions: [0]) }

    before do
      allow(configuration).to receive(:workers).and_return([enabled_config, disabled_config])
      allow(OutboxRelay::ConsumerControl).to receive(:disabled_consumer_groups).and_return(Set["disabled_cg"])
      allow(supervisor).to receive(:start_worker)
    end

    it "forks workers for enabled consumer groups only" do
      supervisor.send(:start_workers)

      expect(supervisor).to have_received(:start_worker).with(enabled_config, 0)
      expect(supervisor).to have_received(:start_worker).with(enabled_config, 1)
      expect(supervisor).not_to have_received(:start_worker).with(disabled_config, anything)
    end

    it "emits a disabled notification at boot for already-disabled groups" do
      events = capture_events { supervisor.send(:start_workers) }

      disabled_events = events.select { |e| e[:name] == "outbox_relay.consumer_group.disabled" }
      expect(disabled_events.size).to eq(1)
      expect(disabled_events.first[:payload]).to include(consumer_group: "disabled_cg", phase: "boot")
    end
  end

  describe "#start_worker kill-switch guard" do
    let(:config) { worker_config_double(consumer_group: "cg", partitions: [0]) }

    it "does not build or fork a worker when the consumer group is disabled" do
      supervisor.instance_variable_set(:@disabled_consumer_groups, Set["cg"])

      expect(OutboxRelay::Worker).not_to receive(:new)
      expect { supervisor.send(:start_worker, config, 0) }.not_to(change { supervisor.forks })
    end
  end

  describe "#enforce_consumer_controls" do
    context "when a consumer group becomes newly disabled" do
      let(:cg_a_config) { worker_config_double(consumer_group: "cg_a", partitions: [0]) }
      let(:cg_b_config) { worker_config_double(consumer_group: "cg_b", partitions: [0]) }

      before do
        supervisor.forks[100] = { worker: nil, partition_key: 0, started_at: Time.current }
        supervisor.forks[200] = { worker: nil, partition_key: 0, started_at: Time.current }
        supervisor.worker_configs[100] = cg_a_config
        supervisor.worker_configs[200] = cg_b_config

        allow(OutboxRelay::ConsumerControl).to receive(:disabled_consumer_groups).and_return(Set["cg_a"])
        allow(supervisor).to receive(:signal_processes).and_return([])
      end

      it "stops only the workers of the disabled group" do
        supervisor.send(:enforce_consumer_controls)

        expect(supervisor).to have_received(:signal_processes).with([100], :TERM)
      end

      it "records the disabled group so workers are not restarted" do
        supervisor.send(:enforce_consumer_controls)

        expect(supervisor.send(:consumer_group_disabled?, "cg_a")).to be(true)
        expect(supervisor.send(:consumer_group_disabled?, "cg_b")).to be(false)
      end

      it "emits a disabled transition notification" do
        events = capture_events { supervisor.send(:enforce_consumer_controls) }

        names = events.map { |e| e[:name] }
        expect(names).to include("outbox_relay.consumer_group.disabled")
        disabled = events.find { |e| e[:name] == "outbox_relay.consumer_group.disabled" }
        expect(disabled[:payload]).to include(consumer_group: "cg_a")
      end
    end

    context "when a consumer group becomes newly enabled" do
      let(:cg_a_config) { worker_config_double(consumer_group: "cg_a", partitions: [0, 1]) }

      before do
        supervisor.instance_variable_set(:@disabled_consumer_groups, Set["cg_a"])
        allow(configuration).to receive(:workers).and_return([cg_a_config])
        allow(OutboxRelay::ConsumerControl).to receive(:disabled_consumer_groups).and_return(Set.new)
        allow(supervisor).to receive(:start_worker)
      end

      it "restarts the workers of the re-enabled group" do
        supervisor.send(:enforce_consumer_controls)

        expect(supervisor).to have_received(:start_worker).with(cg_a_config, 0)
        expect(supervisor).to have_received(:start_worker).with(cg_a_config, 1)
      end

      it "emits an enabled transition notification" do
        events = capture_events { supervisor.send(:enforce_consumer_controls) }

        enabled = events.find { |e| e[:name] == "outbox_relay.consumer_group.enabled" }
        expect(enabled).not_to be_nil
        expect(enabled[:payload]).to include(consumer_group: "cg_a")
      end
    end

    context "when nothing changes" do
      before do
        supervisor.instance_variable_set(:@disabled_consumer_groups, Set["cg_a"])
        allow(OutboxRelay::ConsumerControl).to receive(:disabled_consumer_groups).and_return(Set["cg_a"])
        allow(supervisor).to receive(:signal_processes)
        allow(supervisor).to receive(:start_worker)
      end

      it "does not stop or start any workers" do
        supervisor.send(:enforce_consumer_controls)

        expect(supervisor).not_to have_received(:signal_processes)
        expect(supervisor).not_to have_received(:start_worker)
      end
    end
  end

  describe "#restart_fork kill-switch guard" do
    let(:pid) { 123 }
    let(:partition_key) { 0 }
    let(:consumer_group) { "test-consumer-group" }
    let(:worker_config) do
      instance_double("WorkerConfig", topic: "test-topic", consumer_group: consumer_group)
    end
    let(:worker) { instance_double("OutboxRelay::Worker", name: "TestWorker") }
    let(:worker_key) { "#{consumer_group}-test-topic-#{partition_key}" }
    let(:status) { instance_double(Process::Status, success?: true, signaled?: false, exitstatus: 0) }

    before do
      supervisor.forks[pid] = { worker: worker, partition_key: partition_key, started_at: Time.current }
      supervisor.worker_configs[pid] = worker_config
      supervisor.instance_variable_set(:@disabled_consumer_groups, Set[consumer_group])
      allow(supervisor).to receive(:start_worker)
    end

    it "does not restart a worker whose consumer group is disabled" do
      supervisor.send(:restart_fork, pid, status)

      expect(supervisor).not_to have_received(:start_worker)
      expect(supervisor.instance_variable_get(:@restart_backoff_until)).not_to have_key(worker_key)
    end
  end
end
