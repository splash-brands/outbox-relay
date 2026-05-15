# frozen_string_literal: true

require 'spec_helper'

RSpec.describe OutboxRelay::PartitionMonitor do
  let(:topic) { 'test-topic' }
  let(:consumer_group) { 'test-group' }
  let(:configuration) { instance_double(OutboxRelay::Configuration) }
  let(:worker_config) do
    instance_double(
      OutboxRelay::Configuration::WorkerConfig,
      consumer_group: consumer_group,
      topic: topic,
      partitions: [0, 1, 2, 3]
    )
  end

  before do
    allow(configuration).to receive(:workers).and_return([worker_config])
    allow(configuration).to receive(:lag_alert_threshold).and_return(100)
    allow(configuration).to receive(:stale_worker_timeout).and_return(60)
  end

  subject(:monitor) { described_class.new(configuration) }

  describe '#orphaned_partitions' do
    before do
      # Create offsets - some claimed, some not
      OutboxRelay::ConsumerOffset.create!(
        consumer_group: "#{consumer_group}_p0",
        topic: topic,
        last_consumed_sequence: 100,
        claimed_by: 'worker-1',
        claimed_until: 1.minute.from_now
      )
      OutboxRelay::ConsumerOffset.create!(
        consumer_group: "#{consumer_group}_p1",
        topic: topic,
        last_consumed_sequence: 100,
        claimed_by: nil,
        claimed_until: nil
      )
      OutboxRelay::ConsumerOffset.create!(
        consumer_group: "#{consumer_group}_p2",
        topic: topic,
        last_consumed_sequence: 100,
        claimed_by: 'dead-worker',
        claimed_until: 1.hour.ago # Expired claim
      )
    end

    it 'returns partitions without active claims' do
      orphaned = monitor.orphaned_partitions

      expect(orphaned.size).to eq(2)
      expect(orphaned.map { |o| o[:partition_key] }).to contain_exactly(1, 2)
    end

    it 'includes consumer_group, topic, and partition_key' do
      orphaned = monitor.orphaned_partitions.first

      expect(orphaned).to include(
        consumer_group: consumer_group,
        topic: topic
      )
      expect(orphaned[:partition_key]).to be_a(Integer)
    end

    it 'includes claimed_until timestamp' do
      orphaned = monitor.orphaned_partitions

      expired_partition = orphaned.find { |o| o[:partition_key] == 2 }
      expect(expired_partition[:claimed_until]).to be_present
      expect(expired_partition[:claimed_until]).to be < Time.current

      never_claimed = orphaned.find { |o| o[:partition_key] == 1 }
      expect(never_claimed[:claimed_until]).to be_nil
    end
  end

  describe '#partition_health' do
    before do
      # Create claimed partition with recent heartbeat
      OutboxRelay::ConsumerOffset.create!(
        consumer_group: "#{consumer_group}_p0",
        topic: topic,
        last_consumed_sequence: 100,
        claimed_by: 'worker-1',
        claimed_until: 1.minute.from_now,
        heartbeat_at: 10.seconds.ago
      )

      # Create claimed partition with stale heartbeat
      OutboxRelay::ConsumerOffset.create!(
        consumer_group: "#{consumer_group}_p1",
        topic: topic,
        last_consumed_sequence: 100,
        claimed_by: 'worker-2',
        claimed_until: 1.minute.from_now,
        heartbeat_at: 2.minutes.ago # Stale
      )

      # Create orphaned partition (no claim)
      OutboxRelay::ConsumerOffset.create!(
        consumer_group: "#{consumer_group}_p2",
        topic: topic,
        last_consumed_sequence: 100,
        claimed_by: nil,
        claimed_until: nil
      )

      # Partition 3 has no offset record at all (also orphaned)
    end

    it 'returns status for all expected partitions' do
      health = monitor.partition_health

      expect(health.size).to eq(4)
      expect(health.map { |h| h[:partition_key] }).to contain_exactly(0, 1, 2, 3)
    end

    it 'marks active partitions correctly' do
      health = monitor.partition_health
      partition_0 = health.find { |h| h[:partition_key].zero? }

      expect(partition_0[:status]).to eq(:active)
      expect(partition_0[:claimed_by]).to eq('worker-1')
    end

    it 'marks stale partitions correctly' do
      health = monitor.partition_health
      partition_1 = health.find { |h| h[:partition_key] == 1 }

      expect(partition_1[:status]).to eq(:stale)
    end

    it 'marks orphaned partitions correctly' do
      health = monitor.partition_health
      partition_2 = health.find { |h| h[:partition_key] == 2 }
      partition_3 = health.find { |h| h[:partition_key] == 3 }

      expect(partition_2[:status]).to eq(:orphaned)
      expect(partition_3[:status]).to eq(:orphaned)
    end

    it 'includes lag for each partition' do
      # Create events to generate lag
      OutboxRelay::OutboxEvent.create!(
        topic: topic,
        event_name: 'test.event',
        payload: {},
        partition_key: 0,
        sequence: 150
      )

      health = monitor.partition_health
      partition_0 = health.find { |h| h[:partition_key].zero? }

      expect(partition_0[:lag]).to eq(1) # 1 event with sequence > 100
    end
  end

  describe '#health_report' do
    before do
      # Active partition
      OutboxRelay::ConsumerOffset.create!(
        consumer_group: "#{consumer_group}_p0",
        topic: topic,
        last_consumed_sequence: 100,
        claimed_by: 'worker-1',
        claimed_until: 1.minute.from_now,
        heartbeat_at: 10.seconds.ago
      )

      # Stale partition
      OutboxRelay::ConsumerOffset.create!(
        consumer_group: "#{consumer_group}_p1",
        topic: topic,
        last_consumed_sequence: 100,
        claimed_by: 'worker-2',
        claimed_until: 1.minute.from_now,
        heartbeat_at: 2.minutes.ago
      )

      # Orphaned partitions (2 and 3 have no claim)
      OutboxRelay::ConsumerOffset.create!(
        consumer_group: "#{consumer_group}_p2",
        topic: topic,
        last_consumed_sequence: 0,
        claimed_by: nil,
        claimed_until: nil
      )

      # Create enough events on partition 2 to exceed lag threshold (default: 100)
      101.times do |_i|
        OutboxRelay::OutboxEvent.create!(
          topic: topic,
          event_name: 'test.event',
          payload: {},
          partition_key: 2,
          sequence: OutboxRelay::OutboxEvent.next_sequence
        )
      end
    end

    it 'returns total partition count' do
      report = monitor.health_report

      expect(report[:total]).to eq(4)
    end

    it 'returns active partition count' do
      report = monitor.health_report

      expect(report[:active]).to eq(1)
    end

    it 'returns stale partition count' do
      report = monitor.health_report

      expect(report[:stale]).to eq(1)
    end

    it 'returns orphaned partitions array' do
      report = monitor.health_report

      expect(report[:orphaned]).to be_an(Array)
      expect(report[:orphaned].size).to eq(2)
      expect(report[:orphaned].map { |o| o[:partition_key] }).to contain_exactly(2, 3)
    end

    it 'returns high_lag partitions array' do
      report = monitor.health_report

      expect(report[:high_lag]).to be_an(Array)
      expect(report[:high_lag].any? { |p| p[:lag] > 100 }).to be true
    end

    it 'includes timestamp' do
      report = monitor.health_report

      expect(report[:timestamp]).to be_present
      expect { Time.iso8601(report[:timestamp]) }.not_to raise_error
    end
  end

  describe '#partition_lag' do
    before do
      OutboxRelay::ConsumerOffset.create!(
        consumer_group: "#{consumer_group}_p0",
        topic: topic,
        last_consumed_sequence: 100
      )

      OutboxRelay::OutboxEvent.create!(
        topic: topic,
        event_name: 'test.event',
        payload: {},
        partition_key: 0,
        sequence: 250
      )
    end

    it 'returns lag for specific partition' do
      lag = monitor.partition_lag(
        consumer_group: consumer_group,
        topic: topic,
        partition_key: 0
      )

      expect(lag).to eq(1) # 1 event with sequence > 100
    end

    it 'returns 0 for non-existent partition' do
      lag = monitor.partition_lag(
        consumer_group: consumer_group,
        topic: topic,
        partition_key: 99
      )

      expect(lag).to eq(0)
    end
  end

  describe '#stale_consumer_offsets' do
    before do
      # In-config: should never appear in stale list
      OutboxRelay::ConsumerOffset.create!(
        consumer_group: "#{consumer_group}_p0",
        topic: topic,
        last_consumed_sequence: 100,
        heartbeat_at: 1.minute.ago
      )

      # Stale: consumer_group is NOT in configuration, idle for days
      OutboxRelay::ConsumerOffset.create!(
        consumer_group: 'retired_group_p0',
        topic: topic,
        last_consumed_sequence: 50,
        heartbeat_at: 30.days.ago,
        last_consumed_at: 30.days.ago
      )
      OutboxRelay::ConsumerOffset.create!(
        consumer_group: 'retired_group_p1',
        topic: topic,
        last_consumed_sequence: 51,
        heartbeat_at: nil,
        last_consumed_at: 30.days.ago
      )

      # Stale but recently active: heartbeat is fresh
      OutboxRelay::ConsumerOffset.create!(
        consumer_group: 'just_removed_group_p0',
        topic: topic,
        last_consumed_sequence: 60,
        heartbeat_at: 30.seconds.ago
      )

      # Stale but still claimed (worker draining)
      OutboxRelay::ConsumerOffset.create!(
        consumer_group: 'draining_group_p0',
        topic: topic,
        last_consumed_sequence: 70,
        heartbeat_at: 30.days.ago,
        claimed_by: 'worker-x',
        claimed_until: 1.minute.from_now
      )
    end

    it 'returns offsets whose base consumer_group is not in configuration' do
      results = monitor.stale_consumer_offsets

      groups = results.map { |r| r[:consumer_group] }.uniq
      expect(groups).to contain_exactly('retired_group', 'just_removed_group')
      # `draining_group` excluded because claimed; in-config group excluded by name.
    end

    it 'includes diagnostic metadata for each stale offset' do
      results = monitor.stale_consumer_offsets

      retired_p0 = results.find { |r| r[:full_consumer_group] == 'retired_group_p0' }
      expect(retired_p0).to include(
        consumer_group: 'retired_group',
        topic: topic,
        partition_key: 0,
        last_consumed_sequence: 50
      )
      expect(retired_p0[:id]).to be_a(Integer)
      expect(retired_p0[:last_consumed_at]).to be_present
      expect(retired_p0[:heartbeat_at]).to be_present
    end

    context 'with :idle_for filter' do
      it 'excludes offsets whose heartbeat is newer than the cutoff' do
        results = monitor.stale_consumer_offsets(idle_for: 7.days)

        groups = results.map { |r| r[:consumer_group] }.uniq
        expect(groups).to contain_exactly('retired_group')
        # `just_removed_group` heartbeat (30s ago) is inside the 7-day window.
      end

      it 'still includes offsets with NULL heartbeat regardless of cutoff' do
        # NULL heartbeat means "never alive in current schema" — always eligible
        # so a forever-idle offset can be pruned.
        results = monitor.stale_consumer_offsets(idle_for: 7.days)

        full_groups = results.map { |r| r[:full_consumer_group] }
        expect(full_groups).to include('retired_group_p1') # heartbeat_at is nil
      end
    end

    context 'with exclude_claimed: false' do
      it 'includes claimed stale offsets' do
        results = monitor.stale_consumer_offsets(exclude_claimed: false)

        full_groups = results.map { |r| r[:full_consumer_group] }
        expect(full_groups).to include('draining_group_p0')
      end
    end
  end
end
