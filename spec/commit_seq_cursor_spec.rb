# frozen_string_literal: true

require 'spec_helper'
require 'outbox_relay/models/outbox_consumer'

# Regression coverage for SB-2140 — the consumer cursor must follow `commit_seq`
# (assigned at the COMMIT edge), NOT `sequence` (assigned at INSERT). The
# production guarantee (commit_seq order == commit order == visibility order) is
# enforced by a PostgreSQL DEFERRABLE trigger and verified manually against
# Postgres/Aurora; here we verify the Ruby layer reads/orders/advances by
# commit_seq even when it diverges from sequence.
RSpec.describe 'commit_seq consumer cursor (SB-2140)' do
  class CollectingConsumer < OutboxRelay::OutboxConsumer
    attr_reader :consumed

    def initialize(partition_key: 0, auto_offset_reset: :earliest)
      super(
        consumer_group: 'commit_seq_cursor_consumer',
        topic: 'commit_seq_topic',
        partition_key: partition_key,
        auto_offset_reset: auto_offset_reset
      )
      @consumed = []
    end

    def consume_message(event)
      @consumed << event
    end
  end

  let(:consumer) { CollectingConsumer.new }

  # Advisory locks use pg_try_advisory_lock, which SQLite lacks. Stub them as the
  # other consumer specs do — they are orthogonal to cursor ordering.
  before do
    allow_any_instance_of(CollectingConsumer).to receive(:acquire_advisory_lock).and_return(true)
    allow_any_instance_of(CollectingConsumer).to receive(:release_advisory_lock).and_return(true)
  end

  # The race shape: the long transaction is assigned a LOWER sequence first but
  # COMMITS LATER, so it gets the HIGHER commit_seq. The short transaction is
  # assigned a HIGHER sequence but commits first, getting the LOWER commit_seq.
  def create_event(sequence:, commit_seq:, event_name: 'evt')
    OutboxRelay::OutboxEvent.create!(
      topic: 'commit_seq_topic',
      event_name: event_name,
      partition_key: 0,
      payload: {},
      headers: {},
      sequence: sequence,
      commit_seq: commit_seq
    )
  end

  describe '#fetch_batch' do
    it 'orders by commit_seq, not sequence' do
      long_txn  = create_event(sequence: 100, commit_seq: 2) # low seq, committed late
      short_txn = create_event(sequence: 101, commit_seq: 1) # high seq, committed first

      events = consumer.send(:fetch_batch, 10)

      expect(events.map(&:id)).to eq([short_txn.id, long_txn.id])
    end
  end

  describe 'end-to-end delivery (the SB-2140 lost-event scenario)' do
    it 'delivers the low-sequence/late-commit event instead of skipping it' do
      long_txn  = create_event(sequence: 100, commit_seq: 2)
      short_txn = create_event(sequence: 101, commit_seq: 1)

      consumer.consume_all

      # Both delivered, in commit_seq order. Under the old sequence cursor the
      # offset would have advanced to 101 and event 100 (commit_seq 2) would have
      # been silently skipped.
      expect(consumer.consumed.map(&:id)).to eq([short_txn.id, long_txn.id])
    end
  end

  describe 'offset semantics' do
    it 'advances the offset by commit_seq' do
      create_event(sequence: 101, commit_seq: 1)
      create_event(sequence: 100, commit_seq: 2)

      consumer.consume_all

      offset = OutboxRelay::ConsumerOffset.find_by(
        consumer_group: 'commit_seq_cursor_consumer_p0',
        topic: 'commit_seq_topic'
      )
      expect(offset.last_consumed_sequence).to eq(2) # max commit_seq, not max sequence (101)
    end

    it 'skips events whose commit_seq is at or below the current offset' do
      # Offset already past commit_seq 5, regardless of sequence values.
      OutboxRelay::ConsumerOffset.create!(
        consumer_group: 'commit_seq_cursor_consumer_p0',
        topic: 'commit_seq_topic',
        last_consumed_sequence: 5
      )
      create_event(sequence: 1, commit_seq: 3) # below offset → must be skipped
      fresh = create_event(sequence: 2, commit_seq: 9) # above offset → delivered

      consumer = CollectingConsumer.new(auto_offset_reset: :earliest)
      consumer.consume_all

      expect(consumer.consumed.map(&:id)).to eq([fresh.id])
    end
  end

  describe ':latest offset reset' do
    it 'seeds from the maximum commit_seq' do
      create_event(sequence: 500, commit_seq: 7)

      offset = OutboxRelay::ConsumerOffset.find_or_initialize_for(
        consumer_group: 'new_group',
        topic: 'commit_seq_topic'
      )

      expect(offset.last_consumed_sequence).to eq(7) # max commit_seq, not max sequence (500)
    end
  end
end
