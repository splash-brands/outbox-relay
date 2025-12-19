# frozen_string_literal: true

require "spec_helper"
require "outbox_relay/models/outbox_consumer"

RSpec.describe "OutboxConsumer retriable exceptions" do
  # Custom exception for testing (simulates Prop::RateLimited)
  class TestRateLimitedError < StandardError
    attr_reader :retry_after

    def initialize(message = "Rate limited", retry_after: 60)
      super(message)
      @retry_after = retry_after
    end
  end

  class TestTransientError < StandardError
    def initialize(message = "Transient error")
      super(message)
    end
  end

  # Test consumer that marks rate limiting as retriable
  class TestRetriableConsumer < OutboxRelay::OutboxConsumer
    attr_accessor :call_count, :should_fail, :fail_count, :fail_with

    def initialize(partition_key: 0)
      super(
        consumer_group: "test_retriable_consumer",
        topic: "test_topic",
        partition_key: partition_key,
        dead_letter_config: { max_retries: 3 }
      )
      @call_count = 0
      @should_fail = false
      @fail_count = 0
      @fail_with = TestRateLimitedError
    end

    def consume_message(_event)
      @call_count += 1

      if @should_fail && @call_count <= @fail_count
        if @fail_with == TestRateLimitedError
          raise @fail_with.new("Test rate limit", retry_after: 0.01)
        else
          raise @fail_with.new("Test error")
        end
      end

      true
    end

    def retriable_exception?(exception)
      exception.is_a?(TestRateLimitedError)
    end

    def retry_delay_for(exception)
      exception.respond_to?(:retry_after) ? exception.retry_after : 0.01
    end

    def max_retriable_attempts
      3
    end
  end

  # Test consumer that does NOT use retriable exceptions
  class TestNonRetriableConsumer < OutboxRelay::OutboxConsumer
    attr_accessor :call_count

    def initialize(partition_key: 0)
      super(
        consumer_group: "test_non_retriable_consumer",
        topic: "test_topic",
        partition_key: partition_key,
        dead_letter_config: { max_retries: 3 }
      )
      @call_count = 0
    end

    def consume_message(_event)
      @call_count += 1
      raise TestRateLimitedError.new("Test rate limit", retry_after: 0.01)
    end

    # Default: retriable_exception? returns false
  end

  let(:event) do
    OutboxRelay::OutboxEvent.create!(
      topic: "test_topic",
      event_name: "test_event",
      event_id: SecureRandom.uuid,
      partition_key: 0,
      payload: { "data" => "test" },
      headers: {},
      sequence: 1
    )
  end

  before do
    # Setup offset tracking
    OutboxRelay::ConsumerOffset.find_or_create_by!(
      consumer_group: "test_retriable_consumer_p0",
      topic: "test_topic"
    ).update!(last_consumed_sequence: 0)

    OutboxRelay::ConsumerOffset.find_or_create_by!(
      consumer_group: "test_non_retriable_consumer_p0",
      topic: "test_topic"
    ).update!(last_consumed_sequence: 0)
  end

  describe "#retriable_exception?" do
    it "returns false by default" do
      consumer = OutboxRelay::OutboxConsumer.allocate
      consumer.instance_variable_set(:@consumer_group, "test")

      expect(consumer.send(:retriable_exception?, StandardError.new)).to be false
    end

    it "can be overridden in subclass" do
      consumer = TestRetriableConsumer.new

      expect(consumer.send(:retriable_exception?, TestRateLimitedError.new)).to be true
      expect(consumer.send(:retriable_exception?, StandardError.new)).to be false
    end
  end

  describe "#retry_delay_for" do
    it "uses retry_after if available" do
      consumer = TestRetriableConsumer.new
      error = TestRateLimitedError.new("test", retry_after: 30)

      expect(consumer.send(:retry_delay_for, error)).to eq(30)
    end

    it "defaults to 60 seconds if retry_after not available" do
      consumer = OutboxRelay::OutboxConsumer.allocate
      consumer.instance_variable_set(:@consumer_group, "test")

      expect(consumer.send(:retry_delay_for, StandardError.new("test"))).to eq(60)
    end

    it "caps delay at 300 seconds" do
      consumer = OutboxRelay::OutboxConsumer.allocate
      consumer.instance_variable_set(:@consumer_group, "test")

      error = TestRateLimitedError.new("test", retry_after: 600)

      expect(consumer.send(:retry_delay_for, error)).to eq(300)
    end
  end

  describe "#max_retriable_attempts" do
    it "defaults to 5" do
      consumer = OutboxRelay::OutboxConsumer.allocate
      consumer.instance_variable_set(:@consumer_group, "test")

      expect(consumer.send(:max_retriable_attempts)).to eq(5)
    end

    it "can be overridden in subclass" do
      consumer = TestRetriableConsumer.new

      expect(consumer.send(:max_retriable_attempts)).to eq(3)
    end
  end

  describe "#process_event with retriable exceptions" do
    # Stub advisory lock methods since tests use SQLite (no pg_try_advisory_lock)
    # Stub handle_event_failure since SQLite doesn't have the right schema for DLQ
    before do
      allow_any_instance_of(OutboxRelay::OutboxConsumer).to receive(:acquire_advisory_lock).and_return(true)
      allow_any_instance_of(OutboxRelay::OutboxConsumer).to receive(:release_advisory_lock).and_return(true)
      allow_any_instance_of(OutboxRelay::OutboxConsumer).to receive(:handle_event_failure)
    end

    context "when exception is retriable" do
      it "retries and succeeds after transient failure" do
        consumer = TestRetriableConsumer.new
        consumer.should_fail = true
        consumer.fail_count = 2  # Fail first 2 times, succeed on 3rd

        result = consumer.send(:process_event, event)

        expect(result).to be true
        expect(consumer.call_count).to eq(3)  # 2 failures + 1 success
      end

      it "logs retriable exception with attempt count" do
        consumer = TestRetriableConsumer.new
        consumer.should_fail = true
        consumer.fail_count = 1  # Fail once, succeed on 2nd

        expect(OutboxRelay.logger).to receive(:info).with(
          hash_including(
            event_name: "retriable_exception_waiting",
            attempt: 1,
            max_attempts: 3
          )
        )

        consumer.send(:process_event, event)
      end

      it "goes to DLQ after max_retriable_attempts exceeded" do
        consumer = TestRetriableConsumer.new
        consumer.should_fail = true
        consumer.fail_count = 10  # Always fail

        expect(OutboxRelay.logger).to receive(:info).exactly(3).times  # 3 retry logs
        expect(OutboxRelay.logger).to receive(:error).with(
          hash_including(
            event_name: "consume_message_failed",
            retriable_attempts_exhausted: true
          )
        )

        expect { consumer.send(:process_event, event) }.to raise_error(TestRateLimitedError)
        expect(consumer.call_count).to eq(4)  # 3 retries + 1 final attempt
      end
    end

    context "when exception is NOT retriable" do
      it "goes directly to DLQ without retrying" do
        consumer = TestNonRetriableConsumer.new

        expect(OutboxRelay.logger).to receive(:error).with(
          hash_including(
            event_name: "consume_message_failed",
            retriable_attempts_exhausted: false
          )
        )

        expect { consumer.send(:process_event, event) }.to raise_error(TestRateLimitedError)
        expect(consumer.call_count).to eq(1)  # No retries
      end
    end

    context "with mixed exception types" do
      it "only retries retriable exceptions" do
        consumer = TestRetriableConsumer.new
        consumer.should_fail = true
        consumer.fail_count = 1
        consumer.fail_with = TestTransientError  # Not retriable

        expect { consumer.send(:process_event, event) }.to raise_error(TestTransientError)
        expect(consumer.call_count).to eq(1)  # No retries for non-retriable
      end
    end
  end
end
