# frozen_string_literal: true

require "spec_helper"

RSpec.describe OutboxRelay::Instrumentation do
  # Helper to capture events emitted via ActiveSupport::Notifications
  def capture_events(pattern = /^outbox_relay\./)
    events = []
    subscriber = ActiveSupport::Notifications.subscribe(pattern) do |name, start, finish, id, payload|
      events << { name: name, payload: payload }
    end

    yield

    events
  ensure
    ActiveSupport::Notifications.unsubscribe(subscriber) if subscriber
  end

  describe ".error" do
    it "emits event with exception and metadata" do
      exception = StandardError.new("Test error")
      events = capture_events do
        described_class.error("test.error", exception, severity: "high", context: "test")
      end

      expect(events.size).to eq(1)
      expect(events.first[:name]).to eq("outbox_relay.test.error")
      expect(events.first[:payload][:exception]).to eq(exception)
      expect(events.first[:payload][:error]).to eq("Test error")
      expect(events.first[:payload][:error_class]).to eq("StandardError")
      expect(events.first[:payload][:severity]).to eq("high")
      expect(events.first[:payload][:context]).to eq("test")
      expect(events.first[:payload][:backtrace]).to be_a(Array).or be_nil
    end
  end

  describe ".message" do
    it "emits event with message and metadata" do
      events = capture_events do
        described_class.message("test.message", "Test message", severity: "warning", context: "test")
      end

      expect(events.size).to eq(1)
      expect(events.first[:name]).to eq("outbox_relay.test.message")
      expect(events.first[:payload][:message]).to eq("Test message")
      expect(events.first[:payload][:severity]).to eq("warning")
      expect(events.first[:payload][:context]).to eq("test")
    end
  end

  describe OutboxRelay::Instrumentation::Worker do
    describe ".poll_error" do
      it "emits worker.poll_error event with critical severity" do
        exception = StandardError.new("Poll failed")
        events = capture_events(/worker\.poll_error/) do
          described_class.poll_error(
            exception,
            process_id: "worker-1",
            consumer_class: "TestConsumer",
            partition_key: 0,
            loop_count: 5,
            total_processed: 100
          )
        end

        expect(events.size).to eq(1)
        expect(events.first[:name]).to eq("outbox_relay.worker.poll_error")
        expect(events.first[:payload][:exception]).to eq(exception)
        expect(events.first[:payload][:severity]).to eq("critical")
        expect(events.first[:payload][:process_id]).to eq("worker-1")
        expect(events.first[:payload][:consumer_class]).to eq("TestConsumer")
        expect(events.first[:payload][:partition_key]).to eq(0)
        expect(events.first[:payload][:loop_count]).to eq(5)
        expect(events.first[:payload][:total_processed]).to eq(100)
      end
    end

    describe ".delay_calculation_error" do
      it "emits worker.delay_calculation_error event with high severity" do
        exception = StandardError.new("Delay calculation failed")
        events = capture_events(/worker\.delay_calculation_error/) do
          described_class.delay_calculation_error(
            exception,
            process_id: "worker-1",
            consumer_class: "TestConsumer",
            partition_key: 0,
            processed_count: 10
          )
        end

        expect(events.size).to eq(1)
        expect(events.first[:name]).to eq("outbox_relay.worker.delay_calculation_error")
        expect(events.first[:payload][:exception]).to eq(exception)
        expect(events.first[:payload][:severity]).to eq("high")
        expect(events.first[:payload][:processed_count]).to eq(10)
      end
    end
  end

  describe OutboxRelay::Instrumentation::Heartbeat do
    describe ".failure" do
      it "emits heartbeat.failure event with warning severity when failures below threshold" do
        exception = StandardError.new("Heartbeat failed")
        events = capture_events(/heartbeat\.failure/) do
          described_class.failure(
            exception,
            process_id: "proc-1",
            consecutive_failures: 3,
            max_failures: 5
          )
        end

        expect(events.size).to eq(1)
        expect(events.first[:payload][:severity]).to eq("warning")
        expect(events.first[:payload][:consecutive_failures]).to eq(3)
        expect(events.first[:payload][:max_failures]).to eq(5)
      end

      it "emits heartbeat.failure event with critical severity when failures reach threshold" do
        exception = StandardError.new("Heartbeat failed")
        events = capture_events(/heartbeat\.failure/) do
          described_class.failure(
            exception,
            process_id: "proc-1",
            consecutive_failures: 5,
            max_failures: 5
          )
        end

        expect(events.size).to eq(1)
        expect(events.first[:payload][:severity]).to eq("critical")
      end
    end

    describe ".task_error" do
      it "emits heartbeat.task_error event with high severity" do
        exception = StandardError.new("Task error")
        events = capture_events(/heartbeat\.task_error/) do
          described_class.task_error(exception, process_id: "proc-1")
        end

        expect(events.size).to eq(1)
        expect(events.first[:name]).to eq("outbox_relay.heartbeat.task_error")
        expect(events.first[:payload][:severity]).to eq("high")
        expect(events.first[:payload][:context]).to eq("heartbeat_timer_task")
      end
    end

    describe ".start_error" do
      it "emits heartbeat.start_error event with high severity" do
        exception = StandardError.new("Start error")
        events = capture_events(/heartbeat\.start_error/) do
          described_class.start_error(exception, process_id: "proc-1")
        end

        expect(events.size).to eq(1)
        expect(events.first[:name]).to eq("outbox_relay.heartbeat.start_error")
        expect(events.first[:payload][:severity]).to eq("high")
      end
    end
  end

  describe OutboxRelay::Instrumentation::Process do
    describe ".registration_failed" do
      it "emits process.registration_failed event with critical severity" do
        exception = StandardError.new("Registration failed")
        events = capture_events(/process\.registration_failed/) do
          described_class.registration_failed(
            exception,
            name: "worker-1",
            kind: "worker",
            pid: 12345
          )
        end

        expect(events.size).to eq(1)
        expect(events.first[:name]).to eq("outbox_relay.process.registration_failed")
        expect(events.first[:payload][:severity]).to eq("critical")
        expect(events.first[:payload][:name]).to eq("worker-1")
        expect(events.first[:payload][:kind]).to eq("worker")
        expect(events.first[:payload][:pid]).to eq(12345)
      end
    end

    describe ".deregistration_failed" do
      it "emits process.deregistration_failed event with high severity" do
        exception = StandardError.new("Deregistration failed")
        events = capture_events(/process\.deregistration_failed/) do
          described_class.deregistration_failed(
            exception,
            process_id: "proc-1",
            db_id: 1,
            name: "worker-1"
          )
        end

        expect(events.size).to eq(1)
        expect(events.first[:name]).to eq("outbox_relay.process.deregistration_failed")
        expect(events.first[:payload][:severity]).to eq("high")
        expect(events.first[:payload][:phase]).to eq("shutdown")
      end
    end

    describe ".heartbeat_failed" do
      it "emits process.heartbeat_failed with warning severity when below threshold" do
        exception = StandardError.new("Heartbeat failed")
        events = capture_events(/process\.heartbeat_failed/) do
          described_class.heartbeat_failed(
            exception,
            process_id: "proc-1",
            db_id: 1,
            consecutive_failures: 2,
            max_failures: 5
          )
        end

        expect(events.size).to eq(1)
        expect(events.first[:payload][:severity]).to eq("warning")
      end

      it "emits process.heartbeat_failed with critical severity when at threshold" do
        exception = StandardError.new("Heartbeat failed")
        events = capture_events(/process\.heartbeat_failed/) do
          described_class.heartbeat_failed(
            exception,
            process_id: "proc-1",
            db_id: 1,
            consecutive_failures: 5,
            max_failures: 5
          )
        end

        expect(events.size).to eq(1)
        expect(events.first[:payload][:severity]).to eq("critical")
      end
    end

    describe ".run_error" do
      it "emits process.run_error message event with error severity" do
        exception = StandardError.new("Run loop exited")
        events = capture_events(/process\.run_error/) do
          described_class.run_error(exception, name: "process_123")
        end

        expect(events.size).to eq(1)
        expect(events.first[:name]).to eq("outbox_relay.process.run_error")
        expect(events.first[:payload][:message]).to eq("Process run loop exited with error")
        expect(events.first[:payload][:severity]).to eq("error")
        expect(events.first[:payload][:name]).to eq("process_123")
        expect(events.first[:payload][:error]).to eq("Run loop exited")
        expect(events.first[:payload][:error_class]).to eq("StandardError")
      end
    end
  end

  describe OutboxRelay::Instrumentation::Supervisor do
    describe ".boot_incomplete" do
      it "emits supervisor.boot_incomplete message event" do
        failed_details = [{ worker: "worker-1", error: "Failed to start" }]
        events = capture_events(/supervisor\.boot_incomplete/) do
          described_class.boot_incomplete(
            total_expected: 5,
            running_workers: 4,
            failed_workers: 1,
            failed_details: failed_details
          )
        end

        expect(events.size).to eq(1)
        expect(events.first[:name]).to eq("outbox_relay.supervisor.boot_incomplete")
        expect(events.first[:payload][:message]).to eq("Supervisor boot completed with failures")
        expect(events.first[:payload][:severity]).to eq("error")
        expect(events.first[:payload][:total_expected]).to eq(5)
        expect(events.first[:payload][:running_workers]).to eq(4)
        expect(events.first[:payload][:failed_workers]).to eq(1)
        expect(events.first[:payload][:failed_details]).to eq(failed_details)
      end
    end

    describe ".fork_error" do
      it "emits supervisor.fork_error event with critical severity" do
        exception = StandardError.new("Fork failed")
        events = capture_events(/supervisor\.fork_error/) do
          described_class.fork_error(
            exception,
            worker_name: "worker-1",
            consumer_class: "TestConsumer"
          )
        end

        expect(events.size).to eq(1)
        expect(events.first[:name]).to eq("outbox_relay.supervisor.fork_error")
        expect(events.first[:payload][:severity]).to eq("critical")
        expect(events.first[:payload][:phase]).to eq("fork")
        expect(events.first[:payload][:worker_name]).to eq("worker-1")
        expect(events.first[:payload][:consumer_class]).to eq("TestConsumer")
      end
    end

    describe ".restart_abandoned" do
      it "emits supervisor.restart_abandoned message event" do
        events = capture_events(/supervisor\.restart_abandoned/) do
          described_class.restart_abandoned(
            worker_name: "worker-1",
            worker_key: "key-1",
            exit_status: 1,
            restart_attempts: 10
          )
        end

        expect(events.size).to eq(1)
        expect(events.first[:name]).to eq("outbox_relay.supervisor.restart_abandoned")
        expect(events.first[:payload][:message]).to eq("Worker restart abandoned after excessive failures")
        expect(events.first[:payload][:severity]).to eq("error")
        expect(events.first[:payload][:restart_attempts]).to eq(10)
      end
    end
  end

  describe OutboxRelay::Instrumentation::Poller do
    describe ".poll_error" do
      it "emits poller.poll_error event with warning severity" do
        exception = StandardError.new("Poll error")
        events = capture_events(/poller\.poll_error/) do
          described_class.poll_error(
            exception,
            process_id: "proc-1",
            name: "poller-1"
          )
        end

        expect(events.size).to eq(1)
        expect(events.first[:name]).to eq("outbox_relay.poller.poll_error")
        expect(events.first[:payload][:severity]).to eq("warning")
        expect(events.first[:payload][:phase]).to eq("poll")
      end
    end

    describe ".instrumentation_error" do
      it "emits poller.instrumentation_error event with high severity" do
        exception = StandardError.new("Instrumentation error")
        events = capture_events(/poller\.instrumentation_error/) do
          described_class.instrumentation_error(
            exception,
            process_id: "proc-1",
            name: "poller-1"
          )
        end

        expect(events.size).to eq(1)
        expect(events.first[:name]).to eq("outbox_relay.poller.instrumentation_error")
        expect(events.first[:payload][:severity]).to eq("high")
        expect(events.first[:payload][:phase]).to eq("instrumentation")
      end
    end
  end

  describe OutboxRelay::Instrumentation::Configuration do
    describe ".partition_count_query_failed" do
      it "emits configuration.partition_count_query_failed event with critical severity" do
        exception = StandardError.new("Query failed")
        events = capture_events(/configuration\.partition_count_query_failed/) do
          described_class.partition_count_query_failed(exception, topic: "test_topic")
        end

        expect(events.size).to eq(1)
        expect(events.first[:name]).to eq("outbox_relay.configuration.partition_count_query_failed")
        expect(events.first[:payload][:severity]).to eq("critical")
        expect(events.first[:payload][:topic]).to eq("test_topic")
      end
    end
  end

  describe OutboxRelay::Instrumentation::Tasks do
    describe ".task_error" do
      it "emits tasks.error event with provided context" do
        exception = StandardError.new("Task failed")
        events = capture_events(/tasks\.error/) do
          described_class.task_error(
            exception,
            task_name: "outbox_relay:cleanup",
            phase: "event_deletion",
            severity: "high"
          )
        end

        expect(events.size).to eq(1)
        expect(events.first[:name]).to eq("outbox_relay.tasks.error")
        expect(events.first[:payload][:task_name]).to eq("outbox_relay:cleanup")
        expect(events.first[:payload][:phase]).to eq("event_deletion")
        expect(events.first[:payload][:severity]).to eq("high")
      end
    end
  end

  describe OutboxRelay::Instrumentation::Models do
    describe ".error" do
      it "emits models.error event with model and operation context" do
        exception = StandardError.new("Model error")
        events = capture_events(/models\.error/) do
          described_class.error(
            exception,
            model: "OutboxConsumer",
            operation: "event_processing",
            event_id: "evt-123",
            consumer_group: "test_group"
          )
        end

        expect(events.size).to eq(1)
        expect(events.first[:name]).to eq("outbox_relay.models.error")
        expect(events.first[:payload][:severity]).to eq("high")
        expect(events.first[:payload][:model]).to eq("OutboxConsumer")
        expect(events.first[:payload][:operation]).to eq("event_processing")
        expect(events.first[:payload][:event_id]).to eq("evt-123")
        expect(events.first[:payload][:consumer_group]).to eq("test_group")
      end
    end
  end

  describe OutboxRelay::Instrumentation::Callbacks do
    describe ".boot_failed" do
      it "emits callbacks.boot_failed event with warning severity" do
        exception = StandardError.new("Boot callback failed")
        events = capture_events(/callbacks\.boot_failed/) do
          described_class.boot_failed(exception, callback: :initialize_connection)
        end

        expect(events.size).to eq(1)
        expect(events.first[:name]).to eq("outbox_relay.callbacks.boot_failed")
        expect(events.first[:payload][:severity]).to eq("warning")
        expect(events.first[:payload][:phase]).to eq("boot")
        expect(events.first[:payload][:callback]).to eq(:initialize_connection)
      end
    end

    describe ".shutdown_block_failed" do
      it "emits callbacks.shutdown_block_failed event" do
        exception = StandardError.new("Shutdown block failed")
        events = capture_events(/callbacks\.shutdown_block_failed/) do
          described_class.shutdown_block_failed(exception)
        end

        expect(events.size).to eq(1)
        expect(events.first[:name]).to eq("outbox_relay.callbacks.shutdown_block_failed")
        expect(events.first[:payload][:severity]).to eq("warning")
        expect(events.first[:payload][:phase]).to eq("shutdown_block")
      end
    end

    describe ".shutdown_failed" do
      it "emits callbacks.shutdown_failed event" do
        exception = StandardError.new("Shutdown callback failed")
        events = capture_events(/callbacks\.shutdown_failed/) do
          described_class.shutdown_failed(exception, callback: :cleanup_resources)
        end

        expect(events.size).to eq(1)
        expect(events.first[:name]).to eq("outbox_relay.callbacks.shutdown_failed")
        expect(events.first[:payload][:severity]).to eq("warning")
        expect(events.first[:payload][:phase]).to eq("shutdown")
        expect(events.first[:payload][:callback]).to eq(:cleanup_resources)
      end
    end
  end

  describe OutboxRelay::Instrumentation::Runnable do
    describe ".reconnect_error" do
      it "emits runnable.reconnect_error event with high severity" do
        exception = StandardError.new("Reconnect failed")
        events = capture_events(/runnable\.reconnect_error/) do
          described_class.reconnect_error(
            exception,
            process_id: "proc-1",
            attempt: 3,
            max_attempts: 5
          )
        end

        expect(events.size).to eq(1)
        expect(events.first[:name]).to eq("outbox_relay.runnable.reconnect_error")
        expect(events.first[:payload][:severity]).to eq("high")
        expect(events.first[:payload][:attempt]).to eq(3)
        expect(events.first[:payload][:max_attempts]).to eq(5)
      end
    end

    describe ".fork_initialization_error" do
      it "emits runnable.fork_initialization_error event with critical severity" do
        exception = StandardError.new("Fork init failed")
        events = capture_events(/runnable\.fork_initialization_error/) do
          described_class.fork_initialization_error(
            exception,
            consumer_class: "TestConsumer",
            partition_key: 0
          )
        end

        expect(events.size).to eq(1)
        expect(events.first[:name]).to eq("outbox_relay.runnable.fork_initialization_error")
        expect(events.first[:payload][:severity]).to eq("critical")
        expect(events.first[:payload][:consumer_class]).to eq("TestConsumer")
        expect(events.first[:payload][:partition_key]).to eq(0)
      end
    end
  end

  describe OutboxRelay::Instrumentation::Signals do
    describe ".signal_handler_error" do
      it "emits signals.handler_error event with high severity" do
        exception = StandardError.new("Signal handler failed")
        events = capture_events(/signals\.handler_error/) do
          described_class.signal_handler_error(exception, signal_name: "TERM_to_12345")
        end

        expect(events.size).to eq(1)
        expect(events.first[:name]).to eq("outbox_relay.signals.handler_error")
        expect(events.first[:payload][:severity]).to eq("high")
        expect(events.first[:payload][:signal_name]).to eq("TERM_to_12345")
      end
    end
  end

  describe OutboxRelay::Instrumentation::CLI do
    describe ".start_error" do
      it "emits cli.start_error event with critical severity" do
        exception = StandardError.new("CLI start failed")
        events = capture_events(/cli\.start_error/) do
          described_class.start_error(exception)
        end

        expect(events.size).to eq(1)
        expect(events.first[:name]).to eq("outbox_relay.cli.start_error")
        expect(events.first[:payload][:severity]).to eq("critical")
      end
    end
  end
end
