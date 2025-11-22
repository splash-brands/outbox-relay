# frozen_string_literal: true

module OutboxRelay
  # LogSubscriber - Unified structured logging for all OutboxRelay events
  #
  # This subscribes to ActiveSupport::Notifications instrumentation events
  # and provides consistent, structured logging across all OutboxRelay operations.
  #
  # Follows SolidQueue's consistent formatting pattern for better readability.
  #
  # ## Benefits
  #
  # 1. **Consistent Format**: All logs use `formatted_event(event, action:, **attributes)`
  # 2. **Automatic Duration Tracking**: Every log includes event duration automatically
  # 3. **Version Prefix**: All logs include gem version for debugging
  # 4. **Structured Output**: Consistent log format across all operations
  #
  # ## Usage
  #
  # By default, this subscriber is automatically attached in the Engine:
  #
  #   OutboxRelay::LogSubscriber.attach_to :outbox_relay
  #
  # Log levels:
  # - INFO: Process lifecycle (start, stop, registration), batch processing (when events processed)
  # - WARN: Errors, failures, unexpected conditions
  # - DEBUG: Polling operations (noisy, off by default)
  #
  # ## Example Output
  #
  #   OutboxRelay-0.7.0 Started worker (45.2ms)  name: "worker-abc123", kind: :worker, topic: "orders", partition: 0
  #   OutboxRelay-0.7.0 Batch processed (123.4ms)  worker: "worker-abc123", processed: 10, throughput_per_sec: 81.0
  #   OutboxRelay-0.7.0 Worker stopped (12.3ms)  name: "worker-abc123", uptime: 3600.0
  #
  class LogSubscriber < ActiveSupport::LogSubscriber
    # Process lifecycle events

    def start_process(event)
      process = event.payload[:process]
      attributes = { name: process.name, kind: process.kind }
      attributes.merge!(process.metadata) if process.metadata.any?

      info formatted_event(event, action: "Started process", **attributes)
    end

    def start_supervisor(event)
      process = event.payload[:process]
      metadata = process.metadata

      info formatted_event(
        event,
        action: "Supervisor started",
        pid: process.pid,
        workers: metadata[:workers_count],
        polling_interval: "#{metadata[:polling_interval]}s",
        batch_size: metadata[:batch_size]
      )
    end

    def shutdown_process(event)
      process = event.payload[:process]
      attributes = {
        kind: process.kind,
        name: process.name
      }
      attributes[:uptime] = "#{process.metadata[:uptime]&.round(1)}s" if process.metadata[:uptime]

      info formatted_event(event, action: "Process stopped", **attributes)
    end

    def shutdown_supervisor(event)
      process = event.payload[:process]
      metadata = process.metadata

      info formatted_event(
        event,
        action: "Supervisor stopped",
        pid: process.pid,
        uptime: "#{metadata[:uptime]&.round(1)}s"
      )
    end

    # Worker polling events (debug level - noisy)

    def poll(event)
      debug formatted_event(event, action: "Polling")
    end

    def process_batch(event)
      payload = event.payload
      process = payload[:process]
      consumer = payload[:consumer]
      processed_count = payload[:processed_count] || 0

      # Only log when events were actually processed (reduce noise when idle)
      return unless processed_count > 0

      # Calculate throughput for monitoring
      throughput = processed_count / (event.duration / 1000.0)

      info formatted_event(
        event,
        action: "Batch processed",
        worker: process&.name,
        consumer_group: consumer&.consumer_group,
        topic: consumer&.topic,
        partition: consumer&.partition_key,
        processed: processed_count,
        lag: payload[:lag],
        throughput_per_sec: throughput.round(1)
      )
    end

    # Registration events

    def process_registered(event)
      payload = event.payload

      info formatted_event(
        event,
        action: "Process registered",
        id: payload[:process_id],
        name: payload[:name],
        kind: payload[:kind],
        pid: payload[:pid]
      )
    end

    def process_deregistered(event)
      payload = event.payload

      info formatted_event(
        event,
        action: "Process deregistered",
        id: payload[:process_id],
        name: payload[:name]
      )
    end

    # Fork management events (supervisor)

    def worker_forked(event)
      payload = event.payload

      info formatted_event(
        event,
        action: "Worker forked",
        pid: payload[:worker_pid],
        name: payload[:worker_name],
        topic: payload[:topic],
        partition: payload[:partition_key]
      )
    end

    def restart_fork(event)
      payload = event.payload

      if payload[:exit_status] == 0
        info formatted_event(
          event,
          action: "Worker restarted",
          pid: payload[:pid],
          exit: "success"
        )
      else
        warn formatted_event(
          event,
          action: "Worker restarted",
          pid: payload[:pid],
          exit: payload[:exit_status],
          signaled: payload[:signaled]
        )
      end
    end

    # Graceful shutdown events

    def graceful_termination(event)
      payload = event.payload
      attributes = {
        supervisor_pid: payload[:supervisor_pid],
        workers: payload[:worker_pids]&.size || 0
      }
      attributes[:timeout_exceeded] = true if payload[:shutdown_timeout_exceeded]

      info formatted_event(event, action: "Graceful termination initiated", **attributes)
    end

    def immediate_termination(event)
      payload = event.payload

      warn formatted_event(
        event,
        action: "Immediate termination",
        workers_killed: payload[:worker_pids]&.size || 0
      )
    end

    # Error events

    def thread_error(event)
      exception = event.payload[:error]

      error formatted_event(
        event,
        action: "Thread error",
        exception: exception.class.name,
        message: exception.message
      )
    end

    # Heartbeat events (debug level - very noisy)

    def heartbeat(event)
      payload = event.payload

      debug formatted_event(
        event,
        action: "Heartbeat",
        process_id: payload[:process_id]
      )
    end

    def heartbeat_failed(event)
      payload = event.payload

      warn formatted_event(
        event,
        action: "Heartbeat failed",
        process_id: payload[:process_id],
        error: payload[:error]
      )
    end

    # Helper methods for colored output (Rails style)

    private

    # Central formatting method - all logs use this for consistency
    # Format: "OutboxRelay-VERSION Action (duration)  key1: value1, key2: value2"
    def formatted_event(event, action:, **attributes)
      attributes_str = formatted_attributes(**attributes)
      base = "OutboxRelay-#{OutboxRelay::VERSION} #{action} (#{event.duration.round(1)}ms)"
      attributes_str.present? ? "#{base}  #{attributes_str}" : base
    end

    def formatted_attributes(**attributes)
      attributes.map { |attr, value| "#{attr}: #{value.inspect}" }.join(", ")
    end

    def info(message)
      logger.info(color(message, :green, true))
    end

    def warn(message)
      logger.warn(color(message, :yellow, true))
    end

    def error(message)
      logger.error(color(message, :red, true))
    end

    def debug(message)
      logger.debug(color(message, :cyan, true))
    end

    def color(message, color_name, bold = false)
      return message unless logger.formatter.is_a?(ActiveSupport::Logger::SimpleFormatter)

      color_code = {
        black: 30,
        red: 31,
        green: 32,
        yellow: 33,
        blue: 34,
        magenta: 35,
        cyan: 36,
        white: 37
      }[color_name]

      "\e[#{bold ? 1 : 0};#{color_code}m[OutboxRelay] #{message}\e[0m"
    end
  end
end
