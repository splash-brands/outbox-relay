# frozen_string_literal: true

module OutboxRelay
  # LogSubscriber - Unified structured logging for all OutboxRelay events
  #
  # This subscribes to ActiveSupport::Notifications instrumentation events
  # and provides consistent, structured logging across all OutboxRelay operations.
  #
  # ## Benefits
  #
  # 1. **Automatic Duration Tracking**: Every instrumented event gets duration logged
  # 2. **Structured Output**: Consistent log format across all operations
  # 3. **Debug Mode**: Set log level to see detailed polling/batch operations
  # 4. **Rails Integration**: Uses Rails.logger with proper log levels
  #
  # ## Usage
  #
  # By default, this subscriber is automatically attached in the Engine:
  #
  #   OutboxRelay::LogSubscriber.attach_to :outbox_relay
  #
  # Log levels:
  # - INFO: Process lifecycle (start, stop, registration)
  # - WARN: Errors, failures, unexpected conditions
  # - DEBUG: Polling, batches, heartbeats (noisy, off by default)
  #
  # ## Example Output
  #
  #   [OutboxRelay] Started worker-abc123 (kind: worker, topic: orders, partition: 0)
  #   [OutboxRelay] Polling completed (duration: 45.2ms, events: 10)
  #   [OutboxRelay] Batch processed (duration: 123.4ms, events: 10, topic: orders)
  #   [OutboxRelay] Worker stopped (uptime: 3600s, loops: 1000)
  #
  class LogSubscriber < ActiveSupport::LogSubscriber
    # Process lifecycle events

    def start_process(event)
      process = event.payload[:process]
      info do
        message = "Started #{process.name} (kind: #{process.kind}"
        message += ", #{process.metadata.map { |k, v| "#{k}: #{v}" }.join(", ")}" if process.metadata.any?
        message += ")"
        message
      end
    end

    def start_supervisor(event)
      process = event.payload[:process]
      info do
        metadata = process.metadata
        "Supervisor started (pid: #{process.pid}, workers: #{metadata[:workers_count]}, " \
        "polling_interval: #{metadata[:polling_interval]}s, batch_size: #{metadata[:batch_size]})"
      end
    end

    def shutdown_process(event)
      process = event.payload[:process]
      info do
        message = "#{process.kind.capitalize} stopped (name: #{process.name}"
        message += ", uptime: #{process.metadata[:uptime]&.round(1)}s" if process.metadata[:uptime]
        message += ")"
        message
      end
    end

    def shutdown_supervisor(event)
      process = event.payload[:process]
      info do
        metadata = process.metadata
        "Supervisor stopped (pid: #{process.pid}, uptime: #{metadata[:uptime]&.round(1)}s)"
      end
    end

    # Worker polling events (debug level - noisy)

    def poll(event)
      debug do
        "Polling (duration: #{event.duration.round(1)}ms)"
      end
    end

    def process_batch(event)
      debug do
        payload = event.payload
        process = payload[:process]
        "Batch processed (duration: #{event.duration.round(1)}ms, processed: #{payload[:processed_count]}, " \
        "lag: #{payload[:lag]})"
      end
    end

    # Registration events

    def process_registered(event)
      info do
        payload = event.payload
        "Process registered (id: #{payload[:process_id]}, name: #{payload[:name]}, " \
        "kind: #{payload[:kind]}, pid: #{payload[:pid]})"
      end
    end

    def process_deregistered(event)
      info do
        payload = event.payload
        "Process deregistered (id: #{payload[:process_id]}, name: #{payload[:name]})"
      end
    end

    # Fork management events (supervisor)

    def worker_forked(event)
      info do
        payload = event.payload
        "Worker forked (pid: #{payload[:worker_pid]}, name: #{payload[:worker_name]}, " \
        "topic: #{payload[:topic]}, partition: #{payload[:partition_key]})"
      end
    end

    def restart_fork(event)
      payload = event.payload
      if payload[:exit_status] == 0
        info do
          "Worker restarted (pid: #{payload[:pid]}, exit: success)"
        end
      else
        warn do
          "Worker restarted (pid: #{payload[:pid]}, exit: #{payload[:exit_status]}, " \
          "signaled: #{payload[:signaled]})"
        end
      end
    end

    # Graceful shutdown events

    def graceful_termination(event)
      payload = event.payload
      info do
        message = "Graceful termination initiated (supervisor_pid: #{payload[:supervisor_pid]}"
        message += ", workers: #{payload[:worker_pids]&.size || 0}"
        message += ", timeout_exceeded: true" if payload[:shutdown_timeout_exceeded]
        message += ")"
        message
      end
    end

    def immediate_termination(event)
      warn do
        payload = event.payload
        "Immediate termination (KILL signal sent to #{payload[:worker_pids]&.size || 0} workers)"
      end
    end

    # Error events

    def thread_error(event)
      error do
        exception = event.payload[:error]
        "Thread error: #{exception.class.name}: #{exception.message}"
      end
    end

    # Heartbeat events (debug level - very noisy)

    def heartbeat(event)
      debug do
        payload = event.payload
        "Heartbeat (process_id: #{payload[:process_id]}, duration: #{event.duration.round(1)}ms)"
      end
    end

    def heartbeat_failed(event)
      warn do
        payload = event.payload
        "Heartbeat failed (process_id: #{payload[:process_id]}, error: #{payload[:error]})"
      end
    end

    # Helper methods for colored output (Rails style)

    private

    def info(&block)
      return unless logger.info?
      logger.info(color(yield, :green, true))
    end

    def warn(&block)
      return unless logger.warn?
      logger.warn(color(yield, :yellow, true))
    end

    def error(&block)
      return unless logger.error?
      logger.error(color(yield, :red, true))
    end

    def debug(&block)
      return unless logger.debug?
      logger.debug(color(yield, :cyan, true))
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
