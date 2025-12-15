# frozen_string_literal: true

module OutboxRelay
  # Worker - Fork-based event processing with dynamic polling
  #
  # Each Worker instance runs in a separate forked process and is responsible for:
  # 1. Continuously polling for events in a specific topic partition
  # 2. Processing events via the configured consumer class
  # 3. Tracking offset progress for exactly-once processing
  # 4. Dynamically adjusting poll frequency based on backlog
  #
  # ## Architecture
  #
  # Workers inherit from Poller which provides the continuous polling loop.
  # The supervisor forks multiple workers - one per partition per consumer group.
  #
  # Example deployment with 2 topics, 4 partitions each, 2 consumer groups:
  #   - 1 Supervisor process
  #   - 16 Worker processes (2 topics × 4 partitions × 2 groups)
  #
  # ## Dynamic Polling (Solid Queue Pattern)
  #
  # Workers adjust their polling interval based on workload:
  #   - Significant backlog (lag > batch_size): 10ms delay
  #   - Some backlog (lag > 0): 100ms delay
  #   - No backlog: Full polling_interval (default 1s)
  #
  # This provides:
  #   - Low latency when busy (10ms-100ms)
  #   - Low CPU usage when idle (1s between polls)
  #
  # ## Safety Mechanisms
  #
  # - Max loops: Worker restarts after processing max_loops batches (default 1000)
  #   to prevent memory leaks and ensure fresh state
  # - Heartbeat: Sends heartbeat every 10 loops to prove liveness
  # - Advisory locks: Prevent duplicate processing within same consumer group
  # - Offset tracking: Maintains exactly-once processing semantics
  #
  # ## Example Usage
  #
  #   worker = OutboxRelay::Worker.new(
  #     consumer_class: "NotificationsConsumer",
  #     consumer_group: "notifications",
  #     topic: "user_events",
  #     partition_key: 0,
  #     batch_size: 100,
  #     polling_interval: 1.0
  #   )
  #   worker.start  # Blocks until shutdown signal received
  #
  # ## Lifecycle
  #
  # 1. Boot: Reconnect DB, register process, set up signals
  # 2. Poll loop: Fetch batch → Process → Update offset → Calculate delay → Sleep
  # 3. Shutdown: Deregister, close connections, exit
  #
  class Worker < Processes::Poller
    include Processes::AppExecutor
    include Processes::PartitionClaiming

    after_boot :log_worker_start
    before_shutdown :release_partition_claim
    before_shutdown :log_worker_stop

    attr_reader :consumer_class_name, :consumer_group, :topic, :partition_key, :batch_size, :max_loops
    attr_reader :supervisor_db_process

    # Set the supervisor's database process record
    # Called by supervisor before fork to pass its DB record to the worker
    # This avoids PID-based lookup which fails in multi-container deployments
    def supervised_by(supervisor_process)
      @supervisor_db_process = supervisor_process
    end

    def initialize(consumer_class:, consumer_group:, topic:, partition_key:, polling_interval: 1.0, batch_size: 100, max_loops: 1000)
      @consumer_class_name = consumer_class
      @consumer_group = consumer_group
      @topic = topic
      @partition_key = partition_key
      @batch_size = batch_size
      @max_loops = max_loops
      @total_processed = 0
      @loop_count = 0
      @last_heartbeat = nil

      super(polling_interval: polling_interval)
    end

    def metadata
      super.merge(
        consumer_class: consumer_class_name,
        consumer_group: consumer_group,
        topic: topic,
        partition_key: partition_key,
        batch_size: batch_size,
        total_processed: @total_processed,
        loop_count: @loop_count,
      )
    end

    private

    # Main polling method - called continuously in loop
    # Returns the delay until next poll
    def poll
      # Wrap entire poll cycle in Rails executor for proper context management
      # This ensures database connections, code reloading, and other Rails
      # features work correctly in forked worker processes
      wrap_in_app_executor(source: "outbox_relay.poll") do
        consumer = instantiate_consumer

        # Process one batch
        count = process_batch(consumer)
        @total_processed += count
        @loop_count += 1

        # Update heartbeat periodically
        if should_send_heartbeat?
          heartbeat
          @last_heartbeat = Time.current
        end

        # Determine next poll delay based on work availability
        calculate_next_delay(consumer, count)
      end
    rescue => e
      # Handle thread-level errors using AppExecutor pattern
      handle_thread_error(e)

      OutboxRelay.logger.error(
        event_name: "worker_poll_error",
        process_id: process_id,
        consumer_class: consumer_class_name,
        partition_key: partition_key,
        error: e.message,
        backtrace: e.backtrace&.first(10)&.join("\n"),
      )

      # Report to Sentry - worker poll errors are CRITICAL because:
      # 1. This partition stops processing (events pile up)
      # 2. Consumer lag increases (SLA breach risk)
      # 3. May indicate systemic issues (DB down, consumer bug)
      # 4. Requires immediate investigation and intervention
      #
      # Expected operator actions:
      # - Check error type (DB connection? Consumer bug? Resource exhaustion?)
      # - Verify database connectivity
      # - Check consumer implementation for bugs
      # - Monitor consumer lag (rake outbox_relay:lag)
      # - Consider manual restart if issue persists
      OutboxRelay::Instrumentation::Worker.poll_error(
        e,
        process_id: process_id,
        consumer_class: consumer_class_name,
        partition_key: partition_key,
        loop_count: @loop_count,
        total_processed: @total_processed
      )

      # On error, wait full polling interval (avoid tight error loop)
      polling_interval
    end

    def process_batch(consumer)
      # Update procline to show processing status
      update_procline(processing: true)

      OutboxRelay.instrument(:process_batch, process: self, consumer: consumer) do |payload|
        count = consumer.consume_batch(batch_size: batch_size)

        payload[:processed_count] = count
        payload[:lag] = consumer.lag

        count
      end
    ensure
      # Reset procline to waiting status
      update_procline(processing: false)
    end

    # Calculate next poll delay using Solid Queue adaptive polling pattern
    #
    # ## Solid Queue Pattern
    #
    # Instead of fixed polling interval, dynamically adjust based on workload:
    #   - Heavy load: Poll fast (10ms) for low latency
    #   - Some load: Poll medium (100ms) for balance
    #   - Idle: Poll slow (1s default) for low CPU usage
    #
    # ## Delay Values Explained
    #
    # - **10ms**: Minimum safe delay to prevent tight loop (1% CPU overhead)
    #   Used when lag > batch_size (significant backlog detected)
    #   Achieves ~10-20ms end-to-end latency under load
    #
    # - **100ms**: Balanced delay for moderate workload
    #   Used when lag > 0 (some events pending)
    #   Good compromise: responsive but not aggressive
    #
    # - **1s** (polling_interval): Idle polling
    #   Used when lag = 0 (no backlog)
    #   Minimizes CPU usage when queue is empty
    #
    # ## Performance Characteristics
    #
    # Under load (1000 events/sec):
    #   - Polls every 10ms → 100 polls/sec
    #   - Each batch processes 100 events
    #   - Throughput: 10,000 events/sec/worker
    #   - Latency: 10-20ms average
    #
    # When idle (0 events):
    #   - Polls every 1s → 1 poll/sec
    #   - CPU usage: ~0.01% per worker
    #
    def calculate_next_delay(consumer, processed_count)
      if processed_count > 0
        # We processed some events - check for backlog
        lag = consumer.lag

        if lag > batch_size
          # Significant backlog - poll again immediately
          0.01 # 10ms delay to prevent tight loop
        elsif lag > 0
          # Some events pending - poll quickly
          0.1 # 100ms delay
        else
          # No backlog - use normal polling interval
          polling_interval
        end
      else
        # No events processed - use normal polling interval
        polling_interval
      end
    rescue => e
      OutboxRelay.logger.error(
        event_name: "worker_delay_calculation_error",
        process_id: process_id,
        processed_count: processed_count,
        error: e.message,
        error_class: e.class.name,
        backtrace: e.backtrace&.first(5)&.join("\n"),
      )

      OutboxRelay::Instrumentation::Worker.delay_calculation_error(
        e,
        process_id: process_id,
        consumer_class: consumer_class_name,
        partition_key: partition_key,
        processed_count: processed_count
      )

      # Conservative fallback: Use normal polling interval regardless of processed_count
      # This prevents aggressive polling during database issues which could worsen DB load
      # Trade-off: May increase latency if lag queries fail during normal operation,
      # but prevents exacerbating database problems during outages
      OutboxRelay.logger.warn(
        event_name: "using_conservative_delay_on_lag_failure",
        process_id: process_id,
        processed_count: processed_count,
        fallback_delay: polling_interval,
        reason: "Cannot determine lag - using normal polling interval to avoid overwhelming database"
      )

      polling_interval
    end

    def instantiate_consumer
      consumer_class_name.constantize.new(partition_key: partition_key)
    rescue => e
      OutboxRelay.logger.error(
        event_name: "consumer_instantiation_failed",
        consumer_class: consumer_class_name,
        partition_key: partition_key,
        error: e.message,
      )
      raise
    end

    def should_send_heartbeat?
      # Send heartbeat every 10 loops or every 10 seconds
      @loop_count % 10 == 0 || (@last_heartbeat.nil? || Time.current - @last_heartbeat > 10)
    end

    def finished?
      # Stop if we've exceeded max loops (safety mechanism)
      @loop_count >= max_loops
    end

    def shutdown
      # CRITICAL: Call super first to stop heartbeat timer before logging
      # Logging could trigger database activity, which should not happen while heartbeat is running
      # This prevents race where heartbeat fires during shutdown logging
      super

      # Log different messages based on shutdown reason
      # Now safe to log since heartbeat has been stopped by super
      if supervised? && supervisor_went_away?
        OutboxRelay.logger.warn(
          event_name: "worker_orphaned_shutting_down",
          process_id: process_id,
          name: name,
          supervisor_pid: @supervisor_pid,
          current_ppid: ::Process.ppid,
          total_processed: @total_processed,
          loop_count: @loop_count,
        )
      else
        OutboxRelay.logger.info(
          event_name: "worker_shutting_down",
          process_id: process_id,
          name: name,
          total_processed: @total_processed,
          loop_count: @loop_count,
        )
      end
    end

    def set_procline
      # Initial procline set during boot
      update_procline(processing: false)
    end

    def update_procline(processing:)
      if processing
        # Show processing status with progress
        procline("processing #{topic}[#{partition_key}] (#{@total_processed} processed)")
      else
        # Show waiting status with consumer group
        procline("waiting for #{topic}[#{partition_key}] (#{consumer_group})")
      end
    end

    # Lifecycle callbacks

    def log_worker_start
      # Log structured startup event
      print_startup_banner
    end

    def print_startup_banner
      # Log structured data instead of ASCII banner
      # LogSubscriber already handles the visual presentation with start_process event
      log_data = {
        event_name: "worker_started",
        process_id: process_id,
        name: name,
        consumer_class: consumer_class_name,
        consumer_group: consumer_group,
        topic: topic,
        partition_key: partition_key,
        polling_interval: polling_interval,
        batch_size: batch_size
      }

      # Add optional descriptions if available
      if OutboxRelay.configuration.consumer_group_configs[consumer_group]
        description = OutboxRelay.configuration.consumer_group_configs[consumer_group]["description"]
        log_data[:consumer_group_description] = description if description.present?
      end

      if OutboxRelay.configuration.topic_descriptions[topic]
        log_data[:topic_description] = OutboxRelay.configuration.topic_descriptions[topic]
      end

      OutboxRelay.logger.info(log_data)
    end

    def log_worker_stop
      OutboxRelay.logger.info(
        event_name: "worker_stopped",
        process_id: process_id,
        name: name,
        consumer_class: consumer_class_name,
        topic: topic,
        partition_key: partition_key,
        total_processed: @total_processed,
        loop_count: @loop_count,
      )
    end

  end
end
