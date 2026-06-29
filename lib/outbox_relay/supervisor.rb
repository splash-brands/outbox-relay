# frozen_string_literal: true

module OutboxRelay
  class Supervisor < Processes::Base
    include Processes::Signals

    CLAIM_UNAVAILABLE_EXIT_STATUS =
      Processes::PartitionClaiming::CLAIM_UNAVAILABLE_EXIT_STATUS
    CLAIM_UNAVAILABLE_RETRY_DELAY =
      Processes::PartitionClaiming::CLAIM_UNAVAILABLE_RETRY_DELAY

    # Default interval for partition health checks (can be overridden via configuration)
    DEFAULT_HEALTH_CHECK_INTERVAL = 30.seconds

    after_boot :log_supervisor_start
    before_shutdown :log_supervisor_stop

    class << self
      def start(**options)
        configuration = Configuration.new(**options)

        if configuration.valid?
          new(configuration).tap(&:start)
        else
          abort("OutboxRelay configuration errors:\n#{configuration.errors.join("\n")}\nExiting...")
        end
      end
    end

    attr_reader :configuration, :forks, :worker_configs

    def initialize(configuration)
      @configuration = configuration
      @forks = {} # pid => Worker instance metadata
      @worker_configs = {} # pid => WorkerConfig
      @signal_queue = []
      @restart_attempts = Hash.new(0)  # Track restart attempts per worker
      @restart_backoff_until = {}      # Track when worker can restart
      @failed_worker_starts = []       # Track workers that failed to start
      @last_health_check = Time.current # Track last partition health check
      @disabled_consumer_groups = Set.new # Base consumer_groups disabled via kill switch

      super()
    end

    def start
      boot
      start_workers
      supervise
    end

    def stop
      super
      OutboxRelay.logger.info(
        event_name: 'supervisor_stopping',
        process_id: process_id,
        supervisor_pid: ::Process.pid
      )
    end

    # Must be public - overrides Processes::Base#metadata
    # Called by LogSubscriber for instrumentation events
    def metadata
      super.merge(
        workers_count: forks.size,
        uptime: Time.current - (@started_at ||= Time.current)
      )
    end

    private

    def boot
      safe_instrument(:start_supervisor, process: self) do
        run_callbacks(:boot) do
          sync_std_streams
          register
          prune_dead_processes
          start_heartbeat # Start automatic heartbeat after registration
          register_signal_handlers
          set_procline
        end
      end
    end

    # Safely wrap instrumentation - continue even if instrumentation fails
    def safe_instrument(event_name, **metadata, &block)
      OutboxRelay.instrument(event_name, **metadata, &block)
    rescue StandardError => e
      # DEBUG: Instrumentation failure doesn't affect operation - continue anyway
      OutboxRelay.logger.debug(
        event_name: 'instrumentation_failed',
        failed_event: event_name,
        error: e.message,
        error_class: e.class.name
      )
      # Continue with the operation even if instrumentation failed
      block.call({}) if block_given?
    end

    def start_workers
      total_expected = configuration.workers.sum(&:partition_count)

      # Load the kill-switch state once before forking. Disabled consumer groups
      # are skipped here and guarded again in #start_worker so crash/backoff
      # restarts cannot bring them back while disabled.
      @disabled_consumer_groups = ConsumerControl.disabled_consumer_groups
      announce_disabled_consumer_groups_at_boot

      configuration.workers.each do |worker_config|
        next if consumer_group_disabled?(worker_config.consumer_group)

        worker_config.partitions.each do |partition_key|
          start_worker(worker_config, partition_key)
        end
      end

      # Report startup status including any failures
      return unless @failed_worker_starts.any?

      OutboxRelay.logger.error(
        event_name: 'supervisor_boot_incomplete',
        total_expected: total_expected,
        running_workers: forks.size,
        failed_workers: @failed_worker_starts.size,
        failed_details: @failed_worker_starts.map do |f|
          {
            topic: f[:worker_config].topic,
            partition_key: f[:partition_key],
            error: f[:error]
          }
        end
      )

      OutboxRelay::Instrumentation::Supervisor.boot_incomplete(
        total_expected: total_expected,
        running_workers: forks.size,
        failed_workers: @failed_worker_starts.size,
        failed_details: @failed_worker_starts
      )
    end

    # Fork Safety - Critical considerations for forking workers
    #
    # ## Why Fork?
    #
    # Fork-based worker architecture provides:
    #   1. **Fault isolation**: Worker crash doesn't affect supervisor or other workers
    #   2. **Resource limits**: Each worker has separate memory space
    #   3. **Clean restarts**: Kill worker process → all resources freed
    #   4. **Parallel processing**: True parallelism (not GIL-limited like threads)
    #
    # Alternative (threads) has problems:
    #   - Ruby GIL prevents parallel CPU usage
    #   - Worker crash can corrupt supervisor state
    #   - Memory leaks accumulate across all workers
    #
    # ## Fork Safety Challenges
    #
    # Forking is safe ONLY if parent process hasn't initialized certain resources:
    #
    # ### Database Connections (ALL PLATFORMS)
    #   - Problem: Parent's connection copied to child → both use same socket
    #   - Symptom: "Connection closed by server" or silent data corruption
    #   - Solution: Close all connections before fork, reconnect after
    #   - Implementation: Worker.boot calls reconnect_after_fork
    #
    # ### macOS-Specific Issues (Objective-C Runtime)
    #   - Problem: macOS Core Foundation uses locks that break across fork
    #   - Symptom: Worker hangs on first database query (gssencmode authentication)
    #   - Solution: Set environment variables BEFORE Ruby starts:
    #       OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
    #       PGGSSENCMODE=disable
    #   - Implementation: tasks.rb checks and re-execs if needed
    #
    # ### Linux glibc Issues (Rare)
    #   - Problem: glibc malloc locks can deadlock after fork
    #   - Symptom: Worker hangs on first memory allocation
    #   - Solution: Usually automatic, but can require malloc tunables
    #
    # ## Our Fork Safety Strategy
    #
    # 1. **Lazy Loading**: Don't load AR models or establish connections before fork
    #    - Configuration.load_workers_config uses STRING class names, not constants
    #    - Worker.boot reconnects database after fork
    #
    # 2. **Environment Check**: Verify macOS environment before starting
    #    - CLI.check_macos_fork_safety warns if variables missing
    #    - tasks.rb re-execs with correct environment
    #
    # 3. **Error Handling**: Catch fork failures gracefully
    #    - System out of resources → log and continue
    #    - Restart backoff prevents fork storms
    #
    # ## Fork Lifecycle
    #
    #   Time | Supervisor (parent)              | Worker (child)
    #   -----|----------------------------------|---------------------------
    #   T1   | Create Worker object             |
    #   T2   | Call fork()                      | [process created]
    #   T3   | Receive child PID                | Inherit supervisor's memory
    #   T4   | Store PID in @forks hash         | Set process name ($0)
    #   T5   | Continue supervisor loop         | Close parent's DB connections
    #   T6   |                                  | Reconnect to database
    #   T7   |                                  | Register as worker
    #   T8   |                                  | Start polling loop
    #   T9   | Monitor child with waitpid       | Process events
    #   T10  | [if child crashes]               | [worker exits]
    #   T11  | Receive SIGCHLD                  |
    #   T12  | Restart worker with backoff      |
    #
    # ## Production Considerations
    #
    # - **Memory**: Each worker uses ~50-100MB. Plan capacity accordingly.
    # - **File Descriptors**: Each worker opens DB connection. Check ulimit.
    # - **PID Limits**: 100 workers = 101 processes. Ensure PID space available.
    # - **Restart Policy**: Workers restart after max_loops to prevent memory leaks.
    #
    # ## Example Deployment
    #
    #   2 topics × 4 partitions × 2 consumer groups = 16 workers
    #   16 workers × 80MB each = 1.28GB RAM
    #   Plus supervisor: ~50MB
    #   Total: ~1.3GB RAM minimum
    #
    def start_worker(worker_config, partition_key)
      # Kill switch: never fork a worker whose consumer group is disabled.
      # This single guard covers the initial boot, crash restarts, and
      # backoff restarts. Re-enable works because #enforce_consumer_controls
      # removes the group from @disabled_consumer_groups before restarting.
      return if consumer_group_disabled?(worker_config.consumer_group)

      worker_params = worker_config.instantiate(partition_key: partition_key).merge(
        polling_interval: configuration.polling_interval,
        batch_size: configuration.batch_size,
        max_loops: configuration.max_loops
      )

      worker = Worker.new(**worker_params)
      worker.mode = :fork

      # Pass supervisor's DB record to worker before fork
      # This allows worker to register with correct supervisor_id
      # Avoids PID-based lookup which fails in multi-container deployments
      worker.supervised_by(@db_process)

      begin
        pid = fork do
          # In child process (forked worker)
          # CRITICAL: Do NOT access parent's database connections here!
          # Worker.boot will reconnect safely.
          # Worker handles all errors internally after boot completes
          $0 = "outbox_relay worker: #{worker.name}"
          worker.start
        end

        # Check if fork succeeded
        raise OutboxRelay::Error, 'fork() returned nil - system may be out of resources (ENOMEM or EAGAIN)' if pid.nil?

        # In parent process
        worker_configs[pid] = worker_config
        forks[pid] = {
          worker: worker,
          partition_key: partition_key,
          started_at: Time.current
        }

        safe_instrument(
          :worker_forked,
          supervisor_pid: ::Process.pid,
          worker_pid: pid,
          worker_name: worker.name,
          consumer_class: worker_config.consumer_class,
          topic: worker_config.topic,
          partition_key: partition_key
        )
      rescue StandardError => e
        OutboxRelay.logger.error(
          event_name: 'fork_system_error',
          worker_name: worker.name,
          error: e.message,
          error_class: e.class.name,
          backtrace: e.backtrace&.first(10)&.join("\n")
        )

        OutboxRelay::Instrumentation::Supervisor.fork_error(
          e,
          worker_name: worker.name,
          consumer_class: worker_config.consumer_class
        )

        # Track failed worker start for visibility
        @failed_worker_starts << {
          worker_config: worker_config,
          partition_key: partition_key,
          failed_at: Time.current,
          error: e.message
        }

        # Don't crash supervisor - log and continue with other workers
        OutboxRelay.logger.error(
          event_name: 'worker_fork_abandoned',
          worker_name: worker.name,
          message: 'Failed to fork worker - continuing with other workers'
        )
      end
    end

    def supervise
      loop do
        break if stopped?

        set_procline
        process_signal_queue

        next if stopped?

        reap_and_restart_terminated_forks
        restart_workers_after_backoff
        if should_check_health?
          enforce_consumer_controls
          check_partition_health
        end
        interruptible_sleep(1.second)
      end
    ensure
      shutdown
    end

    # ============================================================================
    # Partition Health Monitoring
    # ============================================================================
    # Periodically checks partition health and emits instrumentation events
    # for orphaned partitions (no active worker) and high-lag partitions.
    #
    # This enables external monitoring systems (Datadog, Sentry, etc.) to
    # detect and alert on partition issues before they cause production problems.

    def should_check_health?
      Time.current - @last_health_check >= health_check_interval
    end

    def health_check_interval
      configuration.monitoring_config[:orphan_check_interval]&.seconds || DEFAULT_HEALTH_CHECK_INTERVAL
    end

    # ============================================================================
    # Consumer Group Kill Switch
    # ============================================================================
    # DB-flag enforcement: the `outbox_relay_consumer_controls` table (managed by
    # the host app) marks base consumer_groups as disabled. On each health-check
    # tick we diff the current disabled set against what we last enforced and:
    #   - stop running workers of newly-disabled groups (they are not restarted,
    #     thanks to the guards in #start_worker / #restart_fork)
    #   - restart workers of newly-enabled groups
    #   - emit a transition notification for each change
    # Other consumer groups are left untouched.

    def consumer_group_disabled?(consumer_group)
      @disabled_consumer_groups.include?(consumer_group)
    end

    def enforce_consumer_controls
      previous = @disabled_consumer_groups
      current = ConsumerControl.disabled_consumer_groups
      @disabled_consumer_groups = current

      newly_disabled = current - previous
      newly_enabled = previous - current
      return if newly_disabled.empty? && newly_enabled.empty?

      newly_disabled.each do |consumer_group|
        Instrumentation::ConsumerGroup.disabled(consumer_group: consumer_group)
        stop_workers_for_consumer_group(consumer_group)
      end

      newly_enabled.each do |consumer_group|
        Instrumentation::ConsumerGroup.enabled(consumer_group: consumer_group)
        start_workers_for_consumer_group(consumer_group)
      end
    rescue StandardError => e
      # Never let kill-switch enforcement take down the supervisor.
      OutboxRelay.logger.error(
        event_name: 'consumer_control_enforcement_failed',
        error: e.message,
        error_class: e.class.name,
        backtrace: e.backtrace&.first(5)&.join("\n")
      )
    end

    # Send TERM to every running worker belonging to the given consumer group.
    # Terminated workers are reaped by the main loop and NOT restarted, because
    # the group is now in @disabled_consumer_groups (see #restart_fork).
    def stop_workers_for_consumer_group(consumer_group)
      pids = worker_configs.select { |_pid, wc| wc.consumer_group == consumer_group }.keys
      return if pids.empty?

      OutboxRelay.logger.info(
        event_name: 'consumer_group_workers_stopping',
        consumer_group: consumer_group,
        worker_pids: pids,
        reason: 'kill switch disabled consumer group'
      )

      signal_processes(pids, :TERM)
    end

    # Start all configured workers for a consumer group that was just re-enabled.
    # @disabled_consumer_groups no longer contains it, so the #start_worker guard
    # passes. Partition claiming prevents duplicate processing if a stopped
    # worker has not finished terminating yet.
    def start_workers_for_consumer_group(consumer_group)
      OutboxRelay.logger.info(
        event_name: 'consumer_group_workers_starting',
        consumer_group: consumer_group,
        reason: 'kill switch re-enabled consumer group'
      )

      configuration.workers.each do |worker_config|
        next unless worker_config.consumer_group == consumer_group

        worker_config.partitions.each do |partition_key|
          start_worker(worker_config, partition_key)
        end
      end
    end

    # At boot, surface any consumer groups that are already disabled so monitoring
    # reflects the kill-switch state. These are already in @disabled_consumer_groups
    # so the first health-check tick will not re-emit them as "newly disabled".
    def announce_disabled_consumer_groups_at_boot
      configured_groups = configuration.workers.map(&:consumer_group).uniq
      configured_groups.each do |consumer_group|
        next unless consumer_group_disabled?(consumer_group)

        OutboxRelay.logger.warn(
          event_name: 'consumer_group_disabled_at_boot',
          consumer_group: consumer_group,
          message: 'Consumer group disabled via kill switch - workers will not start'
        )
        Instrumentation::ConsumerGroup.disabled(consumer_group: consumer_group, phase: 'boot')
      end
    end

    def check_partition_health
      @last_health_check = Time.current

      monitor = PartitionMonitor.new(configuration)
      report = monitor.health_report

      # Emit events for orphaned partitions WITH PENDING EVENTS ONLY
      # Orphaned partitions with lag=0 are normal - no events to process, worker released claim.
      # Only alert when lag > 0 (events waiting but no worker processing them).
      #
      # IMPORTANT: Re-check each partition before emitting to avoid race conditions.
      # Between the initial health_report query and now, a worker may have claimed the partition.
      # This prevents false alerts during container failover when new workers are claiming partitions.
      orphaned_with_lag = report[:orphaned].select { |p| p[:lag].positive? }
      confirmed_orphaned = orphaned_with_lag.select { |p| still_orphaned?(p) }
      confirmed_orphaned.each do |partition|
        Instrumentation::PartitionHealth.orphaned(
          consumer_group: partition[:consumer_group],
          topic: partition[:topic],
          partition_key: partition[:partition_key],
          claimed_until: partition[:claimed_until],
          last_consumed_at: partition[:last_consumed_at],
          lag: partition[:lag]
        )
      end

      # Emit events for stale workers
      report[:high_lag].select { |p| p[:status] == :stale }.each do |partition|
        Instrumentation::PartitionHealth.stale_worker(
          consumer_group: partition[:consumer_group],
          topic: partition[:topic],
          partition_key: partition[:partition_key],
          last_heartbeat_at: partition[:heartbeat_at],
          stale_threshold: configuration.monitoring_config[:stale_worker_timeout] || 60
        )
      end

      # Emit events for high-lag partitions (excluding orphaned - they already have an alert)
      lag_threshold = configuration.monitoring_config[:lag_alert_threshold] || 100
      report[:high_lag].reject { |p| p[:status] == :orphaned }.each do |partition|
        Instrumentation::PartitionHealth.high_lag(
          consumer_group: partition[:consumer_group],
          topic: partition[:topic],
          partition_key: partition[:partition_key],
          lag: partition[:lag],
          threshold: lag_threshold
        )
      end

      # Log summary if any actual issues found (confirmed orphaned with lag, or high lag)
      if confirmed_orphaned.any? || report[:high_lag].any?
        OutboxRelay.logger.warn(
          event_name: 'partition_health_issues_detected',
          total_partitions: report[:total],
          active_partitions: report[:active],
          stale_partitions: report[:stale],
          orphaned_count: confirmed_orphaned.size,
          high_lag_count: report[:high_lag].size
        )
      end
    rescue StandardError => e
      # Don't let health check failures affect the supervisor
      OutboxRelay.logger.error(
        event_name: 'partition_health_check_failed',
        error: e.message,
        error_class: e.class.name,
        backtrace: e.backtrace&.first(5)&.join("\n")
      )
    end

    def reap_and_restart_terminated_forks
      loop do
        pid, status = ::Process.waitpid2(-1, ::Process::WNOHANG)
        break unless pid

        restart_fork(pid, status)
      end
    rescue Errno::ECHILD
      # No child processes
    end

    def restart_workers_after_backoff
      return if @restart_backoff_until.empty?

      now = Time.current
      @restart_backoff_until.each do |worker_key, backoff_info|
        next if backoff_info[:restart_at] > now

        # Backoff period has elapsed - restart worker
        worker_config = backoff_info[:worker_config]
        partition_key = backoff_info[:partition_key]

        # DEBUG: Normal operation in multi-container deployments - worker retrying after
        # another container held the partition claim. Not a warning, expected behavior.
        OutboxRelay.logger.debug(
          event_name: 'worker_restarting_after_backoff',
          worker_key: worker_key,
          partition_key: partition_key,
          topic: worker_config.topic
        )

        start_worker(worker_config, partition_key)
        @restart_backoff_until.delete(worker_key)
      end
    end

    def restart_fork(pid, status)
      safe_instrument(:restart_fork, supervisor_pid: ::Process.pid, pid: pid, status: status) do |payload|
        if (fork_info = forks.delete(pid)) && (worker_config = worker_configs.delete(pid))
          payload[:fork_info] = fork_info
          payload[:exit_status] = status.exitstatus
          payload[:signaled] = status.signaled?

          log_fork_terminated(pid, fork_info, status)

          # Generate unique key for this worker
          # IMPORTANT: Must include consumer_group to avoid collision when multiple
          # consumer groups consume the same topic (e.g., shipstation_order_fulfillment
          # and monday_order_lifecycle both consuming order_lifecycle topic)
          worker_key = "#{worker_config.consumer_group}-#{worker_config.topic}-#{fork_info[:partition_key]}"

          # Restart the worker unless we're shutting down
          unless stopped?
            # Kill switch: if this worker's consumer group has been disabled,
            # let it stay dead. Clear any restart bookkeeping so it neither
            # restarts now nor lingers in the backoff queue.
            if consumer_group_disabled?(worker_config.consumer_group)
              @restart_attempts.delete(worker_key)
              @restart_backoff_until.delete(worker_key)
              OutboxRelay.logger.info(
                event_name: 'worker_not_restarted_consumer_group_disabled',
                supervisor_pid: ::Process.pid,
                worker_name: fork_info[:worker]&.name,
                consumer_group: worker_config.consumer_group,
                partition_key: fork_info[:partition_key]
              )
              return
            end

            if status.success?
              # Reset backoff on successful exit
              @restart_attempts.delete(worker_key)
              @restart_backoff_until.delete(worker_key)
              start_worker(worker_config, fork_info[:partition_key])
            elsif claim_unavailable_exit_status?(status)
              delay_restart_for_partition_claim(
                worker_key: worker_key,
                worker_config: worker_config,
                partition_key: fork_info[:partition_key],
                worker_name: fork_info[:worker]&.name
              )
            else
              # Exponential backoff for failed workers
              @restart_attempts[worker_key] += 1
              attempts = @restart_attempts[worker_key]

              # Check if we've exceeded max attempts
              if attempts > 10
                OutboxRelay.logger.error(
                  event_name: 'worker_restart_abandoned',
                  worker_name: fork_info[:worker]&.name,
                  restart_attempts: attempts,
                  reason: 'Too many restart attempts - indicates systemic issue',
                  action: 'Manual intervention required'
                )

                OutboxRelay::Instrumentation::Supervisor.restart_abandoned(
                  worker_name: fork_info[:worker]&.name,
                  worker_key: worker_key,
                  exit_status: status.exitstatus,
                  restart_attempts: attempts
                )

                # Don't restart - stop trying
                return
              end

              # Calculate backoff
              backoff_seconds = [2**(attempts - 1), 60].min # Max 60 seconds
              restart_at = Time.current + backoff_seconds
              @restart_backoff_until[worker_key] = {
                restart_at: restart_at,
                worker_config: worker_config,
                partition_key: fork_info[:partition_key]
              }

              OutboxRelay.logger.warn(
                event_name: 'worker_restart_delayed',
                worker_name: fork_info[:worker]&.name,
                restart_attempts: attempts,
                backoff_seconds: backoff_seconds,
                restart_at: restart_at.iso8601,
                exit_status: status.exitstatus
              )

              # Don't block - check backoff timing in main loop instead
              # This allows supervisor to continue monitoring other workers and processing signals
              # Worker will be restarted when backoff period expires (checked in run loop)
            end
          end
        end
      end
    end

    def delay_restart_for_partition_claim(worker_key:, worker_config:, partition_key:, worker_name:)
      @restart_attempts.delete(worker_key)

      restart_at = Time.current + CLAIM_UNAVAILABLE_RETRY_DELAY
      @restart_backoff_until[worker_key] = {
        restart_at: restart_at,
        worker_config: worker_config,
        partition_key: partition_key
      }

      # DEBUG: This is expected in multi-container deployments where workers compete
      # for partitions. Not a warning - normal operation when another container holds the claim.
      OutboxRelay.logger.debug(
        event_name: 'worker_restart_delayed_claim_unavailable',
        worker_key: worker_key,
        worker_name: worker_name,
        partition_key: partition_key,
        topic: worker_config.topic,
        restart_in_seconds: CLAIM_UNAVAILABLE_RETRY_DELAY.to_i,
        restart_at: restart_at.iso8601
      )
    end

    def claim_unavailable_exit_status?(status)
      status.exitstatus == CLAIM_UNAVAILABLE_EXIT_STATUS
    rescue NoMethodError
      false
    end

    def log_fork_terminated(pid, fork_info, status)
      if status.success?
        OutboxRelay.logger.info(
          event_name: 'worker_terminated_successfully',
          supervisor_pid: ::Process.pid,
          worker_pid: pid,
          worker_name: fork_info[:worker]&.name,
          uptime: Time.current - fork_info[:started_at]
        )
      elsif claim_unavailable_exit_status?(status)
        # DEBUG: Expected in multi-container deployments - worker exited because
        # partition was already claimed by another container. Not an error.
        OutboxRelay.logger.debug(
          event_name: 'worker_terminated_claim_unavailable',
          supervisor_pid: ::Process.pid,
          worker_pid: pid,
          worker_name: fork_info[:worker]&.name,
          exit_status: status.exitstatus,
          partition_key: fork_info[:partition_key],
          uptime: Time.current - fork_info[:started_at]
        )
      else
        OutboxRelay.logger.error(
          event_name: 'worker_terminated_with_error',
          supervisor_pid: ::Process.pid,
          worker_pid: pid,
          worker_name: fork_info[:worker]&.name,
          exit_status: status.exitstatus,
          signaled: status.signaled?,
          signal: status.termsig,
          uptime: Time.current - fork_info[:started_at]
        )
      end
    end

    def terminate_gracefully
      safe_instrument(
        :graceful_termination,
        process_id: process_id,
        supervisor_pid: ::Process.pid,
        worker_pids: forks.keys
      ) do |payload|
        stop

        # Send TERM to all workers
        failed = signal_processes(forks.keys, :TERM)

        if failed.any?
          OutboxRelay.logger.error(
            event_name: 'graceful_shutdown_signal_failures',
            failed_pids: failed,
            message: 'Some workers did not receive TERM signal'
          )
          payload[:signal_failures] = failed
        end

        # Wait for workers to terminate
        deadline = Time.current + OutboxRelay.shutdown_timeout

        while Time.current < deadline && forks.any?
          reap_terminated_forks
          sleep(0.5)
        end

        unless forks.empty?
          payload[:shutdown_timeout_exceeded] = true
          payload[:remaining_workers] = forks.keys
          terminate_immediately
        end
      end
    end

    def terminate_immediately
      safe_instrument(
        :immediate_termination,
        process_id: process_id,
        supervisor_pid: ::Process.pid,
        worker_pids: forks.keys
      ) do |payload|
        # Send KILL to all remaining workers
        failed = signal_processes(forks.keys, :KILL)

        if failed.any?
          OutboxRelay.logger.error(
            event_name: 'kill_signal_failures',
            failed_pids: failed,
            message: 'Some workers did not receive KILL signal - may be zombie processes'
          )
          payload[:signal_failures] = failed
        end

        # Reap all children
        reap_terminated_forks
      end
    end

    def reap_terminated_forks
      loop do
        pid, status = ::Process.waitpid2(-1, ::Process::WNOHANG)
        break unless pid

        if (fork_info = forks.delete(pid))
          log_fork_terminated(pid, fork_info, status)
        end
        worker_configs.delete(pid)
      end
    rescue Errno::ECHILD
      # All children reaped
    end

    def shutdown
      safe_instrument(:shutdown_supervisor, process: self) do
        run_callbacks(:shutdown) do
          stop_heartbeat # Stop heartbeat before deregistration
          restore_signal_handlers # Restore original signal handlers
          deregister
        end
      end
    end

    def set_procline
      procline("supervising #{forks.size} workers (#{configuration.workers.sum(&:partition_count)} total)")
    end

    def sync_std_streams
      $stdout.sync = $stderr.sync = true
    end

    def supervised_worker_pids
      forks.keys
    end

    # Re-check if a partition is still orphaned before emitting an alert.
    # This prevents race conditions during container failover when workers
    # are claiming partitions between the health report query and alert emission.
    #
    # @param partition [Hash] Partition info from health_report[:orphaned]
    # @return [Boolean] true if partition is still orphaned, false if now claimed
    def still_orphaned?(partition)
      consumer_group_with_partition = "#{partition[:consumer_group]}_p#{partition[:partition_key]}"

      offset = ConsumerOffset.find_by(
        consumer_group: consumer_group_with_partition,
        topic: partition[:topic]
      )

      # If record doesn't exist or has no active claim, it's still orphaned
      return true unless offset

      !offset.claimed?
    rescue StandardError => e
      # On error, assume still orphaned to be safe (will emit alert)
      OutboxRelay.logger.debug(
        event_name: 'orphaned_recheck_failed',
        consumer_group: partition[:consumer_group],
        topic: partition[:topic],
        partition_key: partition[:partition_key],
        error: e.message
      )
      true
    end

    # Lifecycle callbacks

    def prune_dead_processes
      pruned = OutboxRelay::Process.prune_dead_processes(timeout: 60)
      return unless pruned.positive?

      OutboxRelay.logger.info(
        event_name: 'dead_processes_pruned',
        process_id: process_id,
        pruned_count: pruned
      )
    rescue StandardError => e
      OutboxRelay.logger.error(
        event_name: 'dead_processes_prune_failed',
        process_id: process_id,
        error: e.message,
        error_class: e.class.name,
        backtrace: e.backtrace&.first(10)&.join("\n")
      )
    end

    def log_supervisor_start
      OutboxRelay.logger.info(
        event_name: 'supervisor_started',
        process_id: process_id,
        supervisor_pid: ::Process.pid,
        hostname: hostname,
        workers_count: configuration.workers.sum(&:partition_count),
        polling_interval: configuration.polling_interval,
        batch_size: configuration.batch_size
      )
    end

    def log_supervisor_stop
      OutboxRelay.logger.info(
        event_name: 'supervisor_stopped',
        process_id: process_id,
        supervisor_pid: ::Process.pid,
        uptime: metadata[:uptime]
      )
    end
  end
end
