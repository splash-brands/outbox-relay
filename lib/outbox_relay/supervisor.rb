# frozen_string_literal: true

module OutboxRelay
  class Supervisor < Processes::Base
    include Processes::Signals

    after_boot :log_supervisor_start
    before_shutdown :log_supervisor_stop

    class << self
      def start(**options)
        configuration = Configuration.new(**options)

        if configuration.valid?
          new(configuration).tap(&:start)
        else
          abort("OutboxRelay configuration errors:\n" + configuration.errors.join("\n") + "\nExiting...")
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

      super()
    end

    def start
      boot
      start_workers
      supervise
    end

    def stop
      super
      custom_logger.info(
        event_name: "supervisor_stopping",
        process_id: process_id,
        supervisor_pid: ::Process.pid,
      )
    end

    private

    def boot
      safe_instrument(:start_supervisor, process: self) do
        run_callbacks(:boot) do
          sync_std_streams
          register
          start_heartbeat  # Start automatic heartbeat after registration
          register_signal_handlers
          set_procline
        end
      end
    end

    # Safely wrap instrumentation - continue even if instrumentation fails
    def safe_instrument(event_name, **metadata, &block)
      OutboxRelay.instrument(event_name, **metadata, &block)
    rescue => e
      custom_logger.warn(
        event_name: "instrumentation_failed",
        failed_event: event_name,
        error: e.message,
        error_class: e.class.name
      )
      # Continue with the operation even if instrumentation failed
      block.call({}) if block_given?
    end

    def start_workers
      total_expected = configuration.workers.sum(&:partition_count)

      configuration.workers.each do |worker_config|
        worker_config.partitions.each do |partition_key|
          start_worker(worker_config, partition_key)
        end
      end

      # Report startup status including any failures
      if @failed_worker_starts.any?
        custom_logger.error(
          event_name: "supervisor_boot_incomplete",
          total_expected: total_expected,
          running_workers: forks.size,
          failed_workers: @failed_worker_starts.size,
          failed_details: @failed_worker_starts.map { |f|
            {
              topic: f[:worker_config].topic,
              partition_key: f[:partition_key],
              error: f[:error]
            }
          }
        )

        OutboxRelay::Instrumentation::Supervisor.boot_incomplete(
          total_expected: total_expected,
          running_workers: forks.size,
          failed_workers: @failed_worker_starts.size,
          failed_details: @failed_worker_starts
        )
      end
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
      worker_params = worker_config.instantiate(partition_key: partition_key).merge(
        polling_interval: configuration.polling_interval,
        batch_size: configuration.batch_size,
        max_loops: configuration.max_loops,
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
        if pid.nil?
          raise OutboxRelay::Error, "fork() returned nil - system may be out of resources (ENOMEM or EAGAIN)"
        end

        # In parent process
        worker_configs[pid] = worker_config
        forks[pid] = {
          worker: worker,
          partition_key: partition_key,
          started_at: Time.current,
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

      rescue => e
        custom_logger.error(
          event_name: "fork_system_error",
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
        custom_logger.error(
          event_name: "worker_fork_abandoned",
          worker_name: worker.name,
          message: "Failed to fork worker - continuing with other workers"
        )
      end
    end

    def supervise
      loop do
        break if stopped?

        set_procline
        process_signal_queue

        unless stopped?
          reap_and_restart_terminated_forks
          restart_workers_after_backoff
          interruptible_sleep(1.second)
        end
      end
    ensure
      shutdown
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

        custom_logger.info(
          event_name: "worker_restarting_after_backoff",
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
          worker_key = "#{worker_config.topic}-#{fork_info[:partition_key]}"

          # Restart the worker unless we're shutting down
          unless stopped?
            if status.success?
              # Reset backoff on successful exit
              @restart_attempts.delete(worker_key)
              @restart_backoff_until.delete(worker_key)
              start_worker(worker_config, fork_info[:partition_key])
            else
              # Exponential backoff for failed workers
              @restart_attempts[worker_key] += 1
              attempts = @restart_attempts[worker_key]

              # Check if we've exceeded max attempts
              if attempts > 10
                custom_logger.error(
                  event_name: "worker_restart_abandoned",
                  worker_name: fork_info[:worker]&.name,
                  restart_attempts: attempts,
                  reason: "Too many restart attempts - indicates systemic issue",
                  action: "Manual intervention required"
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
              backoff_seconds = [2 ** (attempts - 1), 60].min  # Max 60 seconds
              restart_at = Time.current + backoff_seconds
              @restart_backoff_until[worker_key] = {
                restart_at: restart_at,
                worker_config: worker_config,
                partition_key: fork_info[:partition_key]
              }

              custom_logger.warn(
                event_name: "worker_restart_delayed",
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

    def log_fork_terminated(pid, fork_info, status)
      if status.success?
        custom_logger.info(
          event_name: "worker_terminated_successfully",
          supervisor_pid: ::Process.pid,
          worker_pid: pid,
          worker_name: fork_info[:worker]&.name,
          uptime: Time.current - fork_info[:started_at],
        )
      else
        custom_logger.error(
          event_name: "worker_terminated_with_error",
          supervisor_pid: ::Process.pid,
          worker_pid: pid,
          worker_name: fork_info[:worker]&.name,
          exit_status: status.exitstatus,
          signaled: status.signaled?,
          signal: status.termsig,
          uptime: Time.current - fork_info[:started_at],
        )
      end
    end

    def terminate_gracefully
      safe_instrument(
        :graceful_termination,
        process_id: process_id,
        supervisor_pid: ::Process.pid,
        worker_pids: forks.keys,
      ) do |payload|
        stop

        # Send TERM to all workers
        failed = signal_processes(forks.keys, :TERM)

        if failed.any?
          custom_logger.error(
            event_name: "graceful_shutdown_signal_failures",
            failed_pids: failed,
            message: "Some workers did not receive TERM signal"
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
        worker_pids: forks.keys,
      ) do |payload|
        # Send KILL to all remaining workers
        failed = signal_processes(forks.keys, :KILL)

        if failed.any?
          custom_logger.error(
            event_name: "kill_signal_failures",
            failed_pids: failed,
            message: "Some workers did not receive KILL signal - may be zombie processes"
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
          stop_heartbeat  # Stop heartbeat before deregistration
          restore_signal_handlers  # Restore original signal handlers
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

    # Lifecycle callbacks

    def log_supervisor_start
      custom_logger.info(
        event_name: "supervisor_started",
        process_id: process_id,
        supervisor_pid: ::Process.pid,
        hostname: hostname,
        workers_count: configuration.workers.sum(&:partition_count),
        polling_interval: configuration.polling_interval,
        batch_size: configuration.batch_size,
      )
    end

    def log_supervisor_stop
      custom_logger.info(
        event_name: "supervisor_stopped",
        process_id: process_id,
        supervisor_pid: ::Process.pid,
        uptime: metadata[:uptime],
      )
    end

    def metadata
      super.merge(
        workers_count: forks.size,
        uptime: Time.current - (@started_at ||= Time.current),
      )
    end
  end
end
