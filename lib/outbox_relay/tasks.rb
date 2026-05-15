# frozen_string_literal: true

require 'English'
namespace :outbox_relay do
  desc 'Show OutboxRelay configuration'
  task config: :environment do
    require 'outbox_relay'

    puts 'OutboxRelay Configuration'
    puts '=' * 50
    puts "Version: #{OutboxRelay::VERSION}"
    puts "Shutdown timeout: #{OutboxRelay.shutdown_timeout}s"
    puts "Silence polling: #{OutboxRelay.silence_polling?}"
    puts ''

    config = OutboxRelay::Configuration.new
    puts 'Workers Configuration:'
    puts '-' * 50

    config.workers.each do |worker_config|
      puts "Consumer Group: #{worker_config.consumer_group}"
      puts "  Topic: #{worker_config.topic}"
      puts "  Consumer Class: #{worker_config.consumer_class}"
      puts "  Partition Count: #{worker_config.partition_count}"
      puts "  Partitions: #{worker_config.partitions.join(', ')}"
      puts ''
    end

    puts "Total Workers: #{config.workers.sum(&:partition_count)}"
  end

  desc 'Check OutboxRelay status'
  task status: :environment do
    require 'outbox_relay'

    # Get processes using ps command (more reliable than registry in development)
    begin
      ps_output = `ps aux | grep outbox_relay | grep -v grep 2>/dev/null`

      if $CHILD_STATUS.exitstatus != 0 && $CHILD_STATUS.exitstatus != 1 # 1 is ok (no matches)
        raise "ps command failed with exit status #{$CHILD_STATUS.exitstatus}"
      end
    rescue StandardError => e
      puts '✗ ERROR: Failed to check process status'
      puts "  #{e.message} (#{e.class.name})"
      puts ''
      puts 'Try manually: ps aux | grep outbox_relay'

      OutboxRelay.logger.error(
        event_name: 'rake_status_ps_failed',
        error: e.message,
        error_class: e.class.name,
        backtrace: e.backtrace&.first(5)&.join("\n")
      )

      OutboxRelay::Instrumentation::Tasks.task_error(
        e,
        task_name: 'outbox_relay:status',
        severity: 'high'
      )

      exit 1
    end

    if ps_output.empty?
      puts 'No OutboxRelay processes running'
      puts ''
      puts 'To start: ./bin/outbox_relay'
    else
      lines = ps_output.lines

      # Parse and group processes
      supervisors = []
      workers = []

      lines.each do |line|
        parts = line.strip.split(/\s+/)
        user = parts[0]
        pid = parts[1]
        cpu = parts[2]
        mem = parts[3]
        # Command starts after the 10th column
        command = parts[10..].join(' ')

        # Skip if this is a rake task or shell command
        next if command.include?('rake outbox_relay')
        next if command.include?('/bin/zsh')
        next if command.include?('/bin/bash')

        process_info = {
          user: user,
          pid: pid,
          cpu: cpu,
          mem: mem,
          command: command
        }

        if command.include?('supervisor')
          supervisors << process_info
        elsif command.include?('worker')
          workers << process_info
        end
      end

      total_processes = supervisors.size + workers.size

      if total_processes.zero?
        puts 'No OutboxRelay processes running'
        puts ''
        puts 'To start: ./bin/outbox_relay'
        next
      end

      puts "OutboxRelay Processes (#{total_processes})"
      puts '=' * 120

      # Display supervisors
      if supervisors.any?
        puts "\nSupervisors (#{supervisors.size}):"
        puts '-' * 120

        supervisors.each do |proc|
          puts format(
            '  PID: %-6s | CPU: %5s%% | MEM: %5s%% | %s',
            proc[:pid],
            proc[:cpu],
            proc[:mem],
            proc[:command]
          )
        end
      end

      # Display workers
      if workers.any?
        puts "\nWorkers (#{workers.size}):"
        puts '-' * 120

        workers.each do |proc|
          puts format(
            '  PID: %-6s | CPU: %5s%% | MEM: %5s%% | %s',
            proc[:pid],
            proc[:cpu],
            proc[:mem],
            proc[:command]
          )
        end
      end

      # Summary
      puts "\n#{'=' * 120}"
      puts 'Summary:'
      puts "  Supervisors: #{supervisors.size}"
      puts "  Workers: #{workers.size}"
      puts "  Total: #{total_processes}"
      puts ''
      puts 'To stop: bundle exec rake outbox_relay:stop'
      puts 'To check lag: bundle exec rake outbox_relay:lag'
    end
  end

  desc 'Stop all OutboxRelay processes gracefully'
  task stop: :environment do
    require 'outbox_relay'

    puts "\nStopping OutboxRelay..."
    puts '=' * 80

    processes = OutboxRelay::Processes::Registrable::ProcessRegistry.all
    supervisor_processes = processes.select { |p| p[:kind] == 'supervisor' }

    if supervisor_processes.empty?
      puts 'No supervisor processes found (already stopped?)'
      return
    end

    failed_signals = []

    supervisor_processes.each do |process|
      ::Process.kill('TERM', process[:pid])
      puts "✓ Sent TERM signal to #{process[:name]} (PID: #{process[:pid]})"

      OutboxRelay.logger.info(
        event_name: 'rake_stop_signal_sent',
        process_name: process[:name],
        pid: process[:pid]
      )
    rescue Errno::ESRCH
      puts "⚠ Process #{process[:name]} (PID: #{process[:pid]}) not found (already stopped)"

      # DEBUG: Process already stopped is not a problem, just informational
      OutboxRelay.logger.debug(
        event_name: 'rake_stop_process_not_found',
        process_name: process[:name],
        pid: process[:pid]
      )
    rescue StandardError => e
      puts "✗ ERROR stopping #{process[:name]}: #{e.message}"
      puts "  Error class: #{e.class.name}"
      puts "  This may require manual intervention: kill -TERM #{process[:pid]}"

      OutboxRelay.logger.error(
        event_name: 'rake_stop_failed',
        process_name: process[:name],
        pid: process[:pid],
        error: e.message,
        error_class: e.class.name,
        backtrace: e.backtrace&.first(5)&.join("\n")
      )

      OutboxRelay::Instrumentation::Tasks.task_error(
        e,
        task_name: 'outbox_relay:stop',
        process_name: process[:name],
        pid: process[:pid]
      )

      failed_signals << { pid: process[:pid], name: process[:name], error: e.message }
    end

    puts "\n#{'=' * 80}"
    if failed_signals.any?
      puts "⚠ #{failed_signals.size} process(es) failed to receive TERM signal"
      puts "\nManual cleanup required:"
      failed_signals.each do |failure|
        puts "  kill -TERM #{failure[:pid]}  # #{failure[:name]}"
      end
      exit 1
    else
      puts '✓ All processes signaled successfully'
      puts 'Workers will shut down gracefully (may take up to 30 seconds)'
    end
  end

  desc 'Show OutboxRelay lag for all consumers'
  task lag: :environment do
    require 'outbox_relay'

    config = OutboxRelay::Configuration.new

    puts 'OutboxRelay Consumer Lag'
    puts '=' * 80

    total_lag = 0

    total_errors = 0

    config.workers.each do |worker_config|
      puts "\n#{worker_config.consumer_class} (#{worker_config.topic})"
      puts '-' * 80

      worker_config.partitions.each do |partition_key|
        consumer = worker_config.consumer_class.constantize.new(partition_key: partition_key)
        lag = consumer.lag

        status = case lag
                 when 0 then '✓ OK'
                 when 1..100 then '⚠ LOW'
                 when 101..1000 then '⚠ MEDIUM'
                 else '✗ HIGH'
                 end

        puts format('  Partition %2d: %8d events pending [%s]', partition_key, lag, status)
        total_lag += lag
      rescue StandardError => e
        puts format('  Partition %2d: ✗ ERROR - %s (%s)', partition_key, e.message, e.class.name)
        total_errors += 1

        OutboxRelay.logger.error(
          event_name: 'rake_lag_partition_error',
          consumer_class: worker_config.consumer_class,
          topic: worker_config.topic,
          partition_key: partition_key,
          error: e.message,
          error_class: e.class.name,
          backtrace: e.backtrace&.first(5)&.join("\n")
        )

        OutboxRelay::Instrumentation::Tasks.task_error(
          e,
          task_name: 'outbox_relay:lag',
          consumer_class: worker_config.consumer_class,
          topic: worker_config.topic,
          partition_key: partition_key
        )
      end
    end

    puts "\n#{'=' * 80}"
    puts "Total lag: #{total_lag} events"
    if total_errors.positive?
      puts "⚠ #{total_errors} partition(s) failed to calculate lag"
      exit 1
    end
  end

  desc 'Clean up old processed events and resolved DLQ entries'
  task :cleanup, [:ttl_days] => :environment do |_t, args|
    require 'outbox_relay'

    ttl_days = (args[:ttl_days] || 7).to_i
    cutoff_time = ttl_days.days.ago

    puts 'OutboxRelay Cleanup'
    puts '=' * 80
    puts "TTL: #{ttl_days} days (before #{cutoff_time})"
    puts ''

    # Get all consumer offsets to know what's been processed
    begin
      consumer_offsets = OutboxRelay::ConsumerOffset.all
    rescue StandardError => e
      puts '✗ ERROR: Failed to fetch consumer offsets'
      puts "  #{e.message} (#{e.class.name})"
      puts ''
      puts 'Cannot proceed with cleanup without knowing consumer progress.'

      OutboxRelay.logger.error(
        event_name: 'rake_cleanup_offset_fetch_failed',
        error: e.message,
        error_class: e.class.name,
        backtrace: e.backtrace&.first(10)&.join("\n")
      )

      OutboxRelay::Instrumentation::Tasks.task_error(
        e,
        task_name: 'outbox_relay:cleanup',
        phase: 'offset_fetch',
        severity: 'critical'
      )

      exit 1
    end

    if consumer_offsets.empty?
      puts '⚠  No consumer offsets found - skipping cleanup'
      puts '   (Events are safe until consumers process them)'
      next
    end

    # Find minimum offset across all consumer groups per topic
    min_offsets_by_topic = consumer_offsets.group_by(&:topic).transform_values do |offsets|
      offsets.map(&:last_consumed_sequence).min
    end

    puts 'Consumer Progress:'
    puts '-' * 80
    min_offsets_by_topic.each do |topic, min_sequence|
      puts "  #{topic}: All groups processed up to sequence #{min_sequence}"
    end
    puts ''

    # Delete events that:
    # 1. Are older than TTL
    # 2. Have been processed by ALL consumer groups (sequence <= min_offset)
    # 3. Are not in active DLQ entries
    total_deleted = 0
    total_errors = 0

    min_offsets_by_topic.each do |topic, min_sequence|
      # Get active DLQ event IDs (unresolved or retrying) - we MUST keep these
      active_dlq_event_ids = OutboxRelay::DeadLetterEvent
                             .where(resolution_status: %w[unresolved retrying])
                             .where(original_topic: topic)
                             .pluck(:outbox_relay_outbox_event_id)

      # Delete old events that are fully processed by all consumer groups
      deleted_count = OutboxRelay::OutboxEvent
                      .where(topic: topic)
                      .where('sequence <= ?', min_sequence)
                      .where('created_at < ?', cutoff_time)
                      .where.not(id: active_dlq_event_ids)
                      .delete_all

      puts "  #{topic}: Deleted #{deleted_count} events"
      total_deleted += deleted_count
    rescue StandardError => e
      puts "  #{topic}: ✗ ERROR - #{e.message} (#{e.class.name})"
      total_errors += 1

      OutboxRelay.logger.error(
        event_name: 'rake_cleanup_topic_failed',
        topic: topic,
        min_sequence: min_sequence,
        error: e.message,
        error_class: e.class.name,
        backtrace: e.backtrace&.first(10)&.join("\n")
      )

      OutboxRelay::Instrumentation::Tasks.task_error(
        e,
        task_name: 'outbox_relay:cleanup',
        phase: 'event_deletion',
        topic: topic,
        min_sequence: min_sequence
      )
    end

    # Clean up resolved DLQ entries older than TTL
    begin
      resolved_dlq_deleted = OutboxRelay::DeadLetterEvent
                             .where(resolution_status: %w[resolved reprocessed ignored])
                             .where('created_at < ?', cutoff_time)
                             .delete_all
    rescue StandardError => e
      puts ''
      puts '✗ ERROR: Failed to clean up DLQ entries'
      puts "  #{e.message} (#{e.class.name})"

      OutboxRelay.logger.error(
        event_name: 'rake_cleanup_dlq_failed',
        error: e.message,
        error_class: e.class.name,
        backtrace: e.backtrace&.first(10)&.join("\n")
      )

      OutboxRelay::Instrumentation::Tasks.task_error(
        e,
        task_name: 'outbox_relay:cleanup',
        phase: 'dlq_deletion',
        severity: 'high'
      )

      resolved_dlq_deleted = 0 # Continue with summary
      total_errors += 1
    end

    puts ''
    puts 'DLQ Cleanup:'
    puts '-' * 80
    puts "  Resolved entries: Deleted #{resolved_dlq_deleted} old entries"
    puts ''

    puts '=' * 80
    puts 'Total cleaned:'
    puts "  Events: #{total_deleted}"
    puts "  DLQ entries: #{resolved_dlq_deleted}"
    puts "  Combined: #{total_deleted + resolved_dlq_deleted}"

    if total_errors.positive?
      puts ''
      puts "⚠ #{total_errors} error(s) occurred during cleanup"
      exit 1
    end
  end

  desc 'Prune dead processes from database registry'
  task :prune_processes, [:timeout] => :environment do |_t, args|
    require 'outbox_relay'

    timeout = (args[:timeout] || 60).to_i

    puts 'OutboxRelay Process Cleanup'
    puts '=' * 80
    puts "Timeout: #{timeout} seconds (processes without heartbeat in last #{timeout}s)"
    puts ''

    begin
      dead_count = OutboxRelay::Process.prune_dead_processes(timeout: timeout)

      puts 'Results:'
      puts '-' * 80
      puts "  Dead processes pruned: #{dead_count}"
      puts ''

      if dead_count.positive?
        puts "✓ Cleaned up #{dead_count} stale process record(s)"
        puts ''
        puts 'Note: These processes either crashed or were killed without'
        puts 'graceful shutdown. Events they were processing may need retry.'

        OutboxRelay.logger.warn(
          event_name: 'rake_prune_processes_completed',
          dead_processes_count: dead_count,
          timeout: timeout
        )
      else
        puts '✓ No stale processes found - all processes healthy'

        OutboxRelay.logger.info(
          event_name: 'rake_prune_processes_completed',
          dead_processes_count: 0,
          timeout: timeout
        )
      end
    rescue StandardError => e
      puts '✗ ERROR: Failed to prune dead processes'
      puts "  #{e.message} (#{e.class.name})"
      puts ''
      puts 'Backtrace:'
      e.backtrace.first(10).each { |line| puts "  #{line}" }

      OutboxRelay.logger.error(
        event_name: 'rake_prune_processes_failed',
        error: e.message,
        error_class: e.class.name,
        backtrace: e.backtrace&.first(10)&.join("\n")
      )

      OutboxRelay::Instrumentation::Tasks.task_error(
        e,
        task_name: 'outbox_relay:prune_processes',
        timeout: timeout,
        severity: 'high'
      )

      exit 1
    end
  end

  desc 'Show process registry status'
  task process_status: :environment do
    require 'outbox_relay'

    puts 'OutboxRelay Process Registry'
    puts '=' * 80
    puts ''

    begin
      processes = OutboxRelay::Process.order(:id).all

      if processes.empty?
        puts 'No processes registered in database'
        puts ''
        puts 'Note: This is normal if OutboxRelay is not running.'
        next
      end

      # Group by kind
      supervisors = processes.select { |p| p.kind == 'supervisor' }
      workers = processes.select { |p| p.kind == 'worker' }

      # Display supervisors
      if supervisors.any?
        puts "Supervisors (#{supervisors.size}):"
        puts '-' * 80

        supervisors.each do |proc|
          age = Time.current - proc.last_heartbeat_at
          status = if age > 60
                     "✗ DEAD (#{age.to_i}s ago)"
                   elsif age > 30
                     "⚠ STALE (#{age.to_i}s ago)"
                   else
                     "✓ ALIVE (#{age.to_i}s ago)"
                   end

          puts format(
            '  ID: %-4s | PID: %-6s | %s | %s',
            proc.id,
            proc.pid,
            status,
            proc.name
          )
        end
        puts ''
      end

      # Display workers
      if workers.any?
        puts "Workers (#{workers.size}):"
        puts '-' * 80

        workers.each do |proc|
          age = Time.current - proc.last_heartbeat_at
          status = if age > 60
                     "✗ DEAD (#{age.to_i}s ago)"
                   elsif age > 30
                     "⚠ STALE (#{age.to_i}s ago)"
                   else
                     "✓ ALIVE (#{age.to_i}s ago)"
                   end

          puts format(
            '  ID: %-4s | PID: %-6s | %s | Supervisor: %-4s | %s',
            proc.id,
            proc.pid,
            status,
            proc.supervisor_id || 'N/A',
            proc.name
          )
        end
        puts ''
      end

      # Summary
      dead_count = processes.count { |p| (Time.current - p.last_heartbeat_at) > 60 }
      stale_count = processes.count { |p| (Time.current - p.last_heartbeat_at).between?(30, 60) }
      alive_count = processes.count { |p| (Time.current - p.last_heartbeat_at) < 30 }

      puts '=' * 80
      puts 'Summary:'
      puts "  Alive (heartbeat < 30s): #{alive_count}"
      puts "  Stale (heartbeat 30-60s): #{stale_count}"
      puts "  Dead (heartbeat > 60s): #{dead_count}"
      puts "  Total: #{processes.size}"

      if dead_count.positive?
        puts ''
        puts "⚠ #{dead_count} dead process(es) detected!"
        puts '  Run: bundle exec rake outbox_relay:prune_processes'
      end
    rescue StandardError => e
      puts '✗ ERROR: Failed to fetch process status'
      puts "  #{e.message} (#{e.class.name})"

      OutboxRelay.logger.error(
        event_name: 'rake_process_status_failed',
        error: e.message,
        error_class: e.class.name,
        backtrace: e.backtrace&.first(10)&.join("\n")
      )

      OutboxRelay::Instrumentation::Tasks.task_error(
        e,
        task_name: 'outbox_relay:process_status',
        severity: 'high'
      )

      exit 1
    end
  end

  desc 'Check for stuck advisory locks from dead processes'
  task check_locks: :environment do
    require 'outbox_relay'

    puts 'OutboxRelay Advisory Lock Health Check'
    puts '=' * 80

    begin
      # Query PostgreSQL for all advisory locks
      # This shows locks held by all database sessions, including dead ones
      result = ActiveRecord::Base.connection.execute(<<~SQL)
        SELECT
          locktype,
          database,
          pid,
          mode,
          granted,
          (SELECT state FROM pg_stat_activity WHERE pg_stat_activity.pid = pg_locks.pid) as session_state,
          (SELECT query FROM pg_stat_activity WHERE pg_stat_activity.pid = pg_locks.pid) as session_query
        FROM pg_locks
        WHERE locktype = 'advisory'
        ORDER BY pid
      SQL

      advisory_locks = result.to_a

      if advisory_locks.empty?
        puts '✓ No advisory locks currently held'
        puts ''
        puts 'Note: This is normal when no events are being processed.'
        next
      end

      puts "Found #{advisory_locks.size} advisory lock(s):"
      puts '-' * 80

      advisory_locks.each_with_index do |lock, idx|
        pid = lock['pid']
        granted = lock['granted']
        state = lock['session_state'] || 'unknown'
        query = lock['session_query']&.first(80) || 'N/A'

        status_icon = granted == 't' ? '🔒' : '⏳'

        puts "#{idx + 1}. #{status_icon} Session PID: #{pid}"
        puts "   State: #{state}"
        puts "   Granted: #{granted == 't' ? 'Yes' : 'No'}"
        puts "   Query: #{query}"
        puts ''
      end

      # Check for potentially stuck locks (idle sessions with granted locks)
      stuck_locks = advisory_locks.select do |lock|
        lock['granted'] == 't' && lock['session_state'] == 'idle'
      end

      puts '=' * 80
      if stuck_locks.any?
        puts "⚠ WARNING: #{stuck_locks.size} potentially stuck lock(s) detected!"
        puts ''
        puts 'Idle sessions holding advisory locks may indicate:'
        puts '  1. Long-running transactions (normal)'
        puts '  2. Dead connection pool connections (problematic)'
        puts '  3. Application server that crashed mid-transaction'
        puts ''
        puts 'If locks remain stuck for > 5 minutes, consider:'
        puts '  1. Restarting application servers to close connections'
        puts '  2. Killing stuck database sessions (use with caution):'
        stuck_locks.each do |lock|
          puts "     SELECT pg_terminate_backend(#{lock['pid']});"
        end
      else
        puts '✓ All locks are in active sessions - system healthy'
      end
    rescue StandardError => e
      puts '✗ ERROR: Failed to check advisory locks'
      puts "  #{e.message} (#{e.class.name})"

      OutboxRelay.logger.error(
        event_name: 'rake_check_locks_failed',
        error: e.message,
        error_class: e.class.name,
        backtrace: e.backtrace&.first(10)&.join("\n")
      )

      OutboxRelay::Instrumentation::Tasks.task_error(
        e,
        task_name: 'outbox_relay:check_locks',
        severity: 'medium'
      )

      exit 1
    end
  end

  desc 'Show stale consumer offsets (rows blocking cleanup of expired events)'
  task stale_consumers: :environment do
    require 'outbox_relay'

    puts 'OutboxRelay Stale Consumer Offsets'
    puts '=' * 80

    begin
      config = OutboxRelay::Configuration.new
      monitor = OutboxRelay::PartitionMonitor.new(config)
      stale = monitor.stale_consumer_offsets(exclude_claimed: true)
    rescue StandardError => e
      puts '✗ ERROR: Failed to scan consumer offsets'
      puts "  #{e.message} (#{e.class.name})"

      OutboxRelay.logger.error(
        event_name: 'rake_stale_consumers_failed',
        error: e.message,
        error_class: e.class.name,
        backtrace: e.backtrace&.first(10)&.join("\n")
      )

      OutboxRelay::Instrumentation::Tasks.task_error(
        e,
        task_name: 'outbox_relay:stale_consumers',
        severity: 'high'
      )

      exit 1
    end

    if stale.empty?
      puts '✓ No stale consumer offsets found'
      puts ''
      puts 'Every offset in outbox_relay_consumer_offsets maps to a consumer_group'
      puts 'currently declared in config/outbox_consumers.yml.'
      next
    end

    # Group by base consumer_group for readability
    by_group = stale.group_by { |o| o[:consumer_group] }

    puts "Found #{stale.size} stale offset(s) across #{by_group.size} consumer group(s):"
    puts ''

    by_group.each do |group, offsets|
      puts "Consumer group: #{group} (not in configuration)"
      puts '-' * 80

      offsets.sort_by { |o| o[:partition_key] }.each do |o|
        last_consumed = o[:last_consumed_at]&.iso8601 || 'never'
        age = o[:last_consumed_at] ? "#{format_duration(Time.current - o[:last_consumed_at])} ago" : 'n/a'
        heartbeat = o[:heartbeat_at]&.iso8601 || 'never'

        puts format(
          '  full_group: %-50s topic: %-30s partition: %d',
          o[:full_consumer_group],
          o[:topic],
          o[:partition_key]
        )
        puts format(
          '    last_consumed_sequence: %d | last_consumed_at: %s (%s)',
          o[:last_consumed_sequence],
          last_consumed,
          age
        )
        puts format('    heartbeat_at: %s', heartbeat)
      end
      puts ''
    end

    puts '=' * 80
    puts 'Impact:'
    puts '  CleanupExpiredEventsJob deletes events where sequence < MIN(last_consumed_sequence)'
    puts '  per topic. A stale offset pins MIN forever for its topic, so expired events'
    puts '  in that topic accumulate indefinitely.'
    puts ''
    puts 'Affected topics:'
    stale.group_by { |o| o[:topic] }.each do |topic, offsets|
      min_seq = offsets.map { |o| o[:last_consumed_sequence] }.min
      puts "  #{topic}: blocked at sequence #{min_seq} by #{offsets.size} stale offset(s)"
    end
    puts ''
    puts 'To prune (requires idle window for safety):'
    puts "  bundle exec rake 'outbox_relay:prune_stale_consumers[7]'   # 7-day idle minimum"

    # Exit code 1 so monitoring / CI can detect drift
    exit 1
  end

  desc 'Prune stale consumer offsets (rows blocking cleanup). Args: idle_days (required, min 1)'
  task :prune_stale_consumers, [:idle_days] => :environment do |_t, args|
    require 'outbox_relay'

    if args[:idle_days].nil? || args[:idle_days].to_s.strip.empty?
      puts '✗ ERROR: idle_days argument is required'
      puts ''
      puts 'Usage:'
      puts "  bundle exec rake 'outbox_relay:prune_stale_consumers[7]'   # require 7+ days idle"
      puts ''
      puts 'The argument is required (no default) so the operator explicitly chooses how'
      puts 'much grace to give a freshly-removed consumer group before its offsets are'
      puts 'deleted. A value smaller than your deploy-to-rollback window risks pruning'
      puts 'offsets you might want back.'
      exit 1
    end

    idle_days = args[:idle_days].to_i
    if idle_days < 1
      puts "✗ ERROR: idle_days must be >= 1, got #{args[:idle_days].inspect}"
      exit 1
    end

    idle_for = idle_days.days

    puts 'OutboxRelay Prune Stale Consumer Offsets'
    puts '=' * 80
    puts "Idle threshold: #{idle_days} day(s) (heartbeat_at NULL or older than #{idle_for.ago})"
    puts ''

    begin
      config = OutboxRelay::Configuration.new
      monitor = OutboxRelay::PartitionMonitor.new(config)
      stale = monitor.stale_consumer_offsets(idle_for: idle_for, exclude_claimed: true)
    rescue StandardError => e
      puts '✗ ERROR: Failed to scan consumer offsets'
      puts "  #{e.message} (#{e.class.name})"

      OutboxRelay.logger.error(
        event_name: 'rake_prune_stale_consumers_scan_failed',
        error: e.message,
        error_class: e.class.name,
        backtrace: e.backtrace&.first(10)&.join("\n")
      )

      OutboxRelay::Instrumentation::Tasks.task_error(
        e,
        task_name: 'outbox_relay:prune_stale_consumers',
        phase: 'scan',
        severity: 'critical'
      )

      exit 1
    end

    if stale.empty?
      puts '✓ No stale consumer offsets to prune'
      next
    end

    ids = stale.map { |o| o[:id] }

    puts "Will delete #{stale.size} offset(s):"
    puts '-' * 80
    stale.sort_by { |o| [o[:consumer_group], o[:partition_key]] }.each do |o|
      puts format(
        '  %s (topic: %s, partition: %d, last_consumed_at: %s)',
        o[:full_consumer_group],
        o[:topic],
        o[:partition_key],
        o[:last_consumed_at]&.iso8601 || 'never'
      )
    end
    puts ''

    deleted = 0
    started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    begin
      # Bounded by the safety filters in stale_consumer_offsets (not-in-config,
      # idle window, not actively claimed). Single DELETE is fine for ops scale —
      # production has dozens of stale offsets, not millions.
      deleted = OutboxRelay::ConsumerOffset.where(id: ids).delete_all
    rescue StandardError => e
      puts '✗ ERROR: Failed to delete offsets'
      puts "  #{e.message} (#{e.class.name})"

      OutboxRelay.logger.error(
        event_name: 'rake_prune_stale_consumers_delete_failed',
        candidate_ids: ids,
        error: e.message,
        error_class: e.class.name,
        backtrace: e.backtrace&.first(10)&.join("\n")
      )

      OutboxRelay::Instrumentation::Tasks.task_error(
        e,
        task_name: 'outbox_relay:prune_stale_consumers',
        phase: 'delete',
        candidate_count: ids.size,
        severity: 'critical'
      )

      exit 1
    end

    duration = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at

    OutboxRelay.logger.warn(
      event_name: 'rake_prune_stale_consumers_completed',
      offsets_deleted: deleted,
      idle_days: idle_days,
      duration_ms: (duration * 1000).round(2)
    )

    # Mirror CleanupExpiredEventsJob's notification convention so dashboards that
    # subscribe to /^outbox_relay\./ pick this up alongside cleanup events.
    ActiveSupport::Notifications.instrument(
      'outbox_relay.stale_offsets.pruned',
      offsets_deleted: deleted,
      idle_days: idle_days,
      duration: duration
    )

    puts '=' * 80
    puts "✓ Deleted #{deleted} stale offset(s) in #{(duration * 1000).round(2)}ms"
    puts ''
    puts 'Next CleanupExpiredEventsJob run (every 5 min by default) will be able'
    puts 'to remove expired events that were previously blocked by these offsets.'
  end

  # Helper methods
  def format_duration(seconds)
    return '0s' if seconds < 1

    parts = []
    parts << "#{(seconds / 86_400).to_i}d" if seconds >= 86_400
    parts << "#{((seconds % 86_400) / 3600).to_i}h" if seconds >= 3600
    parts << "#{((seconds % 3600) / 60).to_i}m" if seconds >= 60
    parts << "#{(seconds % 60).to_i}s"

    parts.first(2).join(' ')
  end
end
