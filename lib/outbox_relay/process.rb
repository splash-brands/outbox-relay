# frozen_string_literal: true

module OutboxRelay
  # Database-backed process registry for fault tolerance
  #
  # Inspired by Solid Queue's process tracking, this model provides:
  # - Persistent process state across crashes
  # - Heartbeat mechanism for dead process detection
  # - Supervisor-worker relationships
  # - Process metadata storage
  #
  # Critical for ECS deployments where containers can die unexpectedly.
  class Process < ActiveRecord::Base
    self.table_name = "outbox_relay_processes"

    # Process lifecycle states
    KINDS = %w[supervisor worker].freeze

    # Relationships
    belongs_to :supervisor, class_name: "OutboxRelay::Process", optional: true
    has_many :supervisees, class_name: "OutboxRelay::Process", foreign_key: :supervisor_id, dependent: :destroy

    # Validations
    validates :kind, presence: true, inclusion: { in: KINDS }
    validates :name, presence: true
    validates :pid, presence: true
    validates :last_heartbeat_at, presence: true

    # Metadata JSON storage
    store_accessor :metadata, :consumer_class, :topic, :partition_key

    # Register a new process in the database
    #
    # @param kind [String] "supervisor" or "worker"
    # @param name [String] Unique process name
    # @param supervisor_id [Integer, nil] ID of supervisor process (nil for supervisor itself)
    # @param metadata [Hash] Process metadata (consumer_class, topic, etc.)
    # @return [OutboxRelay::Process] Created process record
    def self.register(kind:, name:, supervisor_id: nil, **metadata)
      create!(
        kind: kind,
        name: name,
        pid: ::Process.pid,
        hostname: hostname,
        supervisor_id: supervisor_id,
        last_heartbeat_at: Time.current,
        metadata: metadata,
      )
    end

    # Update heartbeat timestamp to prove liveness
    #
    # Uses optimistic locking with restore_attributes to handle concurrent updates.
    # If record was deleted (deregistered), this will raise RecordNotFound.
    #
    # @return [Boolean] true if heartbeat successful
    def heartbeat
      # Reload attributes in case they were modified elsewhere
      restore_attributes

      # Use NOWAIT to prevent indefinite blocking
      # If we can't acquire lock immediately, skip this heartbeat
      # This prevents timer thread accumulation during database contention
      ActiveRecord::Base.transaction do
        lock!("FOR UPDATE NOWAIT")
        touch(:last_heartbeat_at)
      end

      # Reset lock failure counter on success
      @consecutive_lock_failures = 0 if @consecutive_lock_failures&.> 0

      true
    rescue ActiveRecord::RecordNotFound
      # Process was deregistered, heartbeat fails silently
      false
    rescue ActiveRecord::LockWaitTimeout, ActiveRecord::StatementInvalid => e
      # Could not acquire lock (another heartbeat or deregister in progress)
      # Skip this heartbeat - better to miss one than accumulate blocked threads

      # Track consecutive lock failures
      @consecutive_lock_failures ||= 0
      @consecutive_lock_failures += 1

      # Log at appropriate level based on frequency
      log_level = @consecutive_lock_failures > 3 ? :warn : :debug

      OutboxRelay.logger.send(log_level,
        event_name: "heartbeat_lock_skipped",
        process_id: id,
        consecutive_failures: @consecutive_lock_failures,
        error: e.message,
        error_class: e.class.name
      )

      # Alert on sustained lock contention
      if @consecutive_lock_failures >= 5
        Sentry.capture_message(
          "Sustained heartbeat lock contention detected",
          level: :warning,
          extra: { process_id: id, consecutive_failures: @consecutive_lock_failures }
        ) if defined?(Sentry)
      end

      false
    end

    # Deregister process from database
    #
    # @param pruned [Boolean] True if being cleaned up by maintenance task
    # @return [Boolean] true if deregistration successful
    def deregister(pruned: false)
      destroy!

      # Note: Supervisees are automatically cleaned up by ActiveRecord cascade delete
      # (has_many :supervisees, dependent: :destroy in OutboxRelay::Process model)
      # No need to manually deregister - would cause double deletion attempts
      # IMPORTANT: If the :dependent option is removed from the association, this will leave orphaned records

      true
    rescue ActiveRecord::RecordNotFound
      # Already deregistered, no-op
      false
    end

    # Check if process is supervised by another process
    #
    # @return [Boolean] true if has supervisor
    def supervised?
      supervisor_id.present?
    end

    # Get hostname for this process
    #
    # @return [String] Server hostname
    def self.hostname
      @hostname ||= Socket.gethostname
    end

    # Find stale processes that haven't sent heartbeat recently
    #
    # @param timeout [Integer] Seconds without heartbeat before considered dead
    # @return [ActiveRecord::Relation] Dead processes
    def self.dead(timeout: 60)
      where("last_heartbeat_at < ?", timeout.seconds.ago)
    end

    # Cleanup dead processes
    #
    # @param timeout [Integer] Seconds without heartbeat before considered dead
    # @return [Integer] Number of processes cleaned up
    def self.prune_dead_processes(timeout: 60)
      dead_processes = dead(timeout: timeout)
      count = dead_processes.count

      dead_processes.find_each do |process|
        process.deregister(pruned: true)
      end

      count
    end
  end
end
