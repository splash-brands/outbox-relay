# frozen_string_literal: true

require "socket"
require "securerandom"
require "concurrent"

module OutboxRelay
  module Processes
    class Base
      include Callbacks
      include Interruptible
      include Registrable
      include Heartbeat
      include Procline

      attr_reader :name

      def initialize(*)
        # CRITICAL: Call super first to initialize included modules (Heartbeat, etc.)
        # Without this, module initialize methods never run, leaving @heartbeat_interval nil
        super

        @name = generate_name
        # Use simple boolean for signal-safety (no mutex)
        # Ruby VM guarantees atomicity of boolean read/write
        @stopped = false
        # Note: Heartbeat module will initialize @heartbeat_failures as AtomicFixnum
        @max_heartbeat_failures = 5

        # Note: supervisor_pid is set AFTER fork in boot, not here
        # Setting it in initialize would capture parent PID before fork
        @supervisor_pid = nil
      end

      def kind
        self.class.name.demodulize.underscore
      end

      def hostname
        @hostname ||= Socket.gethostname.force_encoding(Encoding::UTF_8)
      end

      def pid
        @pid ||= ::Process.pid
      end

      def metadata
        {}
      end

      def stop
        # Signal-safe: simple boolean write is atomic in Ruby VM
        @stopped = true
      end

      def stopped?
        # Signal-safe: simple boolean read is atomic in Ruby VM
        @stopped
      end

      # Check if this process is running under supervisor
      # Workers run under supervisor, supervisor itself does not
      def supervised?
        @supervisor_pid.present? && @supervisor_pid != ::Process.pid
      end

      # Check if supervisor process has died
      #
      # When a forked worker's parent dies, the OS reassigns it to init (PID 1)
      # or another adopting process. This is the "orphan" condition.
      #
      # ## Why This Matters
      #
      # During ECS deployment:
      #   1. ECS sends SIGTERM to supervisor
      #   2. Supervisor begins graceful shutdown
      #   3. If supervisor crashes/killed before workers exit → orphaned workers
      #   4. Orphaned workers run forever with no supervision
      #
      # ## Detection
      #
      # Compare current ppid with saved supervisor_pid:
      #   - Same → supervisor still alive
      #   - Different → supervisor died, we're orphaned
      #
      # ## Response
      #
      # Worker should exit immediately when orphaned:
      #   - No point processing more events
      #   - Prevents zombie processes
      #   - ECS can clean up faster
      def supervisor_went_away?
        supervised? && ::Process.ppid != @supervisor_pid
      end

      private

      def generate_name
        [kind, SecureRandom.hex(6)].join("-")
      end

      def custom_logger
        OutboxRelay.logger
      end
    end
  end
end
