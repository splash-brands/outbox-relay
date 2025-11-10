# frozen_string_literal: true

module OutboxRelay
  module Processes
    # Signals - Safe Unix signal handling for graceful shutdown
    #
    # ## Why Signal Queueing?
    #
    # Unix signal handlers have severe restrictions:
    #   - Can't allocate memory
    #   - Can't acquire locks
    #   - Can't make system calls (including I/O, logging)
    #   - Must be reentrant (can interrupt themselves)
    #
    # Calling complex code from signal handlers causes:
    #   - Deadlocks (if handler tries to acquire locks)
    #   - Segfaults (if handler allocates memory)
    #   - Race conditions (if handler modifies shared state)
    #
    # ## Queue-Based Approach
    #
    # Instead of handling signals directly, we:
    #   1. Signal arrives → trap handler enqueues signal name
    #   2. Main loop polls queue → processes signals safely
    #
    # This ensures:
    #   - Signal handling happens in main thread context
    #   - Can safely log, acquire locks, perform I/O
    #   - No race conditions or deadlocks
    #
    # ## Supported Signals
    #
    # - **TERM** (15): Graceful shutdown - finish current work, then exit
    #   Sent by: systemd, Docker, Kubernetes, `kill` command
    #   Behavior: Wait for workers to finish (up to 30s), then TERM workers
    #
    # - **INT** (2): Interrupt - same as TERM, triggered by Ctrl+C
    #   Sent by: Terminal Ctrl+C
    #   Behavior: Same as TERM
    #
    # - **QUIT** (3): Immediate shutdown - exit now, don't wait
    #   Sent by: Terminal Ctrl+\, `kill -QUIT`
    #   Behavior: Send KILL to all workers immediately
    #
    # ## Shutdown Flow
    #
    # 1. User sends TERM signal to supervisor
    # 2. Trap handler enqueues "TERM"
    # 3. Supervisor main loop processes queue
    # 4. Supervisor sets @stopped = true
    # 5. Supervisor sends TERM to all workers
    # 6. Supervisor waits up to 30s for workers
    # 7. Any remaining workers get KILL
    # 8. Supervisor exits
    #
    # ## Double-Signal Handling
    #
    # If TERM received twice:
    #   - First TERM: Start graceful shutdown
    #   - Second TERM: Convert to immediate shutdown (QUIT behavior)
    #
    # This allows operators to "force quit" stuck shutdowns.
    #
    # ## Production Usage
    #
    #   # Graceful shutdown (recommended)
    #   kill -TERM <supervisor_pid>
    #   # or
    #   docker stop <container>  # Sends TERM, waits, then KILL
    #
    #   # Force immediate shutdown (emergency only)
    #   kill -QUIT <supervisor_pid>
    #
    module Signals
      SIGNALS = %w[TERM INT QUIT].freeze

      def register_signal_handlers
        # Save original signal handlers so they can be restored later
        @original_handlers ||= {}

        SIGNALS.each do |signal|
          # Store original handler (if any)
          @original_handlers[signal] = Signal.trap(signal, "IGNORE")

          # Install our handler
          Signal.trap(signal) do
            enqueue_signal(signal)
          end
        end
      end

      def restore_signal_handlers
        return unless @original_handlers

        # Save reference to handlers before clearing
        # This prevents race where signal arrives after check but before restoration
        handlers_to_restore = @original_handlers
        @original_handlers = nil

        # Now restore handlers
        handlers_to_restore.each do |signal, original_handler|
          if original_handler
            Signal.trap(signal, original_handler)
          else
            Signal.trap(signal, "DEFAULT")
          end
        end
      end

      def enqueue_signal(signal)
        @signal_queue ||= []

        # Bound queue to prevent memory exhaustion from signal flooding
        # 10 signals should be more than enough for any shutdown scenario
        # IMPORTANT: Do NOT log here - signal handlers must not perform I/O
        if @signal_queue.size >= 10
          @signal_dropped_count ||= 0
          @signal_dropped_count += 1
          return
        end

        # Deduplicate - no need to handle same signal multiple times
        # IMPORTANT: Do NOT log here - signal handlers must not perform I/O
        if @signal_queue.include?(signal)
          @signal_duplicate_count ||= 0
          @signal_duplicate_count += 1
          return
        end

        @signal_queue << signal

        # CRITICAL: Interrupt sleep immediately via self-pipe
        # This ensures signals are processed promptly instead of waiting
        # for the next polling interval to expire.
        #
        # Without this, shutdowns can take up to polling_interval seconds
        # to begin processing. With this, shutdowns start immediately.
        begin
          interrupt if respond_to?(:interrupt)
        rescue
          # Interrupt failed - not critical, signal will be processed next iteration
          # Don't log here - not signal-safe (I/O operations can deadlock)
        end
      end

      def process_signal_queue
        return unless @signal_queue&.any?

        # Track dropped/duplicate signals for logging after processing
        # (can't log in signal handler due to I/O restrictions)
        dropped_count = @signal_dropped_count || 0
        duplicate_count = @signal_duplicate_count || 0
        queue_size = @signal_queue.size

        # Reset counters
        @signal_dropped_count = 0
        @signal_duplicate_count = 0

        # Log queue state before processing (safe here - not in signal handler)
        if queue_size > 0
          custom_logger.debug(
            event_name: "processing_signal_queue",
            queue_size: queue_size,
            signals: @signal_queue.dup, # dup to avoid modification during logging
            dropped_signals: dropped_count,
            duplicate_signals: duplicate_count
          )
        end

        # Alert if signals were dropped
        if dropped_count > 0
          custom_logger.error(
            event_name: "signals_dropped_queue_full",
            dropped_count: dropped_count,
            reason: "Signal queue reached maximum size (10)"
          )
        end

        while (signal = @signal_queue.shift)
          handle_signal(signal)
        end
      end

      def handle_signal(signal)
        case signal
        when "TERM", "INT"
          custom_logger.warn(
            event_name: "terminating_gracefully",
            signal: signal,
            process_id: process_id
          )
          terminate_gracefully
        when "QUIT"
          custom_logger.warn(
            event_name: "terminating_immediately",
            signal: signal,
            process_id: process_id
          )
          terminate_immediately
        end
      end

      def signal_processes(pids, signal)
        failed_pids = []

        pids.each do |pid|
          begin
            ::Process.kill(signal, pid)
            custom_logger.debug(
              event_name: "signal_sent",
              signal: signal,
              target_pid: pid
            )
          rescue Errno::ESRCH
            # Process already dead - this is fine
            custom_logger.debug(
              event_name: "signal_failed_process_not_found",
              signal: signal,
              target_pid: pid
            )
          rescue => e
            custom_logger.error(
              event_name: "signal_failed",
              signal: signal,
              target_pid: pid,
              error: e.message,
              error_class: e.class.name,
              backtrace: e.backtrace&.first(5)&.join("\n")
            )

            Sentry.capture_exception(e, extra: {
              signal: signal,
              target_pid: pid,
              severity: "high"
            }) if defined?(Sentry)

            failed_pids << pid
          end
        end

        # Return list of PIDs that failed to receive signal
        failed_pids
      end
    end
  end
end
