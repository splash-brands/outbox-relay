# frozen_string_literal: true

module OutboxRelay
  module Processes
    # Interruptible - Responsive sleep for fork-based workers using self-pipe trick
    #
    # ## The Problem
    #
    # Standard Kernel.sleep blocks the process completely:
    #   - Can't be interrupted by signals
    #   - Worker sleeps for full duration even during shutdown
    #   - Poor user experience: "I sent TERM but nothing happened"
    #
    # ## The Solution: Self-Pipe Trick
    #
    # The self-pipe trick is a standard Unix pattern for signal handling:
    #   1. Create a pipe (reader + writer file descriptors)
    #   2. Signal handler writes to the pipe (signal-safe operation)
    #   3. Main thread uses IO.wait_readable with timeout
    #   4. When signal arrives → pipe becomes readable → instant wake-up
    #
    # Reference: http://cr.yp.to/docs/selfpipe.html
    #
    # ## Why Self-Pipe is Better than Polling
    #
    # **Polling approach (old):**
    #   - Sleep in 100ms chunks, check flag between sleeps
    #   - Shutdown latency: 0-100ms
    #   - Periodic wake-ups even when idle
    #
    # **Self-pipe approach (new):**
    #   - Sleep until timeout OR pipe becomes readable
    #   - Shutdown latency: 0-1ms (instant)
    #   - Zero wake-ups when idle
    #   - Standard pattern used by Unicorn, Puma, Sidekiq, Solid Queue
    #
    # ## Signal Safety
    #
    # The self-pipe trick is signal-safe because:
    #   - IO#write_nonblock doesn't use mutexes
    #   - File descriptors are kernel-managed
    #   - Writing to pipe can't deadlock
    #
    # ## Fork Safety
    #
    # Each forked worker gets its own pipe:
    #   - Pipe created in initialize (after fork)
    #   - No shared state between processes
    #   - Clean shutdown closes pipe
    #
    # ## Example Usage
    #
    #   # Dynamic delay based on workload
    #   delay = processed_count > 0 ? 0.1 : 1.0
    #   interruptible_sleep(delay)  # Wakes instantly on signal
    #
    module Interruptible
      # Interruptible sleep that respects shutdown signals
      #
      # Uses IO.wait_readable which blocks until:
      #   - Timeout expires (normal case)
      #   - Pipe becomes readable (signal received)
      #   - Process receives signal (EINTR)
      #
      # Performance:
      #   - Zero CPU overhead when idle
      #   - Instant wake-up on signal (0-1ms)
      #   - Standard Unix pattern, battle-tested
      def interruptible_sleep(time)
        # Create pipe lazily on first sleep (after fork)
        ensure_self_pipe_created

        if time > 0 && self_pipe[:reader].wait_readable(time)
          # Pipe became readable → drain it
          loop { self_pipe[:reader].read_nonblock(SELF_PIPE_BLOCK_SIZE) }
        end
      rescue Errno::EAGAIN, Errno::EINTR, IO::EWOULDBLOCKWaitReadable => e
        # These are expected, but track frequency for debugging
        @spurious_wakeups ||= 0
        @spurious_wakeups += 1

        # Log occasionally (every 100 occurrences) to avoid log spam
        if @spurious_wakeups % 100 == 0
          custom_logger.debug(
            event_name: "interruptible_sleep_spurious_wakeups",
            count: @spurious_wakeups,
            error_class: e.class.name
          )
        end
      end

      # Wake up the process from interruptible sleep
      #
      # Called from signal handlers or external events.
      # Writes a byte to the pipe, making it readable.
      #
      # Signal-safe: write_nonblock doesn't use mutexes
      def wake_up
        interrupt
      end

      private

      # Size of reads when draining the pipe
      # Arbitrary small value - we just need to empty it
      SELF_PIPE_BLOCK_SIZE = 11

      attr_reader :self_pipe

      # Ensure self-pipe is created (lazy initialization after fork)
      #
      # We can't create the pipe in initialize because:
      #   1. Base#initialize doesn't call super with splat
      #   2. Module initialize isn't guaranteed to be called
      #   3. Pipe must be created AFTER fork anyway
      #
      # So we create it lazily on first use
      def ensure_self_pipe_created
        @self_pipe ||= create_self_pipe
      end

      # Write to self-pipe to interrupt sleep
      #
      # Signal-safe: write_nonblock is atomic and mutex-free
      # Retries on EAGAIN (pipe full) and EINTR (interrupted by another signal)
      def interrupt
        ensure_self_pipe_created
        self_pipe[:writer].write_nonblock(".")
      rescue Errno::EAGAIN, Errno::EINTR
        # EAGAIN: Pipe buffer full (unlikely, but possible)
        # EINTR: Interrupted by another signal while writing
        # In both cases: retry the write
        retry
      end

      # Create self-pipe for signal handling
      #
      # Creates a Unix pipe with reader and writer ends.
      # The pipe is used for signal-safe wake-up:
      #   - Signal handler writes to writer
      #   - Main loop reads from reader
      #
      # Reference: http://cr.yp.to/docs/selfpipe.html
      def create_self_pipe
        reader, writer = IO.pipe
        { reader: reader, writer: writer }
      end

      # Close self-pipe file descriptors
      #
      # Called during shutdown to prevent file descriptor leaks
      def close_self_pipe
        return unless @self_pipe

        begin
          @self_pipe[:reader].close unless @self_pipe[:reader].closed?
        rescue => e
          custom_logger.warn(
            event_name: "self_pipe_reader_close_failed",
            error: e.message,
            error_class: e.class.name
          )
        end

        begin
          @self_pipe[:writer].close unless @self_pipe[:writer].closed?
        rescue => e
          custom_logger.warn(
            event_name: "self_pipe_writer_close_failed",
            error: e.message,
            error_class: e.class.name
          )
        end

        @self_pipe = nil
      end
    end
  end
end
