# frozen_string_literal: true

require "thor"

module OutboxRelay
  class CLI < Thor
    class << self
      def exit_on_failure?
        true
      end
    end

    desc "start", "Start OutboxRelay server with workers"
    long_desc <<~DESC
      Starts the OutboxRelay supervisor and worker processes to process outbox events.

      The server will poll configured consumer groups and process events according
      to your configuration in config/outbox_consumers.yml.
    DESC

    class_option :polling_interval,
      type: :numeric,
      aliases: "-p",
      default: Configuration::DEFAULT_POLLING_INTERVAL,
      desc: "Polling interval in seconds"

    class_option :batch_size,
      type: :numeric,
      aliases: "-b",
      default: Configuration::DEFAULT_BATCH_SIZE,
      desc: "Batch size for processing"

    class_option :max_loops,
      type: :numeric,
      aliases: "-m",
      default: Configuration::DEFAULT_MAX_LOOPS,
      desc: "Maximum loops before restart"

    class_option :environment,
      type: :string,
      aliases: "-e",
      desc: "Rails environment (default: from RAILS_ENV)"

    class_option :log_level,
      type: :string,
      aliases: "-l",
      desc: "Log level (debug, info, warn, error)"

    default_task :start

    def start
      setup_environment
      validate_options!

      # Check macOS fork-safety environment on startup
      check_macos_fork_safety!

      OutboxRelay.logger.info(
        event_name: "outbox_relay_starting",
        version: OutboxRelay::VERSION,
        options: symbolized_options
      )

      # Start the supervisor
      OutboxRelay::Supervisor.start(**symbolized_options)
    rescue => e
      OutboxRelay.logger.error(
        event_name: "outbox_relay_failed_to_start",
        error: e.message,
        backtrace: e.backtrace&.first(10)&.join("\n")
      )
      abort "Failed to start OutboxRelay: #{e.message}"
    end

    private

    def symbolized_options
      @symbolized_options ||= options.transform_keys(&:to_sym).compact
    end

    def setup_environment
      # Step 1: Load Rails environment if not already loaded
      begin
        unless defined?(Rails)
          require_relative "../../config/environment"
        end
      rescue LoadError => e
        abort_with_setup_error(
          step: "Load Rails environment",
          error: e,
          suggestion: "Ensure you're running from a valid Rails application directory with config/environment.rb"
        )
      rescue => e
        abort_with_setup_error(
          step: "Load Rails environment",
          error: e,
          suggestion: "Check your Rails configuration for errors"
        )
      end

      # Step 2: Set log level if specified
      if options[:log_level]
        begin
          OutboxRelay.logger.level = Logger.const_get(options[:log_level].upcase)
        rescue NameError => e
          abort_with_setup_error(
            step: "Set log level",
            error: e,
            suggestion: "Valid log levels: debug, info, warn, error, fatal"
          )
        rescue => e
          abort_with_setup_error(
            step: "Set log level",
            error: e,
            suggestion: "Check OutboxRelay.logger configuration"
          )
        end
      end

      # Step 3: Validate consumer groups configuration
      begin
        consumer_groups = OutboxRelay.configuration.consumer_group_configs

        unless consumer_groups.is_a?(Hash)
          raise TypeError, "Consumer groups configuration must be a Hash, got #{consumer_groups.class}"
        end

        if consumer_groups.empty?
          raise ConfigurationError, "No consumer groups configured in config/outbox_consumers.yml"
        end
      rescue YamlConfigLoader::ConfigurationError => e
        abort_with_setup_error(
          step: "Load consumer groups",
          error: e,
          suggestion: "Check config/outbox_consumers.yml syntax and structure. Run: rails generate outbox_relay:install"
        )
      rescue => e
        abort_with_setup_error(
          step: "Validate consumer groups",
          error: e,
          suggestion: "Check consumer groups configuration in config/outbox_consumers.yml"
        )
      end
    end

    def abort_with_setup_error(step:, error:, suggestion:)
      OutboxRelay.logger.error(
        event_name: "cli_setup_failed",
        step: step,
        error: error.message,
        error_class: error.class.name,
        backtrace: error.backtrace&.first(5)&.join("\n")
      )

      Sentry.capture_exception(error, extra: {
        cli_step: step,
        suggestion: suggestion
      }) if defined?(Sentry)

      puts "\n" + "=" * 80
      puts "❌ Setup Failed: #{step}"
      puts "=" * 80
      puts ""
      puts "Error: #{error.message}"
      puts "Type: #{error.class.name}"
      puts ""
      puts "💡 Suggestion: #{suggestion}"
      puts ""
      puts "=" * 80
      puts ""

      abort "OutboxRelay setup failed at: #{step}"
    end

    def validate_options!
      errors = []

      errors << "Polling interval must be positive" if options[:polling_interval] <= 0
      errors << "Batch size must be positive" if options[:batch_size] <= 0
      errors << "Max loops must be positive" if options[:max_loops] <= 0

      if errors.any?
        abort "Configuration errors:\n#{errors.join("\n")}"
      end
    end

    def check_macos_fork_safety!
      return unless RUBY_PLATFORM.include?("darwin")

      missing_vars = []
      missing_vars << "OBJC_DISABLE_INITIALIZE_FORK_SAFETY" unless ENV["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] == "YES"
      missing_vars << "PGGSSENCMODE" unless ENV["PGGSSENCMODE"] == "disable"

      return if missing_vars.empty?

      OutboxRelay.logger.warn(
        event_name: "macos_fork_safety_not_configured",
        missing_variables: missing_vars,
        message: "Running on macOS without proper fork-safety configuration. Workers may crash!"
      )

      puts "\n" + "=" * 80
      puts "⚠️  WARNING: macOS Fork-Safety Not Configured"
      puts "=" * 80
      puts ""
      puts "OutboxRelay uses fork() to create worker processes, which requires specific"
      puts "environment variables to be set on macOS to prevent crashes."
      puts ""
      puts "Missing environment variables:"
      missing_vars.each { |var| puts "  - #{var}" }
      puts ""
      puts "This should not happen if you're using the generated bin/outbox_relay."
      puts "The executable should automatically set these variables."
      puts ""
      puts "If you're seeing this, please report it as a bug."
      puts ""
      puts "=" * 80
      puts ""
    end
  end
end
