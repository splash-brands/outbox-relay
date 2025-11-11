# frozen_string_literal: true

require "optparse"

module OutboxRelay
  class CLI
    class << self
      def start(argv = ARGV)
        new(argv).run
      end
    end

    attr_reader :options

    def initialize(argv)
      @argv = argv
      @options = parse_options(argv)
    end

    def run
      setup_environment
      validate_options!

      # Check macOS fork-safety environment on startup
      check_macos_fork_safety!

      OutboxRelay.logger.info(
        event_name: "outbox_relay_starting",
        version: OutboxRelay::VERSION,
        options: options
      )

      # Start the supervisor
      OutboxRelay::Supervisor.start(**options)
    rescue => e
      OutboxRelay.logger.error(
        event_name: "outbox_relay_failed_to_start",
        error: e.message,
        backtrace: e.backtrace&.first(10)&.join("\n")
      )
      abort "Failed to start OutboxRelay: #{e.message}"
    end

    private

    def parse_options(argv)
      options = {
        polling_interval: Configuration::DEFAULT_POLLING_INTERVAL,
        batch_size: Configuration::DEFAULT_BATCH_SIZE,
        max_loops: Configuration::DEFAULT_MAX_LOOPS
      }

      parser = OptionParser.new do |opts|
        opts.banner = "Usage: outbox_relay [options]"

        opts.on("-p", "--polling-interval SECONDS", Float, "Polling interval in seconds (default: 1.0)") do |interval|
          options[:polling_interval] = interval
        end

        opts.on("-b", "--batch-size SIZE", Integer, "Batch size for processing (default: 100)") do |size|
          options[:batch_size] = size
        end

        opts.on("-m", "--max-loops COUNT", Integer, "Maximum loops before restart (default: 1000)") do |count|
          options[:max_loops] = count
        end

        opts.on("-e", "--environment ENV", "Rails environment (default: from RAILS_ENV)") do |env|
          options[:environment] = env
        end

        opts.on("-l", "--log-level LEVEL", "Log level (debug, info, warn, error)") do |level|
          options[:log_level] = level
        end

        opts.on("-h", "--help", "Show this help message") do
          puts opts
          exit
        end

        opts.on("-v", "--version", "Show version") do
          puts "OutboxRelay #{OutboxRelay::VERSION}"
          exit
        end
      end

      parser.parse!(argv)
      options
    rescue OptionParser::InvalidOption => e
      abort "Invalid option: #{e.message}\n\n#{parser}"
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

      OutboxRelay::Instrumentation::CLI.start_error(error)

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
      puts "To fix this, run OutboxRelay using the rake task:"
      puts "  bundle exec rake outbox_relay:start"
      puts ""
      puts "Or set the variables manually before starting:"
      puts "  OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES PGGSSENCMODE=disable bundle exec rake outbox_relay:start"
      puts ""
      puts "=" * 80
      puts ""
    end
  end
end
