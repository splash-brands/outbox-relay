# frozen_string_literal: true

require "outbox_relay/version"

require "active_support"
require "active_support/core_ext/numeric/time"
require "active_record"
require "concurrent-ruby"

module OutboxRelay
  extend self

  class Error < StandardError; end
  class ConfigurationError < Error; end

  DEFAULT_LOGGER = ActiveSupport::Logger.new($stdout)

  mattr_accessor :logger, default: DEFAULT_LOGGER
  mattr_accessor :custom_logger # For application-specific logging

  # Configuration
  mattr_accessor :polling_interval, default: 1.0 # seconds
  mattr_accessor :batch_size, default: 100
  mattr_accessor :max_loops, default: 1000
  mattr_accessor :shutdown_timeout, default: 30.seconds
  mattr_accessor :silence_polling, default: true

  # Callbacks
  mattr_accessor :on_thread_error

  # Rails executor wrapper (optional)
  # Set to Rails.application.executor in Rails apps for proper context management
  # Set to nil to disable wrapping (for non-Rails apps)
  mattr_accessor :app_executor

  def silence_polling?
    silence_polling
  end

  def instrument(channel, **options, &block)
    ActiveSupport::Notifications.instrument("#{channel}.outbox_relay", **options, &block)
  end

  def configure
    yield self if block_given?
  end

  # Global configuration instance
  # Used for accessing topic_descriptions and consumer_group_configs
  def configuration
    @configuration ||= Configuration.new
  end

  # Check macOS fork-safety environment configuration
  #
  # NOTE: These environment variables CANNOT be set programmatically in Ruby code!
  # They must be set in the shell environment BEFORE the Ruby process starts.
  #
  # The Objective-C runtime checks OBJC_DISABLE_INITIALIZE_FORK_SAFETY during
  # the fork() system call, before any Ruby code can modify the environment.
  #
  # Use the generated bin/outbox_relay executable which sets them automatically:
  #   ./bin/outbox_relay
  #
  # The executable handles this automatically via bin/outbox_relay template.
  def setup_macos_fork_safety!
    return unless macos?

    if ENV["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] == "YES" && ENV["PGGSSENCMODE"] == "disable"
      logger.info(
        event_name: "macos_fork_safety_configured",
        pggssencmode: ENV["PGGSSENCMODE"],
        objc_disable_fork_safety: ENV["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"]
      )
    else
      logger.warn(
        event_name: "macos_fork_safety_not_configured",
        pggssencmode: ENV["PGGSSENCMODE"],
        objc_disable_fork_safety: ENV["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"],
        message: "macOS fork-safety environment not properly configured. Workers may crash!"
      )
    end
  end

  def macos?
    RUBY_PLATFORM.include?("darwin")
  end
end

# Load models first (base class first)
require "outbox_relay/models/application_record"
require "outbox_relay/models/outbox_event"
require "outbox_relay/models/consumer_offset"
require "outbox_relay/models/dead_letter_event"
require "outbox_relay/process"

# Load core components
require "outbox_relay/instrumentation"
require "outbox_relay/yaml_config_loader"
require "outbox_relay/configuration"
require "outbox_relay/cli"
require "outbox_relay/outbox_publisher"

# Load jobs (optional features)
require "outbox_relay/jobs/cleanup_expired_events_job"

# Load process management (modules first, then classes in dependency order)
require "outbox_relay/processes/callbacks"
require "outbox_relay/processes/interruptible"
require "outbox_relay/processes/procline"
require "outbox_relay/processes/registrable"
require "outbox_relay/processes/heartbeat"
require "outbox_relay/processes/runnable"
require "outbox_relay/processes/signals"
require "outbox_relay/processes/app_executor"  # Rails executor wrapper
require "outbox_relay/processes/base"      # Base uses above modules
require "outbox_relay/processes/poller"    # Poller < Base

require "outbox_relay/worker"
require "outbox_relay/supervisor"

# Load LogSubscriber for structured logging
require "outbox_relay/log_subscriber"

# Load Rails integration if Rails is present
require "outbox_relay/engine" if defined?(Rails::Engine)

# Run load hooks for extensions
ActiveSupport.run_load_hooks(:outbox_relay, self)
