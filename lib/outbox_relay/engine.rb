# frozen_string_literal: true

require "rails"

module OutboxRelay
  class Engine < ::Rails::Engine
    isolate_namespace OutboxRelay

    config.outbox_relay = ActiveSupport::OrderedOptions.new

    # Configure app executor and error handling before Rails prepares
    # This ensures OutboxRelay can report errors during initialization
    initializer "outbox_relay.app_executor", before: :run_prepare_callbacks do |app|
      config.outbox_relay.app_executor ||= app.executor

      # Default error handler using Rails.error (Rails 7+)
      # Falls back to Rails.logger for Rails 6
      config.outbox_relay.on_thread_error ||= ->(exception) {
        if defined?(Rails.error)
          Rails.error.report(exception, handled: false, source: "outbox_relay")
        else
          Rails.logger.error("[OutboxRelay] Thread error: #{exception.message}")
          Rails.logger.error(exception.backtrace.join("\n"))
        end
      }

      OutboxRelay.app_executor = config.outbox_relay.app_executor
      OutboxRelay.on_thread_error = config.outbox_relay.on_thread_error
    end

    # Apply configuration from config.outbox_relay to OutboxRelay module
    initializer "outbox_relay.config" do
      config.outbox_relay.each do |name, value|
        # Skip internal Rails options (app_executor, on_thread_error)
        next if [:app_executor, :on_thread_error].include?(name)

        if OutboxRelay.respond_to?("#{name}=")
          OutboxRelay.public_send("#{name}=", value)
        end
      end
    end

    initializer "outbox_relay.logger" do
      # Attach LogSubscriber for automatic structured logging
      OutboxRelay::LogSubscriber.attach_to :outbox_relay

      config.after_initialize do |app|
        OutboxRelay.logger = config.outbox_relay.logger || Rails.logger
      end

      ActiveSupport.on_load(:outbox_relay) do
        OutboxRelay.logger = ::Rails.logger if OutboxRelay.logger == OutboxRelay::DEFAULT_LOGGER
      end
    end

    initializer "outbox_relay.active_record" do
      ActiveSupport.on_load(:active_record) do
        # Ensure models are loaded
        require "outbox_relay/models/outbox_event"
        require "outbox_relay/models/consumer_offset"
        require "outbox_relay/models/consumer_control"
        require "outbox_relay/models/dead_letter_event"
        require "outbox_relay/models/outbox_consumer"
      end
    end

    rake_tasks do
      load "outbox_relay/tasks.rb"
    end
  end
end
