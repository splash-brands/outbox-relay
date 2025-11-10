# frozen_string_literal: true

require "rails"

module OutboxRelay
  class Engine < ::Rails::Engine
    isolate_namespace OutboxRelay

    config.outbox_relay = ActiveSupport::OrderedOptions.new

    initializer "outbox_relay.configs" do
      config.after_initialize do |app|
        OutboxRelay.logger = app.config.outbox_relay.logger || Rails.logger
        OutboxRelay.custom_logger = Rails.application.config.custom_logger if Rails.application.config.respond_to?(:custom_logger)

        # Configure Rails executor wrapper for proper context management
        # This ensures database connections, code reloading, and other Rails
        # features work correctly in forked worker processes
        OutboxRelay.app_executor = app.executor
      end
    end

    initializer "outbox_relay.active_record" do
      ActiveSupport.on_load(:active_record) do
        # Ensure models are loaded
        require "outbox_relay/models/outbox_event"
        require "outbox_relay/models/consumer_offset"
        require "outbox_relay/models/dead_letter_event"
        require "outbox_relay/models/outbox_consumer"
      end
    end

    rake_tasks do
      load "outbox_relay/tasks.rb"
    end
  end
end
