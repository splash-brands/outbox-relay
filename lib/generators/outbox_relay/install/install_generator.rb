# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"

module OutboxRelay
  module Generators
    class InstallGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration

      source_root File.expand_path("templates", __dir__)

      desc "Creates OutboxRelay initializer and generates migrations"

      def copy_initializer
        template "initializer.rb", "config/initializers/outbox_relay.rb"
      end

      def copy_yaml_config
        template "outbox_consumers.yml", "config/outbox_consumers.yml"
      end

      def create_executable
        template "bin/outbox_relay", "bin/outbox_relay"
        chmod "bin/outbox_relay", 0755 & ~File.umask, verbose: false
      end

      def create_migrations
        # Core tables: outbox_events, consumer_offsets, dead_letter_events
        # Uses immutable event log architecture (no state machine, no lock_version)
        migration_template "create_outbox_relay_tables.rb.erb",
          "db/migrate/create_outbox_relay_tables.rb"

        # Process registry table for fault tolerance and monitoring
        # Tracks supervisor and worker processes with heartbeat mechanism
        migration_template "create_outbox_relay_processes.rb.erb",
          "db/migrate/create_outbox_relay_processes.rb"
      end

      def show_readme
        readme "README" if behavior == :invoke
      end
    end
  end
end
