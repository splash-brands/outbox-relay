# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"

module OutboxRelay
  module Generators
    class AddDlqCleanupIndexGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration

      source_root File.expand_path("templates", __dir__)

      desc "Adds the partial index that CleanupExpiredEventsJob uses to delete resolved DLQ entries"

      def copy_migration
        migration_template(
          "add_dlq_cleanup_index.rb.erb",
          "db/migrate/add_dlq_cleanup_index_to_outbox_relay_dead_letter_events.rb"
        )
      end

      private

      def migration_version
        "[#{ActiveRecord::Migration.current_version}]"
      end
    end
  end
end
