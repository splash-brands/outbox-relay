# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"

module OutboxRelay
  module Generators
    class AddDlqRetryBackoffGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration

      source_root File.expand_path("templates", __dir__)

      desc "Adds retry_after column to dead_letter_events for exponential backoff"

      def copy_migration
        migration_template(
          "add_retry_after_to_dead_letter_events.rb.erb",
          "db/migrate/add_retry_after_to_outbox_relay_dead_letter_events.rb"
        )
      end

      private

      def migration_version
        "[#{ActiveRecord::Migration.current_version}]"
      end
    end
  end
end
