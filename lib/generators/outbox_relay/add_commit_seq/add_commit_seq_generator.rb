# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"

module OutboxRelay
  module Generators
    # Stage 1 of the SB-2140 fix (long-transaction offset skipping).
    #
    # Adds the commit-ordered sequence machinery WITHOUT changing consumer
    # behaviour: a per-partition sequencer table, a `commit_seq` column on
    # outbox_relay_outbox_events, and a DEFERRABLE INITIALLY DEFERRED constraint
    # trigger that assigns `commit_seq` at the COMMIT edge so that, per
    # (topic, partition_key), commit_seq order == commit order == visibility
    # order. Existing rows are backfilled with commit_seq = sequence.
    #
    # Consumers keep reading by `sequence` after this migration. The follow-up
    # gem release flips them to `commit_seq` (no offset-row migration needed).
    #
    # Run with: rails generate outbox_relay:add_commit_seq
    class AddCommitSeqGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration

      source_root File.expand_path("templates", __dir__)

      desc "Adds commit_seq, the partition sequencer, and the commit-edge trigger (SB-2140 Stage 1)"

      def copy_migration
        migration_template(
          "add_commit_seq.rb.erb",
          "db/migrate/add_commit_seq_to_outbox_relay_outbox_events.rb"
        )
      end

      private

      def migration_version
        "[#{ActiveRecord::Migration.current_version}]"
      end
    end
  end
end
