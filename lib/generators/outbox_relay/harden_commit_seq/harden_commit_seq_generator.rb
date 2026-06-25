# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"

module OutboxRelay
  module Generators
    # Stage 3 of the SB-2140 fix — fail-loud hardening.
    #
    # Run this ONLY after `add_commit_seq` has been deployed and verified in
    # production (no committed rows with commit_seq IS NULL, consumers happily
    # reading by commit_seq).
    #
    # It installs a second DEFERRABLE INITIALLY DEFERRED constraint trigger that
    # raises at COMMIT if any just-inserted row was left with a NULL commit_seq —
    # i.e. if the assignment trigger was dropped or disabled. This converts the
    # worst failure mode (silent un-fetchable NULL rows → silent data loss, the
    # exact class of bug SB-2140 fixes) into a loud, fail-closed error.
    #
    # NOTE: a plain `ALTER COLUMN commit_seq SET NOT NULL` does NOT work here —
    # commit_seq is NULL during INSERT and only assigned by the DEFERRED trigger at
    # commit, so a column NOT NULL constraint (checked at insert time) would reject
    # every insert. A deferred assert trigger is the correct mechanism.
    #
    # Run with: rails generate outbox_relay:harden_commit_seq
    class HardenCommitSeqGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration

      source_root File.expand_path("templates", __dir__)

      desc "Adds the deferred assert trigger that fails loud if commit_seq is left unassigned (SB-2140 Stage 3)"

      def copy_migration
        migration_template(
          "harden_commit_seq.rb.erb",
          "db/migrate/harden_commit_seq_on_outbox_relay_outbox_events.rb"
        )
      end

      private

      def migration_version
        "[#{ActiveRecord::Migration.current_version}]"
      end
    end
  end
end
