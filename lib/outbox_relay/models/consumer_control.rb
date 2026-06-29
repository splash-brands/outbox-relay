# frozen_string_literal: true

require "set"

module OutboxRelay
  # ConsumerControl - Database-backed kill switch for consumer groups.
  #
  # Backed by the `outbox_relay_consumer_controls` table (created by the host
  # application, see SB-2199). Each row keys off the BASE consumer_group name
  # (no `_pN` partition suffix).
  #
  # The `disabled` boolean is the source of truth read by the Supervisor: when
  # true, the consumer group is disabled and none of its workers may run. The
  # `disabled_at` timestamp is audit metadata (when the kill switch was last
  # flipped on) and is maintained by the host app alongside the boolean.
  #
  # ## Defensive table-absence handling
  #
  # The gem must boot cleanly even before the host application has run the
  # migration that creates `outbox_relay_consumer_controls`. Every query is
  # therefore wrapped to rescue `ActiveRecord::StatementInvalid` (the error
  # raised when the underlying table does not exist) and treat the kill switch
  # as inert (nothing disabled). This mirrors the defensive querying used
  # elsewhere in the gem (see `models/outbox_event.rb#next_sequence`).
  class ConsumerControl < ApplicationRecord
    validates :consumer_group, presence: true

    # Rows representing a currently-disabled consumer group.
    # Driven by the boolean for a simple, unambiguous read.
    scope :disabled, -> { where(disabled: true) }

    # Set of base consumer_group names that are currently disabled.
    #
    # Returns an empty Set if the backing table does not exist yet, so the
    # kill switch is simply inert until the host app provisions the table.
    #
    # @return [Set<String>]
    def self.disabled_consumer_groups
      disabled.pluck(:consumer_group).to_set
    rescue ActiveRecord::StatementInvalid => e
      log_table_absent(e)
      Set.new
    end

    # Whether a single base consumer_group is currently disabled.
    #
    # @param consumer_group [String] base consumer group name (no `_pN` suffix)
    # @return [Boolean]
    def self.disabled?(consumer_group)
      disabled.exists?(consumer_group: consumer_group)
    rescue ActiveRecord::StatementInvalid => e
      log_table_absent(e)
      false
    end

    def self.log_table_absent(error)
      OutboxRelay.logger.debug(
        event_name: "consumer_control_table_absent",
        error: error.message,
        message: "outbox_relay_consumer_controls table not found - kill switch inert"
      )
    end
    private_class_method :log_table_absent
  end
end
