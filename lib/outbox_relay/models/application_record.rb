# frozen_string_literal: true

module OutboxRelay
  class ApplicationRecord < ActiveRecord::Base
    self.abstract_class = true

    # Configure table name prefix for all OutboxRelay models
    # This ensures proper table name resolution in production:
    #   OutboxRelay::OutboxEvent → outbox_relay_outbox_events
    #   OutboxRelay::ConsumerOffset → outbox_relay_consumer_offsets
    #   OutboxRelay::DeadLetterEvent → outbox_relay_dead_letter_events
    #
    # Without this, ActiveRecord infers table names without the namespace prefix,
    # causing "relation does not exist" errors in production.
    self.table_name_prefix = "outbox_relay_"
  end
end
