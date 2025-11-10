# frozen_string_literal: true

module OutboxRelay
  module Processes
    module Procline
      # Update process title for better monitoring
      def procline(message)
        $0 = "outbox_relay #{kind}: #{message}"
      end
    end
  end
end
