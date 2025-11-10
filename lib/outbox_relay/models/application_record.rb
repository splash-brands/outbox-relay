# frozen_string_literal: true

module OutboxRelay
  class ApplicationRecord < ActiveRecord::Base
    self.abstract_class = true
  end
end
