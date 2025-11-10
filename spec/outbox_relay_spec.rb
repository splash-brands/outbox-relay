# frozen_string_literal: true

RSpec.describe OutboxRelay do
  it "has a version number" do
    expect(OutboxRelay::VERSION).not_to be nil
  end

  it "has a logger" do
    expect(OutboxRelay.logger).not_to be nil
  end

  it "has configurable shutdown timeout" do
    expect(OutboxRelay.shutdown_timeout).to be_a(Numeric)
  end
end
