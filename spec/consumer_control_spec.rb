# frozen_string_literal: true

require "spec_helper"

RSpec.describe OutboxRelay::ConsumerControl do
  describe "validations" do
    it "requires a consumer_group" do
      control = described_class.new(consumer_group: nil)
      expect(control).not_to be_valid
      expect(control.errors[:consumer_group]).to be_present
    end
  end

  describe ".disabled scope" do
    it "returns only rows whose disabled boolean is true" do
      enabled = described_class.create!(consumer_group: "billing", disabled: false)
      disabled = described_class.create!(consumer_group: "shipping", disabled: true, disabled_at: Time.current)

      expect(described_class.disabled).to include(disabled)
      expect(described_class.disabled).not_to include(enabled)
    end

    it "ignores disabled_at when the boolean is false" do
      # disabled_at may linger as audit metadata after re-enabling; the boolean wins.
      row = described_class.create!(consumer_group: "billing", disabled: false, disabled_at: Time.current)

      expect(described_class.disabled).not_to include(row)
    end
  end

  describe ".disabled_consumer_groups" do
    it "returns a Set of base consumer_group names that are disabled" do
      described_class.create!(consumer_group: "shipping", disabled: true, disabled_at: Time.current)
      described_class.create!(consumer_group: "monday", disabled: true, disabled_at: Time.current)
      described_class.create!(consumer_group: "billing", disabled: false)

      expect(described_class.disabled_consumer_groups).to eq(Set["shipping", "monday"])
    end

    it "returns an empty Set when nothing is disabled" do
      described_class.create!(consumer_group: "billing", disabled: false)

      expect(described_class.disabled_consumer_groups).to eq(Set.new)
    end

    it "returns an empty Set (inert) when the backing table does not exist" do
      relation = instance_double(ActiveRecord::Relation)
      allow(described_class).to receive(:disabled).and_return(relation)
      allow(relation).to receive(:pluck)
        .and_raise(ActiveRecord::StatementInvalid.new("relation does not exist"))

      expect(described_class.disabled_consumer_groups).to eq(Set.new)
    end
  end

  describe ".disabled?" do
    it "is true for a disabled consumer group" do
      described_class.create!(consumer_group: "shipping", disabled: true, disabled_at: Time.current)
      expect(described_class.disabled?("shipping")).to be(true)
    end

    it "is false for an enabled consumer group" do
      described_class.create!(consumer_group: "billing", disabled: false)
      expect(described_class.disabled?("billing")).to be(false)
    end

    it "is false for an unknown consumer group" do
      expect(described_class.disabled?("nope")).to be(false)
    end

    it "is false (inert) when the backing table does not exist" do
      relation = instance_double(ActiveRecord::Relation)
      allow(described_class).to receive(:disabled).and_return(relation)
      allow(relation).to receive(:exists?)
        .and_raise(ActiveRecord::StatementInvalid.new("relation does not exist"))

      expect(described_class.disabled?("shipping")).to be(false)
    end
  end
end
