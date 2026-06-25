# frozen_string_literal: true

source "https://rubygems.org"

# Specify your gem's dependencies in outbox_relay.gemspec
gemspec

gem "irb"
gem "rake", "~> 13.0"

# PostgreSQL driver for the trigger integration specs (spec/pg). Optional so the
# default SQLite test run does not need libpq. CI's spec_pg job sets BUNDLE_WITH=pg;
# spec/pg skips when the pg gem or a database is unavailable.
group :pg, optional: true do
  gem "pg", "~> 1.5"
end
