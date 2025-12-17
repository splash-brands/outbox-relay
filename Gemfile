# frozen_string_literal: true

source "https://rubygems.org"

# Specify your gem's dependencies in outbox_relay.gemspec
gemspec

gem "irb"
gem "rake", "~> 13.0"

# CI matrix: test against different Rails versions
# RAILS_VERSION env var is set by GitHub Actions workflow
if ENV["RAILS_VERSION"]
  gem "activerecord", ENV["RAILS_VERSION"]
  gem "activesupport", ENV["RAILS_VERSION"]
end
