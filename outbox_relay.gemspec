# frozen_string_literal: true

require_relative "lib/outbox_relay/version"

Gem::Specification.new do |spec|
  spec.name = "outbox_relay"
  spec.version = OutboxRelay::VERSION
  spec.authors = ["Rafal Grabowski"]
  spec.email = ["rafal.grabowski@gmail.com"]

  spec.summary = "Production-ready transactional outbox pattern with continuous polling"
  spec.description = "A high-performance, Solid Queue-inspired continuous polling system for processing PostgreSQL outbox events with sub-second latency. Implements the transactional outbox pattern with fork-based workers, optimistic locking, dead letter queues, and intelligent backlog detection."
  spec.homepage = "https://github.com/splash-brands/outbox-relay"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.1.0"
  spec.required_rubygems_version = ">= 3.0.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/splash-brands/outbox-relay"
  spec.metadata["changelog_uri"] = "https://github.com/splash-brands/outbox-relay/blob/main/CHANGELOG.md"
  spec.metadata["bug_tracker_uri"] = "https://github.com/splash-brands/outbox-relay/issues"
  spec.metadata["documentation_uri"] = "https://github.com/splash-brands/outbox-relay/blob/main/README.md"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  gemspec = File.basename(__FILE__)
  spec.files = IO.popen(%w[git ls-files -z], chdir: __dir__, err: IO::NULL) do |ls|
    ls.readlines("\x0", chomp: true).reject do |f|
      (f == gemspec) ||
        f.start_with?(*%w[bin/ test/ spec/ features/ .git appveyor Gemfile]) ||
        f.end_with?(".gem")
    end
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  # Core dependencies
  # Support Rails 7.0, 7.1, 7.2, and 8.0
  spec.add_dependency "activerecord", ">= 7.0", "< 9.0"
  spec.add_dependency "activesupport", ">= 7.0", "< 9.0"
  spec.add_dependency "concurrent-ruby", "~> 1.3"
  spec.add_dependency "thor", "~> 1.3"

  # Development dependencies (only needed for gem development, not for users)
  spec.add_development_dependency "sqlite3", "~> 2.1"
  spec.add_development_dependency "rspec", "~> 3.13"
end
