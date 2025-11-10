# frozen_string_literal: true

module OutboxRelay
  # Loads OutboxRelay configuration from YAML files with environment-specific overrides.
  #
  # Expected YAML structure:
  #   topics:
  #     <topic_name>:
  #       partitions: <number>
  #       description: <string>
  #
  #   consumer_groups:
  #     <consumer_group>:
  #       description: <string>
  #       topics:
  #         - name: <topic_name>
  #           class: <class_name>
  #           partitions: all | [0, 1, 2]
  #
  # Supports per-environment overrides:
  #   1. config/outbox_consumers.yml (base configuration)
  #   2. config/outbox_consumers.#{Rails.env}.yml (environment-specific overrides)
  #
  # Example:
  #   config = YamlConfigLoader.load(base_path: Rails.root)
  #   config[:partitions] # => {"product_updates" => 4, ...}
  #
  class YamlConfigLoader
    class ConfigurationError < StandardError; end

    def self.load(base_path:, env: nil)
      new(base_path: base_path, env: env).load
    end

    def initialize(base_path:, env: nil)
      @base_path = base_path
      @env = env || (defined?(Rails) ? Rails.env : "development")
    end

    def load
      config = load_yaml_with_overrides
      validate!(config)
      transform(config)
    end

    private

    attr_reader :base_path, :env

    def load_yaml_with_overrides
      # Sanitize base_path to prevent path traversal attacks
      # Expand to absolute path and validate it's within expected directory structure
      safe_base_path = File.expand_path(base_path)
      config_dir = File.expand_path("config", safe_base_path)

      base_config_file = File.expand_path("outbox_consumers.yml", config_dir)
      env_config_file = File.expand_path("outbox_consumers.#{env}.yml", config_dir)

      # Validate resolved paths stay within config directory (prevents traversal)
      # Using expand_path ensures ".." sequences are resolved before checking
      unless base_config_file.start_with?(config_dir) && env_config_file.start_with?(config_dir)
        raise ConfigurationError, "Configuration path traversal detected"
      end

      unless File.exist?(base_config_file)
        raise ConfigurationError, "Missing OutboxRelay configuration file: #{base_config_file}"
      end

      base_config = load_yaml_file(base_config_file, "base")

      # Merge environment-specific overrides if they exist
      if File.exist?(env_config_file)
        env_config = load_yaml_file(env_config_file, "environment-specific (#{env})")
        merged_config = deep_merge(base_config, env_config)

        OutboxRelay.logger.info(
          event_name: "yaml_config_environment_override",
          env: env,
          file: env_config_file,
          message: "Loaded environment-specific configuration overrides"
        )

        merged_config
      else
        base_config
      end
    end

    def validate!(config)
      errors = []

      # Validate topics section exists and is a Hash
      unless config.is_a?(Hash) && config["topics"].is_a?(Hash)
        errors << "Configuration must contain a 'topics' hash"
      end

      # Validate consumer_groups section exists and is a Hash
      unless config.is_a?(Hash) && config["consumer_groups"].is_a?(Hash)
        errors << "Configuration must contain a 'consumer_groups' hash"
      end

      # If basic structure is invalid, raise immediately
      raise ConfigurationError, "Invalid configuration:\n  - #{errors.join("\n  - ")}" if errors.any?

      # Validate each topic
      config["topics"]&.each do |topic_name, topic_config|
        unless topic_config.is_a?(Hash)
          errors << "Topic '#{topic_name}' must be a hash"
          next
        end

        unless topic_config["partitions"].is_a?(Integer) && topic_config["partitions"] > 0
          errors << "Topic '#{topic_name}' must have a positive integer 'partitions' value"
        end
      end

      # Validate each consumer group
      config["consumer_groups"]&.each do |group_name, group_config|
        unless group_config.is_a?(Hash)
          errors << "Consumer group '#{group_name}' must be a hash"
          next
        end

        unless group_config["topics"].is_a?(Array)
          errors << "Consumer group '#{group_name}' must have a 'topics' array"
          next
        end

        group_config["topics"].each do |topic_config|
          unless topic_config.is_a?(Hash)
            errors << "Consumer group '#{group_name}' topics must be hashes"
            next
          end

          unless topic_config["name"].is_a?(String)
            errors << "Consumer group '#{group_name}' topic must have a 'name' string"
          end

          unless topic_config["class"].is_a?(String)
            errors << "Consumer group '#{group_name}' topic must have a 'class' string"
          end

          # Validate topic exists in topics section
          topic_name = topic_config["name"]
          unless config["topics"]&.key?(topic_name)
            errors << "Consumer group '#{group_name}' references undefined topic '#{topic_name}'"
          end

          # Validate partitions if specified
          partitions = topic_config["partitions"]
          next if partitions.nil? || partitions == "all"

          unless partitions.is_a?(Array) && partitions.all? { |p| p.is_a?(Integer) && p >= 0 }
            errors << "Consumer group '#{group_name}' topic '#{topic_name}' partitions must be 'all' or array of non-negative integers"
          end
        end
      end

      raise ConfigurationError, "Invalid configuration:\n  - #{errors.join("\n  - ")}" if errors.any?
    end

    def transform(config)
      {
        partitions: build_partitions(config["topics"]),
        topic_descriptions: build_topic_descriptions(config["topics"]),
        consumer_groups: build_consumer_groups(config["consumer_groups"]),
      }
    end

    def build_partitions(topics)
      topics.each_with_object({}) do |(topic_name, topic_config), hash|
        hash[topic_name] = topic_config["partitions"] || 1
      end
    end

    def build_topic_descriptions(topics)
      topics.each_with_object({}) do |(topic_name, topic_config), hash|
        hash[topic_name] = topic_config["description"] || ""
      end
    end

    def build_consumer_groups(consumer_groups)
      consumer_groups.each_with_object({}) do |(group_name, group_config), hash|
        hash[group_name] = {
          "description" => group_config["description"] || "",
          "topics" => group_config["topics"] || [],
        }
      end
    end

    # Load and parse YAML file with comprehensive error handling
    #
    # @param file_path [String] Path to YAML file
    # @param file_type [String] Description for error messages (e.g., "base", "environment-specific")
    # @return [Hash] Parsed YAML content
    # @raise [ConfigurationError] If file cannot be loaded or parsed
    def load_yaml_file(file_path, file_type)
      YAML.safe_load_file(file_path, permitted_classes: [Symbol], aliases: true) || {}
    rescue Errno::EACCES => e
      raise ConfigurationError, "Permission denied reading #{file_type} configuration file: #{file_path} (#{e.message})"
    rescue Errno::EISDIR => e
      raise ConfigurationError, "Expected file but found directory: #{file_path} (#{e.message})"
    rescue Errno::ENOENT => e
      raise ConfigurationError, "Configuration file not found: #{file_path} (#{e.message})"
    rescue Errno::EMFILE, Errno::ENFILE => e
      raise ConfigurationError, "Too many open files - cannot load #{file_type} configuration: #{file_path} (#{e.message})"
    rescue Psych::SyntaxError => e
      raise ConfigurationError, "Invalid YAML syntax in #{file_type} configuration file: #{file_path}\n#{e.message}"
    rescue Psych::BadAlias => e
      raise ConfigurationError, "Invalid YAML alias in #{file_type} configuration file: #{file_path} (#{e.message})"
    rescue StandardError => e
      # Log unexpected errors for debugging
      OutboxRelay.logger.error(
        event_name: "unexpected_yaml_load_error",
        file_type: file_type,
        file_path: file_path,
        error_class: e.class.name,
        error: e.message,
        backtrace: e.backtrace&.first(10)&.join("\n")
      )
      raise ConfigurationError, "Unexpected error loading #{file_type} configuration file: #{file_path} (#{e.class}: #{e.message})"
    end

    # Deep merge two hashes (environment overrides base config)
    #
    # Arrays are intentionally REPLACED (not merged/appended):
    #   - Base: topics: [{name: "a"}, {name: "b"}]
    #   - Override: topics: [{name: "c"}]
    #   - Result: topics: [{name: "c"}]  # Complete replacement
    #
    # This follows Rails environment config pattern where environment-specific
    # configs completely override base configs, not extend them.
    #
    # For additive array merging, use separate environment files that don't
    # duplicate keys, or manually combine arrays in the override file.
    def deep_merge(base, override)
      base.merge(override) do |_key, base_val, override_val|
        if base_val.is_a?(Hash) && override_val.is_a?(Hash)
          deep_merge(base_val, override_val)
        else
          # Non-hash values (including Arrays) are replaced, not merged
          override_val
        end
      end
    end
  end
end
