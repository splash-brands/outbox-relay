# frozen_string_literal: true

require 'concurrent'

module OutboxRelay
  class Configuration
    DEFAULT_POLLING_INTERVAL = 1.0 # seconds
    DEFAULT_BATCH_SIZE = 100
    DEFAULT_MAX_LOOPS = 1000 # prevent infinite loops

    # Monitoring defaults
    DEFAULT_LAG_ALERT_THRESHOLD = 100
    DEFAULT_ORPHAN_CHECK_INTERVAL = 30 # seconds
    DEFAULT_STALE_WORKER_TIMEOUT = 60 # seconds

    attr_accessor :polling_interval, :batch_size, :max_loops, :workers_config, :topic_descriptions,
                  :consumer_group_configs
    attr_reader :partitions, :monitoring_config

    def initialize(options = {})
      @polling_interval = options[:polling_interval] || DEFAULT_POLLING_INTERVAL
      @batch_size = options[:batch_size] || DEFAULT_BATCH_SIZE
      @max_loops = options[:max_loops] || DEFAULT_MAX_LOOPS

      # Load configuration from YAML
      yaml_config = load_from_yaml(options[:base_path])
      @partitions = (yaml_config[:partitions] || {}).freeze
      @topic_descriptions = (yaml_config[:topic_descriptions] || {}).freeze
      @consumer_group_configs = (yaml_config[:consumer_groups] || {}).freeze
      @monitoring_config = build_monitoring_config(yaml_config[:monitoring] || {}).freeze

      @workers_config = load_workers_config(options).freeze

      # Freeze configuration to prevent modification after initialization
      # This ensures thread-safe read access without needing locks
      # Skip freezing in test environment to allow mocking
      freeze unless defined?(RSpec)
    end

    def workers
      @workers_config.map do |worker_config|
        WorkerConfig.new(worker_config)
      end
    end

    def valid?
      errors.empty?
    end

    def errors
      # Do NOT memoize - errors must reflect current state
      # Configuration attrs (polling_interval, batch_size) can change after initialization
      [].tap do |errs|
        errs << 'No workers configured' if workers.empty?
        errs << 'Invalid polling interval' if polling_interval <= 0
        errs << 'Invalid batch size' if batch_size <= 0
      end
    end

    # Monitoring configuration convenience accessors
    def lag_alert_threshold
      monitoring_config[:lag_alert_threshold]
    end

    def orphan_check_interval
      monitoring_config[:orphan_check_interval]
    end

    def stale_worker_timeout
      monitoring_config[:stale_worker_timeout]
    end

    private

    def load_from_yaml(base_path = nil)
      base_path ||= defined?(Rails) ? Rails.root : Dir.pwd

      YamlConfigLoader.load(base_path: base_path)
    rescue YamlConfigLoader::ConfigurationError => e
      OutboxRelay.logger.error(
        event_name: 'yaml_config_load_failed',
        error: e.message,
        message: 'Failed to load OutboxRelay configuration from YAML'
      )
      raise
    end

    # Build monitoring configuration with defaults
    #
    # Example YAML config:
    #   monitoring:
    #     lag_alert_threshold: 100     # Alert when partition lag exceeds this value
    #     orphan_check_interval: 30    # How often to check for orphaned partitions (seconds)
    #     stale_worker_timeout: 60     # Consider worker stale after no heartbeat for this long
    #
    def build_monitoring_config(yaml_monitoring)
      {
        lag_alert_threshold: yaml_monitoring['lag_alert_threshold'] || DEFAULT_LAG_ALERT_THRESHOLD,
        orphan_check_interval: yaml_monitoring['orphan_check_interval'] || DEFAULT_ORPHAN_CHECK_INTERVAL,
        stale_worker_timeout: yaml_monitoring['stale_worker_timeout'] || DEFAULT_STALE_WORKER_TIMEOUT
      }
    end

    def load_workers_config(_options)
      # Load from consumer_group_configs (loaded from YAML)
      workers = []

      @consumer_group_configs.each do |consumer_group, group_config|
        group_config['topics'].each do |topic_config|
          # CRITICAL: Do NOT query database OR load consumer classes before forking!
          #
          # Why: Fork-safety on macOS (and some Linux versions) requires:
          # 1. No database connections opened before fork (GSS/Kerberos authentication issues)
          # 2. No ActiveRecord model classes loaded (triggers connection establishment)
          # 3. No Objective-C runtime initialization (macOS-specific)
          #
          # The partition count AND consumer class loading are lazy-loaded by WorkerConfig
          # when each worker boots (after fork).
          #
          # IMPORTANT: topic_config["class"] must be a STRING class name, never a constant:
          #   ✓ GOOD: "NotificationService::OrderConsumer"
          #   ✗ BAD:  NotificationService::OrderConsumer (triggers class loading)
          #
          # See: lib/outbox_relay/processes/runnable.rb:39-63 for post-fork reconnection

          topic_name = topic_config['name']
          consumer_class = topic_config['class']

          # Partition configuration from YAML (if specified)
          # Can be "all" (process all partitions) or [0, 1, 2] (specific partitions)
          partition_spec = topic_config['partitions']

          # Convert "all" to nil (will fetch all partitions from configuration)
          partition_count = if partition_spec == 'all' || partition_spec.nil?
                              nil # Will be fetched from configuration.partitions
                            elsif partition_spec.is_a?(Array)
                              # Specific partitions specified - extract unique partition keys
                              partition_spec.uniq
                            else
                              raise OutboxRelay::ConfigurationError,
                                    "Invalid partitions specification for #{consumer_group}/#{topic_name}: #{partition_spec.inspect}"
                            end

          workers << {
            consumer_group: consumer_group,
            topic: topic_name,
            consumer_class: consumer_class,
            partition_spec: partition_spec, # Store original spec for validation
            partition_count: partition_count # nil or Array of specific partitions
          }
        end
      end

      workers
    end

    class WorkerConfig
      attr_reader :consumer_group, :topic, :consumer_class, :partition_spec

      def initialize(config)
        @consumer_group = config[:consumer_group]
        @topic = config[:topic]
        @consumer_class = config[:consumer_class]
        @partition_spec = config[:partition_spec] # Can be "all", Array, or nil
        @explicit_partitions = config[:partition_count] # Array or nil

        # Thread-safe lazy initialization using Concurrent::AtomicReference
        # Prevents race condition where multiple threads query database simultaneously
        @partition_count_cache = Concurrent::AtomicReference.new(nil)
      end

      def partition_count
        # Thread-safe lazy initialization pattern using compare_and_set
        # If cache is nil, attempt to fetch and set atomically
        # If another thread wins the race, compare_and_set returns false but cache is now populated
        # Either way, cache is guaranteed to be set after this line
        @partition_count_cache.compare_and_set(nil, fetch_partition_count)

        # Return cached value (always populated at this point, either by this thread or a concurrent one)
        @partition_count_cache.get
      end

      def partitions
        # If specific partitions are configured, use those
        return @explicit_partitions.uniq.sort if @explicit_partitions.is_a?(Array)

        # Otherwise, use all partitions (0...partition_count)
        (0...partition_count).to_a

        # REMOVED: Orphaned partition check
        # Previously queried database here to check for orphaned events
        # This violated fork-safety principles (database query before fork)
        # Orphaned events will be logged by workers when they encounter them
        # rather than during configuration loading
        #
        # NOTE: If you need to check for orphaned partitions, run a rake task
        # after workers are started, not during configuration loading
      end

      def instantiate(partition_key:)
        {
          consumer_class: consumer_class,
          consumer_group: consumer_group,
          topic: topic,
          partition_key: partition_key
        }
      end

      private

      def fetch_partition_count
        # Priority 1: Explicit partition list in config
        return @explicit_partitions.size if @explicit_partitions.is_a?(Array)

        # Priority 2: Configuration partitions (from YAML)
        return OutboxRelay.configuration.partitions[topic] if OutboxRelay.configuration.partitions[topic]

        # Priority 3: Query database for actual partitions (fallback)
        OutboxRelay.logger.warn(
          event_name: 'partition_count_not_in_yaml',
          topic: topic,
          message: 'Topic not found in configuration, querying database...'
        )

        count = OutboxRelay::OutboxEvent
                .where(topic: topic)
                .distinct
                .count(:partition_key)

        if count.zero?
          OutboxRelay.logger.warn(
            event_name: 'partition_count_zero_defaulting',
            topic: topic,
            partition_count: 1,
            message: 'No events found for topic - defaulting to 1 partition. ' \
                     'Add topic to config/outbox_consumers.yml with partition count.'
          )
          return 1
        end

        count
      rescue StandardError => e
        OutboxRelay.logger.error(
          event_name: 'partition_count_query_failed',
          topic: topic,
          error: e.message,
          error_class: e.class.name,
          backtrace: e.backtrace&.first(10)&.join("\n")
        )

        OutboxRelay::Instrumentation::Configuration.partition_count_query_failed(
          e,
          topic: topic
        )

        raise OutboxRelay::ConfigurationError, "Failed to determine partition count for topic '#{topic}': #{e.message}"
      end
    end
  end
end
