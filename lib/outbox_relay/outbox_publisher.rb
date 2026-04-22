# frozen_string_literal: true

require 'zlib'

module OutboxRelay
  # Service class for publishing events to the outbox with automatic partition calculation
  #
  # This class provides a high-level interface for publishing events, handling:
  # - Automatic partition key calculation using CRC32 for deterministic distribution
  # - Header normalization (event_name, partition_key)
  # - Validation and error handling
  #
  # @example Basic usage
  #   OutboxPublisher.publish(
  #     topic: "order_updates",
  #     payload: { order_id: 123 },
  #     headers: {
  #       event_name: "created",
  #       partition_key: "order-123"
  #     }
  #   )
  #
  # @example Without partition key (defaults to 0)
  #   OutboxPublisher.publish(
  #     topic: "notifications",
  #     payload: { message: "Hello" },
  #     headers: { event_name: "sent" }
  #   )
  class OutboxPublisher
    class PublishError < StandardError; end

    class << self
      # Publishes an event to the outbox
      #
      # @param topic [String] The topic name (required)
      # @param payload [Hash, Array] The event payload (required)
      # @param headers [Hash] Additional headers including event_name and partition_key
      # @option headers [String] :event_name The event type (e.g., "created", "updated")
      # @option headers [String] :partition_key String key for partition distribution
      # @param opts [Hash] Optional keyword arguments
      # @option opts [Time, nil] :expires_at Event expiration time
      #   - Not passed: falls back to OutboxRelay.default_event_ttl (if set)
      #   - Explicit Time: used as-is
      #   - Explicit nil: event never expires (opt-out from default TTL)
      #
      # @return [OutboxRelay::OutboxEvent] The created event
      #
      # @raise [PublishError] If publishing fails
      #
      # @example With partition key
      #   OutboxPublisher.publish(
      #     topic: "order_updates",
      #     payload: { order_id: 123, total: 99.99 },
      #     headers: {
      #       event_name: "created",
      #       partition_key: "customer-456"  # Will be hashed to partition number
      #     }
      #   )
      #
      # @example With explicit expiration
      #   OutboxPublisher.publish(
      #     topic: "temporary_events",
      #     payload: { session_id: "abc123" },
      #     headers: { event_name: "heartbeat" },
      #     expires_at: 5.minutes.from_now
      #   )
      #
      # @example Opt out of default TTL (never expire)
      #   OutboxPublisher.publish(
      #     topic: "audit_log",
      #     payload: { action: "user_deleted" },
      #     headers: { event_name: "deleted" },
      #     expires_at: nil
      #   )
      def publish(topic:, payload:, headers: {}, **opts)
        # Validate parameters first (ArgumentError bubbles up directly)
        validate_parameters!(topic: topic, payload: payload, headers: headers)

        expires_at = resolve_expires_at(opts)

        # Extract and normalize headers
        event_name = headers[:event_name] || headers['event_name']
        partition_key_string = headers[:partition_key] || headers['partition_key']

        # Calculate numeric partition key from string key using CRC32
        partition_key = if partition_key_string.present?
                          calculate_partition_key(partition_key_string, topic)
                        else
                          0 # Default partition
                        end

        # Remove event_name and partition_key from headers since they're stored as separate fields
        filtered_headers = headers.except(:event_name, 'event_name', :partition_key, 'partition_key')

        # Create the event
        OutboxRelay::OutboxEvent.create!(
          topic: topic,
          event_name: event_name,
          payload: payload,
          headers: filtered_headers,
          partition_key: partition_key,
          expires_at: expires_at
        )
      rescue ArgumentError
        # Let ArgumentError bubble up directly (for parameter validation)
        raise
      rescue ActiveRecord::RecordInvalid => e
        raise PublishError, "Failed to publish event: #{e.message}"
      rescue StandardError => e
        raise PublishError, "Unexpected error publishing event: #{e.message}"
      end

      private

      # Calculates a numeric partition key from a string key using CRC32
      #
      # This ensures:
      # - Same string always maps to same partition (deterministic)
      # - Even distribution across partitions
      # - Fast calculation (CRC32 is very efficient)
      #
      # @param key [String] The string key (e.g., "customer-123", "order-456")
      # @param topic [String] The topic name (for logging/debugging)
      # @return [Integer] The partition number (0 to partition_count-1)
      #
      # @example
      #   calculate_partition_key("customer-123", "order_updates")
      #   # => 2 (assuming 4 partitions configured)
      def calculate_partition_key(key, topic)
        # Get partition count for this topic
        partition_count = fetch_partition_count(topic)

        # Calculate CRC32 hash and modulo by partition count
        crc = Zlib.crc32(key.to_s)
        partition = crc % partition_count

        OutboxRelay.logger&.debug(
          event_name: 'partition_calculated',
          topic: topic,
          partition_key_string: key,
          partition_key_numeric: partition,
          partition_count: partition_count,
          crc32: crc
        )

        partition
      end

      # Fetches the partition count for a topic
      #
      # Looks for the topic in OutboxRelay.configuration.partitions
      # or defaults to 1 (no partitioning)
      #
      # @param topic [String] The topic name
      # @return [Integer] Number of partitions (default: 1)
      def fetch_partition_count(topic)
        OutboxRelay.configuration.partitions[topic] || 1
      end

      # Validates publish parameters
      #
      # @param topic [String] Topic name
      # @param payload [Hash, Array] Event payload
      # @param headers [Hash] Event headers
      #
      # @raise [ArgumentError] If parameters are invalid
      def validate_parameters!(topic:, payload:, headers:)
        raise ArgumentError, 'topic must be present' if topic.blank?
        raise ArgumentError, 'payload must be a Hash or Array' unless payload.is_a?(Hash) || payload.is_a?(Array)
        raise ArgumentError, 'headers must be a Hash' unless headers.is_a?(Hash)

        # Validate partition_key if provided
        partition_key = headers[:partition_key] || headers['partition_key']
        return unless partition_key.present? && !partition_key.respond_to?(:to_s)

        raise ArgumentError, 'partition_key must be convertible to String'
      end

      # Resolves effective expires_at for the event.
      #
      # Semantics (distinguishes "not passed" from "passed as nil"):
      # - opts does not contain :expires_at → fall back to OutboxRelay.default_event_ttl
      # - opts contains :expires_at (even nil) → use as-is (explicit opt-out when nil)
      #
      # @param opts [Hash] keyword arguments passed to publish
      # @return [Time, nil]
      def resolve_expires_at(opts)
        return opts[:expires_at] if opts.key?(:expires_at)

        ttl = OutboxRelay.default_event_ttl
        ttl&.from_now
      end
    end
  end
end
