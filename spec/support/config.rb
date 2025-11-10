# frozen_string_literal: true

# Mock YAML configuration for tests
# This prevents YamlConfigLoader from trying to read actual YAML files
RSpec.configure do |config|
  config.before(:each) do
    # Mock YamlConfigLoader to return test configuration
    allow(OutboxRelay::YamlConfigLoader).to receive(:load).and_return({
      partitions: {
        "test_topic" => 4,
        "single_partition_topic" => 1,
      },
      topic_descriptions: {
        "test_topic" => "Test topic for specs",
        "single_partition_topic" => "Single partition test topic",
      },
      consumer_groups: {
        "test_consumer_group" => {
          "description" => "Test consumer group",
          "topics" => [
            {
              "name" => "test_topic",
              "class" => "TestConsumer",
              "partitions" => "all",
            },
          ],
        },
      },
    })

    # Reset configuration instance between tests
    OutboxRelay.instance_variable_set(:@configuration, nil)
  end
end
