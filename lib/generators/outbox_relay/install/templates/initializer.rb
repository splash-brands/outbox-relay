# frozen_string_literal: true

OutboxRelay.configure do |config|
  # Polling interval in seconds (how often workers check for new events when idle)
  config.polling_interval = 1.0

  # Batch size for processing events
  config.batch_size = 100

  # Max loops before worker restart (prevents memory leaks)
  config.max_loops = 1000

  # Shutdown timeout (how long to wait for graceful shutdown)
  config.shutdown_timeout = 30.seconds

  # Silence ActiveRecord query logs for polling
  config.silence_polling = true
end

# Configure custom logger if available
if Rails.application.config.respond_to?(:custom_logger)
  OutboxRelay.custom_logger = Rails.application.config.custom_logger
end
