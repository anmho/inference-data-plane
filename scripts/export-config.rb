#!/usr/bin/env ruby
# frozen_string_literal: true

require "shellwords"
require "yaml"

config = YAML.load_file(ENV.fetch("CONFIG_FILE"))

def emit(name, value)
  return if value.nil?

  puts "#{name}=#{Shellwords.escape(value.to_s)}"
end

emit("FRONTEND_BIND_ADDR", config.dig("frontend", "bind_addr"))
emit("REDIS_URL", config.dig("broker", "url"))
emit("MODEL_ID", config.dig("model", "id"))
emit("BACKEND", config.dig("backend", "kind"))
emit("MLX_BASE_URL", config.dig("backend", "mlx_base_url"))
emit("SYNTHETIC_TOKEN_DELAY_MS", config.dig("backend", "synthetic_token_delay_ms"))
emit("API_KEY", config.dig("auth", "api_keys", 0))
emit("K6_GENERATE_VUS", config.dig("load_test", "generate_vus"))
emit("K6_STREAM_VUS", config.dig("load_test", "stream_vus"))
emit("K6_DURATION", config.dig("load_test", "duration"))
emit("K6_REQUEST_TIMEOUT", config.dig("load_test", "request_timeout"))
emit("STREAM_MAX_TOKENS", config.dig("smoke", "stream_max_tokens"))
