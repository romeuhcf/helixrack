# frozen_string_literal: true

require "json"

module Fixtures
  # Fixture Rack app for the Phase 2 gate (see PLAN.md, Phase 2 "Gate"):
  # returns the Rack env as JSON, with the two adjustments PLAN.md's gate
  # description specifies since not all env values are JSON-native:
  #
  # * `rack.input` is reported as `env['rack.input'].read` (the request
  #   body string), not the IO object itself.
  # * `rack.errors` is reported as `true` (the key is present and responds
  #   to `#puts`), not the IO object itself.
  #
  # Only the keys PLAN.md's gate mandates are reported. This keeps JSON
  # generation simple and independent of whatever else a real server
  # implementation puts in `env` -- for example, `Rack::Lint` wraps
  # `rack.input`/`rack.errors` in its own objects that aren't JSON
  # -serializable, and may add its own bookkeeping keys (`rack.lint`).
  class EchoApp
    MANDATED_ENV_KEYS = %w[
      REQUEST_METHOD
      PATH_INFO
      QUERY_STRING
      SERVER_NAME
      SERVER_PORT
    ].freeze

    def call(env)
      json = JSON.generate(mandated_env(env))
      [200, { "content-type" => "application/json", "content-length" => json.bytesize.to_s }, [json]]
    end

    private

    def mandated_env(env)
      MANDATED_ENV_KEYS.each_with_object({}) { |key, hash| hash[key] = env[key] }.merge(
        "rack.version" => env["rack.version"],
        "rack.input" => env["rack.input"]&.read,
        "rack.errors" => env["rack.errors"] ? true : false,
        "rack.url_scheme" => env["rack.url_scheme"]
      )
    end
  end
end
