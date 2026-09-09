# frozen_string_literal: true

require_relative "../large_body_source"

module Fixtures
  # Fixture Rack app for the Phase 3 gate (see PLAN.md, Phase 3 "Gate"): a
  # response body that streams a large (~200 MB), deterministic payload via
  # `#each`, one chunk at a time, rather than materializing it as a single
  # Array/String -- the shape PLAN.md's gate description asks for ("an
  # object responding to #each that yields deterministic chunks").
  class LargeBodyApp
    def call(_env)
      headers = {
        "content-type" => "application/octet-stream",
        "content-length" => LargeBodySource::TOTAL_BYTES.to_s
      }
      [200, headers, Body.new]
    end

    # The actual Rack body object. A plain Array of 200 x 1 MiB strings
    # would technically satisfy Rack's Body contract too, but would defeat
    # the purpose of this fixture on the Ruby side (holding the whole
    # payload in one Ruby object) even before the server gets any chance to
    # stream or buffer it -- `#each` here generates and yields one chunk at
    # a time instead, matching `LargeBodySource.each_chunk`.
    class Body
      def each(&block)
        LargeBodySource.each_chunk(&block)
      end
    end
  end
end
