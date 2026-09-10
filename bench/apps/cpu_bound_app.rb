# frozen_string_literal: true

require "grape"
require "json"

module Bench
  # Phase 13 (PLAN.md, Phase 13, PRD.md 7.2's "CPU-Bound Leve (Grape +
  # Serialization JSON)" scenario): a real CPU-bound endpoint -- building
  # and serializing a nested payload, no I/O -- to measure the preemption
  # mechanism's effectiveness (Phase 6/RF07) against queueing delay, not
  # network-parser throughput (that's the Hello World scenario's job).
  class CpuBoundApp < Grape::API
    format :json

    # Deliberately not a constant computed once: PRD.md 7.2 wants each
    # request to do real, repeatable CPU work (building the array *and*
    # serializing it), not just replay a cached string.
    RECORD_COUNT = 2_000

    get :report do
      records = Array.new(RECORD_COUNT) do |i|
        {
          id: i,
          name: "widget-#{i}",
          tags: %w[alpha bravo charlie delta],
          price_cents: (i * 137) % 10_000,
          in_stock: i.even?
        }
      end
      { count: records.size, records: records }
    end
  end
end
