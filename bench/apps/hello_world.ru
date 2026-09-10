# frozen_string_literal: true

# Phase 13 (PLAN.md, Phase 13, PRD.md 7.2's "Payload Leve (Hello World /
# Ping)" scenario): the minimum possible Rack app, to measure raw network-
# parser throughput with as little app-level work as possible in the way.
run(lambda do |_env|
  [200, { "content-type" => "text/plain", "content-length" => "13" }, ["Hello, World!"]]
end)
