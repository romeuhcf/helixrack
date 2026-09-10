# frozen_string_literal: true

require "net/http"
require_relative "../support/phase7_server_helper"

# Phase 7 gate (see `PLAN.md`, Phase 7 "Gate"; read that section, and
# `ext/helix_rack/src/lib.rs`'s `Handler::call`/`RackAppHandler::handle` doc
# comments, before touching this file).
#
# What this proves: for each of three distinct fault modes -- a raised
# `StandardError`, a `SystemStackError` from deep recursion, and a
# deliberate Rust panic inside `RackAppHandler::handle`'s own logic -- (a)
# the response is exactly `500`, (b) the server subprocess's PID is still
# alive and unchanged afterward, (c) the very next, unrelated request on a
# fresh connection still succeeds with `200`, and (d) the fault was actually
# contained by the mechanism this gate claims -- the server's own stderr
# (`ext/helix_rack/src/lib.rs`'s `log_fault`) says `"request handler
# panicked"` for the panic row and `"request failed"` for the two Ruby-
# exception rows, not just "some 500 came back". (d) exists because of a
# real regression this gate would otherwise miss: PLAN.md's Phase 7
# Resolution note records that an earlier version of the third fixture row
# produced an identical `500`/alive/`200` result while silently landing in
# the *wrong* code path (a Ruby exception `Handler::call` already knew how
# to contain, not the new `catch_unwind`) -- (a)-(c) alone could not have
# caught that, only the stderr content could. All four are exact,
# boolean/substring assertions, not timing- or heuristic-based.
#
# One subprocess for the whole gate (booted once per example via
# `with_helix_rack_subprocess`, not shared across examples) -- each example
# gets its own fresh process and stderr log so a failure in one fault mode
# can't pollute another's log-content assertion.
# rubocop:disable Metrics/BlockLength -- one real-server integration example
# per fault mode plus the table driving it reads better kept together in
# one gate file, matching spec/integration/phase5_gvl_discipline_spec.rb's
# precedent, than split across files for a line-count target.
RSpec.describe "Phase 7: fault containment gate (RNF04)" do
  include Phase7::ServerHelper

  # rubocop:disable Lint/ConstantDefinitionInBlock -- deliberately real
  # constants (matching spec/integration/phase5_gvl_discipline_spec.rb's
  # SIGNAL_TIMEOUT_SECONDS and friends), not `let`s: fixed configuration for
  # the whole gate, not per-example state.
  FAULT_APP_PATH = File.expand_path("../fixtures/apps/phase7_fault.ru", __dir__)

  FAULT_MODES = [
    {
      name: "a raised StandardError",
      request: ->(port) { Net::HTTP.get_response("127.0.0.1", "/raise_standard_error", port) },
      expected_log_fragment: "request failed"
    },
    {
      name: "a SystemStackError from deep recursion",
      request: ->(port) { Net::HTTP.get_response("127.0.0.1", "/raise_system_stack_error", port) },
      expected_log_fragment: "request failed"
    },
    {
      name: "a deliberate Rust panic inside RackAppHandler#handle",
      request: lambda { |port|
        Net::HTTP.start("127.0.0.1", port) do |http|
          http.get("/", { "X-HelixRack-Debug-Panic" => "1" })
        end
      },
      expected_log_fragment: "request handler panicked"
    }
  ].freeze
  # rubocop:enable Lint/ConstantDefinitionInBlock

  FAULT_MODES.each do |fault_mode|
    it "contains #{fault_mode[:name]} as a 500, keeps the process alive, and serves the next request" do
      with_helix_rack_subprocess(FAULT_APP_PATH) do |pid, port, stderr_log_path|
        baseline_size = stderr_log_size(stderr_log_path)
        response = fault_mode[:request].call(port)

        expect(response.code).to eq("500")
        expect(process_alive?(pid)).to be(true)
        expect(new_stderr_content(stderr_log_path, baseline_size)).to include(fault_mode[:expected_log_fragment])

        next_response = Net::HTTP.get_response("127.0.0.1", "/", port)
        expect(next_response.code).to eq("200")
        expect(next_response.body).to eq("ok")
      end
    end
  end
end
# rubocop:enable Metrics/BlockLength
