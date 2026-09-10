# frozen_string_literal: true

require "net/http"
require "json"
require_relative "../support/phase11_server_helper"

# Phase 11 gate (see `PLAN.md`, Phase 11 "Gate"; read that section, and
# `spec/fixtures/apps/phase11_grape.ru`'s own top comment, before touching
# this file).
#
# PRD.md section 7.1 asks for two things: the official `Rack::Lint` suite
# green, and a request-spec table (route x params x expected status/JSON
# body) covering a fixture Grape app's nested routes, param validation,
# error middleware, and JSON serialization. This file does both at once,
# deliberately, rather than as two separate gates: `Rack::Lint` is wrapped
# around the fixture app for the whole life of the one server process this
# gate boots (`spec/fixtures/apps/phase11_grape.ru`'s `use Rack::Lint`), so
# every request the table below drives through it is *also* a live
# Rack::Lint check of HelixRack's own env-construction and
# response-handling -- a `Rack::Lint::LintError` raised by any one of them
# would surface as a Ruby exception `RackAppHandler::handle`
# (`ext/helix_rack/src/lib.rs`) turns into a `500` and logs to stderr as
# "request failed" (the exact same path Phase 7's fixture app's
# `StandardError` row exercises), indistinguishable from any other app-level
# Ruby exception at the HTTP layer alone. That's why every example below
# also asserts the server's fault log gained nothing: an unexpected status
# code alone wouldn't prove *which* of "Grape's own logic is wrong" or
# "HelixRack violated the Rack SPEC" caused it, but the fault log does,
# since only the second one ever logs anything.
#
# The `/boom` and missing-`name` rows *do* produce non-2xx statuses (`500`
# and `400`), but neither is a HelixRack-side fault: both are handled
# entirely inside Grape's own `rescue_from` error middleware
# (`spec/fixtures/apps/phase11_grape_app.rb`), which returns a clean
# `[status, headers, body]` triple like any other successful request --  no
# exception ever reaches `RackAppHandler::handle`, so the fault log stays
# empty for those rows too. If a future change to the fixture app changed
# that (an uncaught exception reaching HelixRack directly), this gate would
# catch it as a fault-log-content failure even though the HTTP status might
# coincidentally still look right.
#
# One subprocess for the whole gate (booted once per example, not shared
# across examples), matching `spec/integration/phase7_fault_containment_spec.rb`'s
# precedent and its own reasoning for why: a fresh process per example means
# a bug in one scenario can't pollute another's fault-log assertion.
# rubocop:disable Metrics/BlockLength -- one real-server integration gate
# plus its own scenario table reads better kept together in one file,
# matching spec/integration/phase7_fault_containment_spec.rb's precedent,
# than split up for a line-count target.
RSpec.describe "Phase 11: Rack compliance + Grape integration (PRD 7.1)" do
  include Phase11::ServerHelper

  # rubocop:disable Lint/ConstantDefinitionInBlock -- deliberately real
  # constants (matching spec/integration/phase7_fault_containment_spec.rb's
  # FAULT_APP_PATH/FAULT_MODES), not `let`s: fixed configuration for the
  # whole gate.
  GRAPE_APP_PATH = File.expand_path("../fixtures/apps/phase11_grape.ru", __dir__)

  #
  # Every `expected_body` is a Ruby value compared against `JSON.parse`'s
  # own output, not a raw string -- immune to inconsequential key-order or
  # whitespace differences in the JSON Grape actually emits, still an exact
  # match on structure and values.
  SCENARIOS = [
    {
      name: "POST /widgets with a valid name creates a widget",
      request: ->(port) { Net::HTTP.post(URI("http://127.0.0.1:#{port}/widgets"), "name=bolt") },
      expected_status: "201",
      expected_body: { "id" => 1, "name" => "bolt" }
    },
    {
      name: "POST /widgets missing the required name is rejected by param validation",
      request: ->(port) { Net::HTTP.post(URI("http://127.0.0.1:#{port}/widgets"), "") },
      expected_status: "400",
      expected_body: { "error" => "name is missing" }
    },
    {
      name: "GET /widgets/:id resolves a route param",
      request: ->(port) { Net::HTTP.get_response("127.0.0.1", "/widgets/42", port) },
      expected_status: "200",
      expected_body: { "id" => 42, "name" => "widget-42" }
    },
    {
      name: "GET /widgets/:id/parts resolves a nested route two levels deep",
      request: ->(port) { Net::HTTP.get_response("127.0.0.1", "/widgets/42/parts", port) },
      expected_status: "200",
      expected_body: [{ "id" => 1, "widget_id" => 42, "label" => "bracket" }]
    },
    {
      name: "GET /boom is caught by Grape's own error middleware",
      request: ->(port) { Net::HTTP.get_response("127.0.0.1", "/boom", port) },
      expected_status: "500",
      expected_body: { "error" => "deliberate error for the Phase 11 error-middleware scenario" }
    }
  ].freeze
  # rubocop:enable Lint/ConstantDefinitionInBlock

  SCENARIOS.each do |scenario|
    it scenario[:name] do
      with_helix_rack_subprocess(GRAPE_APP_PATH) do |_pid, port, stderr_log_path|
        baseline_size = File.size(stderr_log_path)
        response = scenario[:request].call(port)

        expect(response.code).to eq(scenario[:expected_status])
        expect(response["content-type"]).to eq("application/json")
        expect(JSON.parse(response.body)).to eq(scenario[:expected_body])
        expect(new_fault_log_content(stderr_log_path, baseline_size)).to eq("")
      end
    end
  end

  it "returns Grape's own 404 for an unmatched route, still without a Rack::Lint violation" do
    with_helix_rack_subprocess(GRAPE_APP_PATH) do |_pid, port, stderr_log_path|
      baseline_size = File.size(stderr_log_path)
      response = Net::HTTP.get_response("127.0.0.1", "/nonexistent", port)

      expect(response.code).to eq("404")
      expect(new_fault_log_content(stderr_log_path, baseline_size)).to eq("")
    end
  end

  # Byte-offset delta, not the whole file, and `File.binread`/`byteslice`
  # rather than a plain character-indexed `String#[]` -- matching
  # `spec/support/phase7_server_helper.rb`'s `new_stderr_content`'s own
  # reasoning: a non-ASCII byte written before the baseline would otherwise
  # silently misalign a character-indexed slice.
  def new_fault_log_content(stderr_log_path, baseline_size)
    File.binread(stderr_log_path).byteslice(baseline_size..) || ""
  end
end
# rubocop:enable Metrics/BlockLength
