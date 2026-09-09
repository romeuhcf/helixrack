# frozen_string_literal: true

require "json"
require "rack/lint"
require_relative "../support/phase2_server_helper"
require_relative "../fixtures/apps/echo_app"

# Phase 2 gate (see PLAN.md, Phase 2 "Gate"): Rack::Lint compliance, plus an
# echo/JSON table test that sends crafted requests as real TCP connections
# to a really-booted HelixRack server and asserts the Rack env it built,
# per PLAN.md's exact gate description.
#
# `HelixRack.serve` (see `lib/helix_rack.rb`) is not implemented yet on this
# branch -- it exists only to give this gate a real entry point to call,
# and always raises `NotImplementedError` (see that file's doc comment for
# why, and what's missing). Every example below is expected to fail with
# that error, via `Phase2::ServerHelper#with_helix_rack_server`, until the
# real Phase 2 wiring (engine's `Handler` trait <-> magnus <-> a loaded
# Rack app) lands. That is the point of this gate: it exists now, and it
# fails for the right reason.
# rubocop:disable Metrics/BlockLength -- a data-driven RSpec table test is
# long by nature; splitting it across files would make the table harder to
# read as one gate, not easier.
RSpec.describe "Phase 2: Rack env + CRuby invocation gate" do
  include Phase2::ServerHelper

  # `pending`, not a plain failure: this suite runs as part of `rake`'s
  # default task, which CI treats as a required check (see this repo's
  # CLAUDE.md and branch protection). A gate that's *supposed* to be red
  # right now must not fail the build -- `pending` reports these examples
  # as yellow while HelixRack.serve raises NotImplementedError, and RSpec
  # itself will fail the suite (a "pending examples fixed" error) the
  # moment any of them starts passing without this marker being removed --
  # exactly the nudge to delete it once Phase 2's real wiring lands.
  # rubocop:disable Lint/ConstantDefinitionInBlock -- deliberately a real
  # constant so it can be used as `it` metadata (`pending: PENDING_REASON`),
  # which needs a value at spec-definition time, not inside an example.
  PENDING_REASON = "Phase 2 not implemented yet -- see PLAN.md and lib/helix_rack.rb"
  # rubocop:enable Lint/ConstantDefinitionInBlock

  describe "Rack::Lint compliance" do
    it "raises no Rack::Lint violation for a basic GET /", pending: PENDING_REASON do
      with_helix_rack_server(Rack::Lint.new(Fixtures::EchoApp.new)) do |client|
        response = client.request(method: "GET", path: "/")

        expect(response.status).to eq(200)
      end
    end
  end

  describe "echoed Rack env as JSON" do
    # method/path/query/headers/body vary per row, per PLAN.md's gate
    # description ("varying verb, path, query string, headers, and at
    # least one request with a body"). PLAN.md's assertion is scoped to
    # the 9 mandated keys, so headers aren't independently asserted on --
    # they're varied here to exercise header parsing along the way.
    # rubocop:disable Lint/ConstantDefinitionInBlock -- deliberately a real
    # constant, defined here (not a `let`) because it drives `.each` at
    # spec-load time to generate one example per row, per PLAN.md's "run a
    # table of crafted requests" gate description.
    TABLE = [
      {
        label: "GET root, empty query, no body",
        method: "GET", path: "/", query: "", headers: {}, body: nil
      },
      {
        label: "GET nested path with a query string",
        method: "GET", path: "/foo/bar", query: "a=1&b=2", headers: {}, body: nil
      },
      {
        label: "GET with a trailing-slash path",
        method: "GET", path: "/foo/", query: "", headers: { "X-Custom" => "a" }, body: nil
      },
      {
        label: "POST with a form body",
        method: "POST", path: "/submit", query: "",
        headers: { "Content-Type" => "application/x-www-form-urlencoded" },
        body: "hello=world"
      },
      {
        label: "PUT with a JSON body and a custom header",
        method: "PUT", path: "/items/42", query: "",
        headers: { "X-Trace-Id" => "abc-123" },
        body: '{"name":"widget"}'
      },
      {
        label: "DELETE with no body",
        method: "DELETE", path: "/items/42", query: "", headers: {}, body: nil
      }
    ].freeze
    # rubocop:enable Lint/ConstantDefinitionInBlock

    # rubocop:disable Metrics/MethodLength -- a flat, one-key-per-line hash
    # literal reads better than a denser one here.
    def mandated_env_json(port:, method:, path:, query:, body:)
      {
        "REQUEST_METHOD" => method,
        "PATH_INFO" => path,
        "QUERY_STRING" => query,
        "SERVER_NAME" => "127.0.0.1",
        "SERVER_PORT" => port.to_s,
        # Rack dropped `rack.version` as a required key in Rack 3 (verified
        # against the installed rack-3.2.7's `Rack::Lint`, which no longer
        # asserts it); PLAN.md's Phase 2 gate still mandates it, so this is
        # an assumption, not a verified value: the historical Rack 1.x/2.x
        # convention of a `[major, minor]` SPEC-version array. Flagged in
        # this branch's report as an ambiguity for whoever wires up the
        # real `rack.version` value.
        "rack.version" => [1, 3],
        "rack.input" => body.to_s,
        "rack.errors" => true,
        "rack.url_scheme" => "http"
      }
    end
    # rubocop:enable Metrics/MethodLength

    TABLE.each do |row|
      it "reports the mandated env keys for: #{row[:label]}", pending: PENDING_REASON do
        with_helix_rack_server(Fixtures::EchoApp.new) do |client|
          response = client.request(
            method: row[:method],
            path: row[:path],
            query: row[:query],
            headers: row[:headers],
            body: row[:body]
          )

          json = JSON.parse(response.body)
          expected = mandated_env_json(
            port: client.port,
            method: row[:method],
            path: row[:path],
            query: row[:query],
            body: row[:body]
          )

          expect(json).to eq(expected)
        end
      end
    end
  end
end
# rubocop:enable Metrics/BlockLength
