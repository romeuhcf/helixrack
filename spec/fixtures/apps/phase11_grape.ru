# frozen_string_literal: true

require "rack/lint"
require_relative "phase11_grape_app"

# `Rack::Lint` wraps every request/response this app handles for the whole
# life of the gate's server process (see `PLAN.md`, Phase 11 "Gate", and
# `spec/integration/phase11_rack_grape_spec.rb`'s own top comment): it
# raises `Rack::Lint::LintError` the moment either the env HelixRack built
# or the `[status, headers, body]` triple the app returned violates the
# Rack SPEC. That exception is indistinguishable, at this layer, from any
# other Ruby exception a Rack app might raise -- it takes the exact same
# path through `RackAppHandler::handle` (`ext/helix_rack/src/lib.rs`) that
# Phase 7's fixture app's `StandardError` row does, so this is real,
# black-box verification of HelixRack's own Rack-protocol conformance, not
# a check reimplemented from scratch.
use Rack::Lint
run Fixtures::Phase11GrapeApp
