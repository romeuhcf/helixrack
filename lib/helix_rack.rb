# frozen_string_literal: true

require_relative "helix_rack/version"
require "helix_rack/helix_rack"

# A Rust-native HTTP/1.1 server embedding CRuby (rb-sys/magnus) for Rack/
# Grape apps. See PRD.md and PLAN.md at the repo root.
module HelixRack
  class Error < StandardError; end

  # Phase 2 entry point (see `PLAN.md` at the repo root, Phase 2): start a
  # HelixRack server bound to `port`, serving `app` (a Rack app responding
  # to `#call(env)`).
  #
  # This call blocks the calling thread for as long as the server runs, the
  # same way `Rack::Handler::Puma.run` or `Rack::Handler::WEBrick.run`
  # would.
  #
  # Not implemented yet: Phase 2's real work (building the Rack `env` from a
  # parsed request via `engine`'s `Handler` trait, invoking `app.call(env)`
  # through magnus, translating `[status, headers, body]` back to bytes) has
  # not been wired up. `engine/` already has the pluggable `Handler` trait
  # and a real HTTP/1.1 engine behind it (see `engine/src/handler.rs`); what
  # is missing is a magnus-backed `Handler` implementation that calls into
  # this `app` and a way to hand it to `engine::serve`. This method exists
  # now so the Phase 2 RSpec gate (`spec/`) has a real entry point to call
  # and can fail for that reason -- a `NotImplementedError` -- rather than a
  # load error or a missing method.
  def self.serve(app, port)
    raise NotImplementedError,
          "Phase 2: HelixRack.serve(app, #{port.inspect}) is not implemented yet -- " \
          "engine/'s Handler trait and HTTP/1.1 engine exist, but nothing here builds " \
          "a Rack env from a request or calls `#{app.class}#call` through it. See " \
          "PLAN.md's Phase 2 section."
  end
end
