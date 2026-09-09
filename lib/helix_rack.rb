# frozen_string_literal: true

require_relative "helix_rack/version"
require "helix_rack/helix_rack"

# A Rust-native HTTP/1.1 server embedding CRuby (rb-sys/magnus) for Rack/
# Grape apps. See PRD.md and PLAN.md at the repo root.
module HelixRack
  class Error < StandardError; end

  # Phase 2 entry point (see `PLAN.md` at the repo root, Phase 2): start a
  # HelixRack server bound to `bind`:`port`, serving `app` (a Rack app
  # responding to `#call(env)`).
  #
  # This call blocks the calling thread for as long as the server runs, the
  # same way `Rack::Handler::Puma.run` or `Rack::Handler::WEBrick.run`
  # would -- see `_serve_native` (`ext/helix_rack/src/lib.rs`) for why: it
  # runs `engine`'s Tokio `current_thread` runtime via `block_on` on this
  # same OS thread. The GVL is released while idle (see `_serve_native`'s
  # doc comment for why that's necessary even in this phase) and reacquired
  # only for each request's `app.call(env)`.
  def self.serve(app, port, bind: "0.0.0.0")
    _serve_native(app, port, bind)
  end
end
