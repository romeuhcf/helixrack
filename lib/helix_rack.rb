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
  #
  # `keep_alive_timeout` and `max_keepalive` implement Phase 4's keep-alive
  # lifecycle (see `PLAN.md`, Phase 4): PRD.md section 6.2's defaults (15
  # seconds, 10000 requests) apply unless the caller (`exe/helix_rack`'s
  # `--keep-alive-timeout`/`--max-keepalive` flags) overrides them.
  #
  # `cpu_time_slice_ms` implements Phase 6's preemption signal (see
  # `PLAN.md`, Phase 6): PRD.md section 6.2's default (5ms) applies unless
  # the caller (`exe/helix_rack`'s `--cpu-time-slice` flag) overrides it. See
  # `ext/helix_rack/src/lib.rs`'s `watchdog` module doc comment for what this
  # does and, importantly, does not do -- it is a correctly-firing signal
  # that a handler ran past the slice, counted and readable via
  # `HelixRack.postponed_job_count`, not a guarantee that other connections
  # actually get serviced during that pause.
  # rubocop:disable Metrics/ParameterLists -- one keyword argument per
  # PRD.md section 6.2 CLI flag (mirroring `exe/helix_rack`'s `parser.on`
  # clauses one for one), plus the two positional Rack-required arguments
  # (`app`, `port`); this is expected to keep growing by one as later
  # phases add flags, the same way Phase 6 just added `cpu_time_slice_ms`
  # to Phase 4's existing `keep_alive_timeout`/`max_keepalive`.
  def self.serve(
    app, port,
    bind: "0.0.0.0", keep_alive_timeout: 15, max_keepalive: 10_000, cpu_time_slice_ms: 5
  )
    _serve_native(app, port, bind, keep_alive_timeout, max_keepalive, cpu_time_slice_ms)
  end
  # rubocop:enable Metrics/ParameterLists

  # Phase 6 (`PLAN.md`, Phase 6): how many times the postponed-job
  # preemption signal has actually fired, process-wide, since the extension
  # loaded -- see `ext/helix_rack/src/lib.rs`'s `watchdog` module doc
  # comment. Does not reset between `serve` calls in the same process; a
  # caller that wants to know whether *this* request tripped it should
  # capture a baseline before the request and compare the delta afterward.
  def self.postponed_job_count
    _postponed_job_count
  end
end
