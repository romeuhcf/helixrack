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
  #
  # `grace_period_seconds` implements Phase 8's graceful shutdown (see
  # `PLAN.md`, Phase 8): PRD.md section 6.2's default (30s, matching
  # Kubernetes' own `terminationGracePeriodSeconds` default -- RNF05 frames
  # this phase around Kubernetes-style termination explicitly) applies
  # unless the caller (`exe/helix_rack`'s `--grace-period` flag) overrides
  # it.
  #
  # Also installs this process's `SIGTERM`/`SIGINT` handling for the
  # duration of this call: see [`install_shutdown_traps`] for what and why,
  # and [`restore_shutdown_traps`] for how any trap a caller had registered
  # before this call is put back once it returns -- `HelixRack.serve` is
  # the authoritative owner of both signals only *while it runs*, the same
  # way Puma or Unicorn own them while serving, not a permanent, unchainable
  # takeover: a safety-review finding on this phase caught that an earlier
  # version left its own trap installed forever (even surviving the
  # `HelixRack.serve` call ending), silently discarding whatever the caller
  # had before -- confirmed concretely, that version made `Ctrl-C` stop
  # working for the rest of the process after a single `HelixRack.serve`
  # call. `install_shutdown_traps` now chains to whatever was registered
  # before it, so an embedder's own `SIGTERM` cleanup (closing a DB pool,
  # say) still runs.
  # rubocop:disable Metrics/ParameterLists -- one keyword argument per
  # PRD.md section 6.2 CLI flag (mirroring `exe/helix_rack`'s `parser.on`
  # clauses one for one), plus the two positional Rack-required arguments
  # (`app`, `port`); this is expected to keep growing by one as later
  # phases add flags, the same way Phase 6 just added `cpu_time_slice_ms`
  # to Phase 4's existing `keep_alive_timeout`/`max_keepalive`.
  def self.serve(
    app, port,
    bind: "0.0.0.0", keep_alive_timeout: 15, max_keepalive: 10_000, cpu_time_slice_ms: 5,
    grace_period_seconds: 30
  )
    # Reset *before* installing this call's own traps -- see
    # `ext/helix_rack/src/lib.rs`'s `reset_shutdown_request` doc comment for
    # the safety-review finding on why that ordering (not resetting from
    # inside `_serve_native`, after the traps were already armed) is the one
    # that actually closes the race, not just moves it.
    _reset_shutdown_request
    previous_traps = install_shutdown_traps
    begin
      _serve_native(
        app, port, bind, keep_alive_timeout, max_keepalive, cpu_time_slice_ms, grace_period_seconds
      )
    ensure
      restore_shutdown_traps(previous_traps)
    end
  end
  # rubocop:enable Metrics/ParameterLists

  # Phase 8 (`PLAN.md`, Phase 8/RNF05): registers `SIGTERM`/`SIGINT` traps
  # that call `_request_shutdown` directly -- see
  # `ext/helix_rack/src/lib.rs`'s `cancelled` doc comment for the full,
  # load-bearing account of why this calls a native function directly
  # rather than relying only on `rb_thread_call_without_gvl`'s own unblock
  # function (the mechanism `Thread#kill` already used, and which an
  # earlier version of this method depended on exclusively): a real signal
  # arriving while a handler is blocked in nested Ruby-level I/O was
  # measured to permanently prevent that unblock function from ever firing
  # for the rest of the process's life, not just delay it. `_request_shutdown`
  # sets a flag `cancelled` polls independently, so this doesn't depend on
  # that unblock function firing at all for the signal case.
  #
  # Captures and returns each signal's *previous* handler (`Signal.trap`'s
  # own return value -- a `Proc`, or a string like `"DEFAULT"`/`"IGNORE"` if
  # nothing custom was registered) so [`restore_shutdown_traps`] can put it
  # back once this `HelixRack.serve` call ends, and so this trap chains to
  # it (if it responds to `#call`) after doing its own work -- an embedder's
  # own signal handling, registered before calling `HelixRack.serve`, still
  # runs; see this constant's -- `serve`'s -- own doc comment for why that
  # matters.
  #
  # `_request_shutdown` itself is documented (in Rust) as trivial and
  # panic-free, safe to call from a trap context. `warn` and the chained
  # call are not given the same guarantee (an embedder's own handler is
  # arbitrary code, and even `warn` can raise from a trap context -- e.g.
  # `$stderr` writing through a `Mutex`, confirmed live on this Ruby version:
  # `ThreadError: can't be called from trap context`) -- each is rescued
  # independently so one raising doesn't stop the other, and neither can
  # turn a real signal into an uncaught exception landing at some arbitrary
  # point on the main thread instead of the clean shutdown this exists for.
  def self.install_shutdown_traps
    %w[TERM INT].each_with_object({}) do |signal, previous|
      previous_handler = Signal.trap(signal) { handle_shutdown_signal(signal, previous_handler) }
      previous[signal] = previous_handler
    end
  end
  private_class_method :install_shutdown_traps

  # The trap body `install_shutdown_traps` registers, split out so that
  # method stays short -- see its own doc comment for why each step here
  # (the native call, the log line, the chain to whatever handler was
  # registered before this one) is independently rescued.
  # rubocop:disable Metrics/MethodLength -- two deliberately separate
  # `begin`/`rescue` blocks (see this method's own doc comment for why one
  # raising must not stop the other from running), not something to merge
  # for a line-count target.
  def self.handle_shutdown_signal(signal, previous_handler)
    _request_shutdown
    begin
      warn "[helix_rack] received SIG#{signal}, shutting down"
    rescue StandardError
      nil
    end
    begin
      previous_handler.call if previous_handler.respond_to?(:call)
    rescue StandardError
      nil
    end
  end
  # rubocop:enable Metrics/MethodLength
  private_class_method :handle_shutdown_signal

  # Restores whatever `Signal.trap` returned as the *previous* handler for
  # each signal in `previous_traps` (from [`install_shutdown_traps`]) --
  # called from `serve`'s `ensure`, so this runs whether `_serve_native`
  # returned normally or raised.
  def self.restore_shutdown_traps(previous_traps)
    previous_traps.each { |signal, handler| Signal.trap(signal, handler) }
  end
  private_class_method :restore_shutdown_traps

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
