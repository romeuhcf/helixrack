# frozen_string_literal: true

require_relative "helix_rack/version"
require "helix_rack/helix_rack"

# A Rust-native HTTP/1.1 server embedding CRuby (rb-sys/magnus) for Rack/
# Grape apps. See PRD.md and PLAN.md at the repo root.
module HelixRack
  class Error < StandardError; end

  # Guards [`install_shutdown_traps`]/[`restore_shutdown_traps`] against a
  # real correctness bug a CodeRabbit finding on this PR traced through
  # them: if two `HelixRack.serve` calls ever overlap in one process (an
  # unusual but not forbidden usage pattern -- e.g. two servers on different
  # ports, each on its own thread), each installs traps and, without this
  # guard, would unconditionally restore whatever it saw as "previous" once
  # its own `_serve_native` call returns. If the *first* call to finish
  # happened to have started *before* the second (so its "previous" is the
  # pre-HelixRack handler, not the second call's), that's fine -- but if the
  # first call to *finish* is actually the *second* one to have *started*
  # (started after the first, so it captured the first's still-active trap
  # as its own "previous"), restoring that "previous" would silently
  # overwrite the *first* call's still-running trap with its own,
  # disconnecting the still-running first server from `SIGTERM`/`SIGINT` for
  # the rest of its life, with nothing visibly wrong until a signal that
  # should have reached it doesn't.
  #
  # An early version of this fix rejected any overlapping `HelixRack.serve`
  # call outright -- simpler, but it surfaced an unrelated, pre-existing
  # race in this project's own same-process test harnesses
  # (`spec/support/phase{2,5,6}_server_helper.rb`'s `ensure { server_thread&.kill }`,
  # which sends `Thread#kill` but never `Thread#join`s it, so the *next*
  # example's own `HelixRack.serve` call can legitimately start before the
  # previous example's killed thread has finished unwinding and running its
  # own `ensure` blocks) -- confirmed by that fix making otherwise-unrelated
  # Phase 2/6 examples flake with "already running", not a real
  # double-`serve` bug. A generation counter instead: each
  # `install_shutdown_traps` call gets the next generation number, and
  # `restore_shutdown_traps` only actually restores if its generation is
  # still the *current* one -- i.e. if no *later* `install_shutdown_traps`
  # call has happened since. If a later call has taken over, this one's
  # "previous" is stale by definition (restoring it would be exactly the
  # clobbering bug above) and is safely skipped -- the newer call's own trap
  # (or whatever it itself later restores) is left in place instead. This
  # fixes the same bug without rejecting anything or needing every call site
  # of `HelixRack.serve` to be mutually exclusive.
  @trap_generation_mutex = Mutex.new
  @trap_generation = 0

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
  #
  # See `@trap_generation_mutex`'s own doc comment (top of this file) for
  # how overlapping `HelixRack.serve` calls in one process are handled
  # safely without being rejected.
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
    # `ext/helix_rack/src/lib.rs`'s `reset_shutdown_request` doc comment
    # for the safety-review finding on why that ordering (not resetting
    # from inside `_serve_native`, after the traps were already armed) is
    # the one that actually closes the race, not just moves it.
    _reset_shutdown_request
    generation, previous_traps = install_shutdown_traps
    begin
      _serve_native(
        app, port, bind, keep_alive_timeout, max_keepalive, cpu_time_slice_ms, grace_period_seconds
      )
    ensure
      restore_shutdown_traps(generation, previous_traps)
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
  # Captures and returns `[generation, previous_traps]` -- `generation`
  # from `@trap_generation_mutex` (see its own doc comment for what it's
  # for), and `previous_traps` each signal's *previous* handler
  # (`Signal.trap`'s own return value -- a `Proc`, or a string like
  # `"DEFAULT"`/`"IGNORE"` if nothing custom was registered) so
  # [`restore_shutdown_traps`] can put it back once this `HelixRack.serve`
  # call ends, and so this trap chains to it (if it responds to `#call`)
  # after doing its own work -- an embedder's own signal handling,
  # registered before calling `HelixRack.serve`, still runs; see this
  # constant's -- `serve`'s -- own doc comment for why that matters.
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
    @trap_generation_mutex.synchronize do
      generation = @trap_generation += 1
      previous_traps = %w[TERM INT].each_with_object({}) do |signal, previous|
        previous_handler = Signal.trap(signal) do |signo|
          handle_shutdown_signal(signal, signo, previous_handler)
        end
        previous[signal] = previous_handler
      end
      [generation, previous_traps]
    end
  end
  private_class_method :install_shutdown_traps

  # The trap body `install_shutdown_traps` registers, split out so that
  # method stays short -- see its own doc comment for why each step here
  # (the native call, the log line, the chain to whatever handler was
  # registered before this one) is independently rescued.
  #
  # `signo` (the signal number `Signal.trap` passes its block, confirmed
  # live on this Ruby version, not assumed) is forwarded to
  # `previous_handler.call` -- a CodeRabbit finding on this PR caught that
  # calling it with no arguments works for a lenient `Proc` (registered via
  # `Signal.trap(sig) { ... }`, the common case) but raises `ArgumentError`
  # for a strict callable (a lambda, or a `Method` object) expecting the
  # same argument `Signal.trap` itself would have passed it directly -- and
  # the rescue below would have silently swallowed that `ArgumentError`
  # rather than actually running the chained handler's real behavior.
  # rubocop:disable Metrics/MethodLength -- two deliberately separate
  # `begin`/`rescue` blocks (see this method's own doc comment for why one
  # raising must not stop the other from running), not something to merge
  # for a line-count target.
  def self.handle_shutdown_signal(signal, signo, previous_handler)
    _request_shutdown
    begin
      warn "[helix_rack] received SIG#{signal}, shutting down"
    rescue StandardError
      nil
    end
    begin
      previous_handler.call(signo) if previous_handler.respond_to?(:call)
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
  #
  # Only if `generation` is still the *current* one -- see
  # `@trap_generation_mutex`'s own doc comment (top of this file) for why a
  # stale generation must skip restoring rather than blindly doing it: a
  # later, still-running `HelixRack.serve` call's own trap would otherwise
  # get silently overwritten by this now-outdated "previous" state.
  def self.restore_shutdown_traps(generation, previous_traps)
    @trap_generation_mutex.synchronize do
      return unless generation == @trap_generation

      previous_traps.each { |signal, handler| Signal.trap(signal, handler) }
    end
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
