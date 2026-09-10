# frozen_string_literal: true

require "socket"
require "timeout"

module Phase8
  # Boots `exe/helix_rack` as a real OS subprocess for the Phase 8 gate (see
  # `PLAN.md`, Phase 8 "Gate"): both assertion (3) ("the process exits with
  # code 0 within `grace_period + epsilon`") and the general shape of this
  # gate (send a real `SIGTERM` and observe a real process's real exit)
  # need the *server's own* OS process, not a same-process background
  # `Thread` -- same reasoning as `spec/support/phase3_server_helper.rb` and
  # `spec/support/phase7_server_helper.rb`, and a new file for the same
  # "each phase gets its own helper" reason those give.
  #
  # Unlike those two, this one does **not** `reap`/force-kill the subprocess
  # in its `ensure` block on the happy path: the whole point of this gate is
  # to prove `exe/helix_rack` exits *on its own*, cleanly, in response to a
  # signal this module sends deliberately mid-example -- reaping
  # unconditionally afterward would mask a bug where the process actually
  # failed to exit within its grace period. `ensure` still guards against a
  # test failure leaving an orphaned process behind (an unexpected exception
  # before the example reaches its own `Process.kill`/`wait` calls), but via
  # a liveness check first, not an unconditional signal.
  module ServerHelper
    BOOT_TIMEOUT_SECONDS = 5
    REAP_TIMEOUT_SECONDS = 5
    # `spec/integration/phase8_graceful_shutdown_spec.rb` only calls
    # `wait_until_new_connections_are_refused!` *after* releasing the
    # in-flight request's barrier (see that file's own top comment for why
    # that ordering, not PLAN.md's original literal one, is what this
    # single-OS-thread architecture can actually guarantee) -- so by the
    # time this polls, the accept loop should regain the OS thread and
    # notice the already-pending shutdown signal promptly (idle-case
    # measurement: consistently well under a second). Still meaningfully
    # more than that, not tight, for real scheduling jitter on a loaded CI
    # box -- see `ext/helix_rack/src/lib.rs`'s `cancelled` doc comment for
    # the full account of what does and doesn't bound this.
    NEW_CONNECTION_REFUSED_TIMEOUT_SECONDS = 10
    # How long to wait between probe connection attempts in
    # `wait_until_new_connections_are_refused!`. Deliberately far coarser
    # than `wait_until_ready!`'s 10ms boot-wait cadence below -- a real bug
    # found while developing this gate: probing every 10ms here (hundreds of
    # attempts over `NEW_CONNECTION_REFUSED_TIMEOUT_SECONDS`) built up a real
    # kernel TCP SYN backlog against a listener that hadn't stopped accepting
    # yet, to the point that a *single* probe's `TCPSocket.new` eventually
    # raised `IO::TimeoutError` instead of the expected prompt
    # `ECONNREFUSED` -- and, worse, competed with the server's own single OS
    # thread for the exact CPU time it needed to notice the shutdown signal
    # in the first place. A slower cadence here is not just less noisy, it
    # avoids the test itself perturbing the very timing it's trying to
    # observe.
    NEW_CONNECTION_PROBE_INTERVAL_SECONDS = 0.25
    REPO_ROOT = File.expand_path("../..", __dir__)

    # Boots `exe/helix_rack -a app_path -p <free port> --grace-period
    # <grace_period_seconds>`, with `HELIX_RACK_GATE_BARRIER_PORT` set to
    # `barrier_port` (see `spec/fixtures/apps/phase8_shutdown_app.rb`'s doc
    # comment for what that's for), waits for it to accept connections,
    # yields `(pid, port)` to the block, then reaps it only if it's still
    # alive when the block returns or raises -- see this module's own doc
    # comment for why that's conditional here, unlike Phase 3/7's
    # unconditional `ensure`.
    def with_helix_rack_subprocess(app_path, barrier_port:, grace_period_seconds:)
      port = free_local_port
      pid = spawn_server(app_path, port, barrier_port, grace_period_seconds)

      wait_until_ready!(pid, port)

      yield pid, port
    ensure
      reap_if_still_alive(pid) if pid
    end

    # A non-reaping liveness probe -- public for the same reason
    # `spec/support/phase7_server_helper.rb`'s is: the gate's own
    # assertions need to call it directly, not just this helper's internal
    # boot-wait loop.
    def process_alive?(pid)
      Process.kill(0, pid)
      true
    rescue Errno::ESRCH, Errno::ECHILD
      false
    end

    # Polls (bounded, small sleeps -- the same style
    # `wait_until_ready!`/`fail_on_boot_timeout!` below already use for "wait
    # for a state transition" checks, not a fixed-sleep timing assertion)
    # until a fresh TCP connection attempt to `port` is refused, or raises
    # if that never happens within `NEW_CONNECTION_REFUSED_TIMEOUT_SECONDS`.
    # This is the gate's assertion (1): once shutdown has started, a *new*
    # connection is refused immediately -- "immediately" bounded the same
    # way `spec/integration/phase4_keep_alive_gate...`-style timeout
    # assertions elsewhere in this project are, not asserted as an exact
    # zero-latency claim (the underlying mechanism -- a pending signal
    # waking a thread blocked in `rb_thread_call_without_gvl` -- is itself
    # bounded by `_serve_native`'s `cancelled` poll interval, ~20ms; see
    # that function's doc comment).
    def wait_until_new_connections_are_refused!(port)
      deadline = Time.now + NEW_CONNECTION_REFUSED_TIMEOUT_SECONDS

      loop do
        return if connection_refused?(port)

        if Time.now > deadline
          raise "port #{port} was still accepting new connections #{NEW_CONNECTION_REFUSED_TIMEOUT_SECONDS}s " \
                "after SIGTERM -- shutdown did not stop the accept loop promptly"
        end

        sleep NEW_CONNECTION_PROBE_INTERVAL_SECONDS
      end
    end

    private

    # A bounded connect attempt (`connect_timeout:`), not a bare
    # `TCPSocket.new` -- a single hung attempt must not be able to eat the
    # whole `NEW_CONNECTION_REFUSED_TIMEOUT_SECONDS` budget silently between
    # this loop's own deadline checks. `IO::TimeoutError` (what a hung
    # attempt raises, verified against this Ruby version's real behavior,
    # not assumed) is treated the same as "not refused yet, keep trying" --
    # it proves nothing about whether the port is still listening, so the
    # outer loop's own deadline is what actually bounds this, not a guess
    # about what a timeout here means.
    def connection_refused?(port)
      Socket.tcp("127.0.0.1", port, connect_timeout: NEW_CONNECTION_PROBE_INTERVAL_SECONDS, &:close)
      false
    rescue Errno::ECONNREFUSED, Errno::EADDRNOTAVAIL
      true
    rescue IO::TimeoutError
      false
    end

    def free_local_port
      server = TCPServer.new("127.0.0.1", 0)
      server.addr[1]
    ensure
      server&.close
    end

    def spawn_server(app_path, port, barrier_port, grace_period_seconds)
      Process.spawn(
        { "HELIX_RACK_GATE_BARRIER_PORT" => barrier_port.to_s },
        "bundle", "exec", "exe/helix_rack", "-a", app_path, "-p", port.to_s,
        "--grace-period", grace_period_seconds.to_s,
        chdir: REPO_ROOT,
        out: File::NULL,
        err: File::NULL
      )
    end

    def wait_until_ready!(pid, port)
      deadline = Time.now + BOOT_TIMEOUT_SECONDS

      loop do
        raise "helix_rack subprocess (pid #{pid}) exited before accepting a connection" unless process_alive?(pid)
        return if port_accepting_connections?(port)

        fail_on_boot_timeout!(pid, port, deadline)
        sleep 0.01
      end
    end

    def port_accepting_connections?(port)
      TCPSocket.new("127.0.0.1", port).close
      true
    rescue Errno::ECONNREFUSED, Errno::EADDRNOTAVAIL
      false
    end

    def fail_on_boot_timeout!(pid, port, deadline)
      return unless Time.now > deadline

      raise "helix_rack subprocess (pid #{pid}) never started listening on port #{port} " \
            "within #{BOOT_TIMEOUT_SECONDS}s"
    end

    # Only signals/reaps if the process is still around -- the happy path
    # for this gate already waited for the subprocess to exit on its own
    # (`Process.waitpid2`, in the example itself) before this `ensure` ever
    # runs, so there's normally nothing left to do here at all.
    def reap_if_still_alive(pid)
      return unless process_alive?(pid)

      Process.kill("KILL", pid)
      # Timeout-bounded, matching `spec/support/phase7_server_helper.rb`'s
      # `wait_for_exit` -- a safety-review finding on this file's first
      # version noted the bare `Process.waitpid` here had no such bound,
      # unlike Phase 7's. `KILL` should make this effectively instant in
      # practice; the only realistic way it wouldn't is a D-state (blocked
      # on uninterruptible I/O) process, which this can't fix but shouldn't
      # hang the whole suite over either.
      Timeout.timeout(REAP_TIMEOUT_SECONDS) { Process.waitpid(pid) }
    rescue Errno::ESRCH, Errno::ECHILD
      nil
    rescue Timeout::Error
      raise "helix_rack subprocess (pid #{pid}) did not exit within #{REAP_TIMEOUT_SECONDS}s of SIGKILL"
    end
  end
end
