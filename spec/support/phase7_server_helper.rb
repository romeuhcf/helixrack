# frozen_string_literal: true

require "socket"
require "tempfile"
require "timeout"

module Phase7
  # Boots `exe/helix_rack` as a real OS subprocess for the Phase 7 gate (see
  # `PLAN.md`, Phase 7 "Gate"): the gate's "the process PID is unchanged
  # afterward" assertion needs the *server process's* own PID, distinct from
  # the RSpec process running the test -- same reason
  # `spec/support/phase3_server_helper.rb` uses a subprocess rather than a
  # background `Thread` in-process (Phase 2/5/6's style), and the same
  # argument for why that's the right call here: if a fault ever did escape
  # containment and the process aborted, a same-process background-`Thread`
  # harness would take the whole RSpec run down with it, which would still
  # fail the example but by crashing the test runner rather than by a clean,
  # readable assertion failure -- and it couldn't produce a "PID unchanged"
  # signal at all, since there'd be only one PID (RSpec's own) to compare
  # against itself.
  #
  # Deliberately a new file rather than reusing/editing
  # `spec/support/phase3_server_helper.rb`, matching this project's
  # established per-phase-own-helper convention -- even though the
  # boot/wait/reap technique below is otherwise the same as Phase 3's.
  module ServerHelper
    BOOT_TIMEOUT_SECONDS = 5
    REAP_TIMEOUT_SECONDS = 5
    REPO_ROOT = File.expand_path("../..", __dir__)

    # Boots `exe/helix_rack -a app_path -p <free port>`, waits for it to
    # accept connections, yields `(pid, port, stderr_log_path)` to the
    # block, then always reaps the child -- including when the block raises
    # -- via an `ensure`.
    #
    # `stderr_log_path` (a real file, not `File::NULL` -- a safety-review
    # finding on this gate's first version): the child's real stderr
    # (`ext/helix_rack/src/lib.rs`'s `log_fault`) is the only way to tell
    # apart the two different paths that both produce an identical HTTP
    # `500` -- a Ruby-side error (`Handler::call`'s `Ok(Err(_))` arm, logged
    # as `"request failed"`) versus a caught Rust panic (its `Err(_)` arm,
    # logged as `"request handler panicked"`) -- and PLAN.md's Phase 7
    # Resolution note itself records that discarding that evidence let an
    # earlier version of this gate's third fixture row silently land in the
    # wrong arm without failing. Spawned with `HELIX_RACK_DEBUG_PANIC=1` for
    # the same reason: `ext/helix_rack/src/lib.rs`'s debug-panic header
    # check is disabled by default (another safety-review finding, on
    # leaving it unconditionally reachable in a release build) and this
    # gate is the one legitimate caller that needs it armed.
    def with_helix_rack_subprocess(app_path)
      port = free_local_port
      stderr_log = Tempfile.new("helix_rack-phase7-stderr")
      pid = spawn_server(app_path, port, stderr_log.path)

      wait_until_ready!(pid, port)

      yield pid, port, stderr_log.path
    ensure
      reap(pid) if pid
      stderr_log&.close
      stderr_log&.unlink
    end

    # The number of bytes written to `stderr_log_path` so far -- a baseline
    # to capture *before* firing a request, so [`new_stderr_content`] can
    # report only what that one request logged, matching this project's
    # established delta-not-absolute pattern for a value that accumulates
    # across the whole server run (see `spec/integration/
    # phase6_preemption_spec.rb`'s use of `HelixRack.postponed_job_count`
    # for the same reason).
    def stderr_log_size(stderr_log_path)
      File.size(stderr_log_path)
    end

    # Everything written to `stderr_log_path` since `baseline_size` (from
    # [`stderr_log_size`]). Reads the path fresh each call rather than
    # keeping an open `IO` across the child's own writes -- simpler than
    # coordinating buffering/position across two separate processes' file
    # descriptors on the same path, and cheap enough for this gate's three
    # examples.
    #
    # `byteslice`, not `[baseline_size..]` on the plain (character-indexed)
    # `String` `File.read` returns -- a re-verification pass on this gate
    # found that mismatch: `stderr_log_size` is `File.size`, a **byte**
    # count, but `String#[]` indexes by **character**, so any non-ASCII byte
    # written before the baseline (a non-ASCII exception message, a UTF-8
    # source path in a panic location) would silently shift the slice and
    # could truncate or miss the expected fragment -- a spurious gate
    # failure, never a false pass, but still wrong. `File.binread` reads the
    # bytes back as `ASCII-8BIT`, matching `baseline_size`'s own units, so
    # `byteslice` (equivalent to plain `[]` on an `ASCII-8BIT` string, spelled
    # out here for clarity) is correct regardless of encoding.
    def new_stderr_content(stderr_log_path, baseline_size)
      File.binread(stderr_log_path).byteslice(baseline_size..) || ""
    end

    # A non-reaping liveness probe (signal 0 checks existence without
    # delivering a signal) -- public, unlike Phase 3's private counterpart,
    # because the Phase 7 gate's own assertion (b) ("the process PID is
    # unchanged afterward") needs to call this directly from the spec, not
    # just from this helper's own boot-wait loop. Only `reap` (below) may
    # ever *reap* this pid -- see that method's doc comment for why calling
    # `Process.kill` a second time after the child has already exited (and
    # its pid possibly reused by an unrelated process) is the hazard this
    # split avoids.
    def process_alive?(pid)
      Process.kill(0, pid)
      true
    rescue Errno::ESRCH, Errno::ECHILD
      false
    end

    private

    def free_local_port
      server = TCPServer.new("127.0.0.1", 0)
      server.addr[1]
    ensure
      server&.close
    end

    def spawn_server(app_path, port, stderr_log_path)
      Process.spawn(
        { "HELIX_RACK_DEBUG_PANIC" => "1" },
        "bundle", "exec", "exe/helix_rack", "-a", app_path, "-p", port.to_s,
        chdir: REPO_ROOT,
        out: File::NULL,
        err: [stderr_log_path, "w"]
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

    def reap(pid)
      Process.kill("TERM", pid)
    rescue Errno::ESRCH
      nil
    ensure
      wait_for_exit(pid)
    end

    def wait_for_exit(pid)
      Timeout.timeout(REAP_TIMEOUT_SECONDS) { Process.waitpid(pid) }
    rescue Errno::ECHILD
      nil
    rescue Timeout::Error
      force_kill(pid)
    end

    def force_kill(pid)
      Process.kill("KILL", pid)
      Process.waitpid(pid)
    rescue Errno::ESRCH, Errno::ECHILD
      nil
    end
  end
end
