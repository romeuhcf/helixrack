# frozen_string_literal: true

require "socket"
require "timeout"

module Phase3
  # Boots `exe/helix_rack` as a real OS subprocess for the Phase 3 gate (see
  # PLAN.md, Phase 3 "Gate"): the memory-bound assertion needs the *server
  # process's* own PID, distinct from the RSpec process running the test --
  # Phase 2's `spec/support/phase2_server_helper.rb` runs the server on a
  # background `Thread` inside the test process instead, sharing one PID
  # with RSpec, which would make an RSS assertion meaningless. Deliberately
  # separate support file rather than reusing/editing Phase 2's helper.
  #
  # `Process.spawn` is called with the command split into separate string
  # arguments (`"bundle", "exec", "exe/helix_rack", ...`), not one shell
  # command line -- Ruby execs that directly (no intermediate `/bin/sh -c`
  # wrapper) per `Process.spawn`'s own documented rule: a single string
  # argument is subject to shell expansion, more than one argument is not.
  # Separately, `bundle exec` itself was verified on this machine (Bundler
  # 4.0.16, Ruby 4.0.6, checked 2026-09-09) to `exec` into the target Ruby
  # process rather than fork+exec it: a script that ran
  # `Process.spawn("bundle", "exec", "ruby", "-e", "STDOUT.puts Process.pid; ...")`
  # printed the exact same PID `Process.spawn` itself returned. Together,
  # that means `spawn_server`'s returned pid is genuinely `exe/helix_rack`'s
  # own OS process, not a Bundler wrapper's.
  module ServerHelper
    BOOT_TIMEOUT_SECONDS = 5
    REAP_TIMEOUT_SECONDS = 5
    REPO_ROOT = File.expand_path("../..", __dir__)

    # Boots `exe/helix_rack -a app_path -p <free port>`, waits for it to
    # accept connections, yields `(pid, port)` to the block, then always
    # reaps the child -- including when the block raises (a failing
    # assertion, most likely, given this gate) -- via an `ensure`, not just
    # on the happy path.
    def with_helix_rack_subprocess(app_path)
      port = free_local_port
      pid = spawn_server(app_path, port)

      wait_until_ready!(pid, port)

      yield pid, port
    ensure
      reap(pid) if pid
    end

    private

    def free_local_port
      server = TCPServer.new("127.0.0.1", 0)
      server.addr[1]
    ensure
      server&.close
    end

    def spawn_server(app_path, port)
      Process.spawn(
        "bundle", "exec", "exe/helix_rack", "-a", app_path, "-p", port.to_s,
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

    # A non-reaping liveness probe (signal 0 checks existence without
    # delivering a signal) -- Process.waitpid(pid, WNOHANG) would work too,
    # but it *reaps* an already-exited child as a side effect of checking,
    # and this method is called from a retry loop (wait_until_ready!) where
    # that reap could race with -- and duplicate -- the one ensure/reap
    # below owns. Only that one call site may reap this pid; see reap's doc
    # comment for why sending a signal to an already-reaped, possibly
    # PID-reused process is the actual hazard being avoided here.
    def process_alive?(pid)
      Process.kill(0, pid)
      true
    rescue Errno::ESRCH, Errno::ECHILD
      false
    end

    def port_accepting_connections?(port)
      TCPSocket.new("127.0.0.1", port).close
      true
    rescue Errno::ECONNREFUSED, Errno::EADDRNOTAVAIL
      false
    end

    def fail_on_boot_timeout!(pid, port, deadline)
      return unless Time.now > deadline

      # No reap(pid) here -- with_helix_rack_subprocess's ensure is the sole
      # place this pid gets reaped (see process_alive?'s doc comment).
      raise "helix_rack subprocess (pid #{pid}) never started listening on port #{port} " \
            "within #{BOOT_TIMEOUT_SECONDS}s"
    end

    # Kills the child and waits for it to actually exit, escalating to
    # SIGKILL if it doesn't within `REAP_TIMEOUT_SECONDS`. Called from
    # exactly one place -- with_helix_rack_subprocess's `ensure` -- and must
    # stay that way: signaling a pid more than once, after it may have
    # already exited, risks the OS having reused that pid for an unrelated
    # process by the time a second call runs. Runs from an `ensure`, so this
    # must not itself hang the suite on a child that refuses to die (no
    # real signal-handling exists on this branch yet; Phase 8 is what
    # teaches the server to trap SIGTERM, so today's default disposition --
    # terminate -- is expected to apply immediately, but the escalation
    # path is here in case that ever changes).
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
