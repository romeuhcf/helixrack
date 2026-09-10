# frozen_string_literal: true

require "socket"
require "tempfile"
require "timeout"

module Phase11
  # Boots `exe/helix_rack` as a real OS subprocess for the Phase 11 gate
  # (see `PLAN.md`, Phase 11 "Gate"): a real server process is what makes
  # this gate black-box, end-to-end verification of HelixRack's own
  # Rack-protocol conformance (via `Rack::Lint`, wrapped around the fixture
  # app in `spec/fixtures/apps/phase11_grape.ru`) rather than a check of the
  # Grape app in isolation.
  #
  # Deliberately a new file rather than reusing/editing
  # `spec/support/phase7_server_helper.rb`, matching this project's
  # established per-phase-own-helper convention -- the boot/wait/reap
  # technique is the same as Phase 7's, minus the `HELIX_RACK_DEBUG_PANIC`
  # env var and the stderr-log plumbing, neither of which this gate needs:
  # Phase 11's assertions are the HTTP responses themselves plus whether the
  # server's stderr fault log grew at all (see the gate spec's own
  # `fault_log_size`/`new_fault_log_content`, analogous to Phase 7's
  # `stderr_log_size`/`new_stderr_content` but kept in the gate spec itself
  # rather than this helper, since only that one gate needs it).
  module ServerHelper
    BOOT_TIMEOUT_SECONDS = 5
    REAP_TIMEOUT_SECONDS = 5
    REPO_ROOT = File.expand_path("../..", __dir__)

    # Boots `exe/helix_rack -a app_path -p <free port>`, waits for it to
    # accept connections, yields `(pid, port, stderr_log_path)` to the
    # block, then always reaps the child -- including when the block raises
    # -- via an `ensure`.
    def with_helix_rack_subprocess(app_path)
      port = free_local_port
      stderr_log = Tempfile.new("helix_rack-phase11-stderr")
      pid = spawn_server(app_path, port, stderr_log.path)

      wait_until_ready!(pid, port)

      yield pid, port, stderr_log.path
    ensure
      reap(pid) if pid
      stderr_log&.close
      stderr_log&.unlink
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
