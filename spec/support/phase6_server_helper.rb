# frozen_string_literal: true

require "socket"

module Phase6
  # Boots a real `HelixRack.serve` instance on a background Ruby `Thread`,
  # in the *same process* as the example itself -- not a subprocess (contrast
  # `spec/support/phase3_server_helper.rb`) -- because Phase 6's gate needs
  # to read `HelixRack.postponed_job_count` (see `ext/helix_rack/src/lib.rs`'s
  # `watchdog` module) from the very same Ruby VM the server ran the fixture
  # handler in; a subprocess wouldn't share that counter.
  #
  # Deliberately a new file rather than reusing or editing
  # `spec/support/phase2_server_helper.rb` or `phase5_server_helper.rb`,
  # matching Phase 3's and Phase 5's precedent of not reusing another
  # phase's helper -- even though the boot/wait/cleanup technique below is
  # otherwise the same as theirs.
  #
  # The one thing this helper adds over Phase 5's near-identical version:
  # `cpu_time_slice_ms`, threaded straight through to `HelixRack.serve` so
  # the gate can configure Phase 6's `--cpu-time-slice` explicitly rather
  # than relying on `lib/helix_rack.rb`'s default staying in sync with what
  # the gate assumes.
  module ServerHelper
    BOOT_TIMEOUT_SECONDS = 2

    def with_helix_rack_server(app, cpu_time_slice_ms:)
      port = free_local_port
      boot_error = nil
      server_thread = boot_server_thread(app, port, cpu_time_slice_ms) { |error| boot_error = error }

      wait_until_ready!(port, server_thread) { boot_error }

      yield port
    ensure
      server_thread&.kill
    end

    private

    def free_local_port
      server = TCPServer.new("127.0.0.1", 0)
      server.addr[1]
    ensure
      server&.close
    end

    def boot_server_thread(app, port, cpu_time_slice_ms)
      Thread.new do
        Thread.current.report_on_exception = false
        HelixRack.serve(app, port, cpu_time_slice_ms: cpu_time_slice_ms)
      rescue Exception => e # rubocop:disable Lint/RescueException
        # Re-raised on the caller's thread in `wait_until_ready!` -- never
        # swallowed.
        yield e
      end
    end

    def wait_until_ready!(port, server_thread)
      deadline = Time.now + BOOT_TIMEOUT_SECONDS

      loop do
        boot_error = yield
        raise boot_error if boot_error
        raise "server thread exited without accepting a connection or raising" unless server_thread.alive?
        return if port_accepting_connections?(port)

        fail_on_boot_timeout!(port, deadline)
        sleep 0.01
      end
    end

    def port_accepting_connections?(port)
      TCPSocket.new("127.0.0.1", port).close
      true
    rescue Errno::ECONNREFUSED, Errno::EADDRNOTAVAIL
      false
    end

    def fail_on_boot_timeout!(port, deadline)
      return unless Time.now > deadline

      raise "HelixRack.serve never started listening on port #{port} within #{BOOT_TIMEOUT_SECONDS}s"
    end
  end
end
