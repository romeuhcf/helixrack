# frozen_string_literal: true

require "socket"
require_relative "phase2_test_client"

module Phase2
  # Boots a real `HelixRack.serve` instance for the Phase 2 gate (see
  # PLAN.md, Phase 2 "Gate") and hands the example a TCP client for it.
  #
  # `HelixRack.serve` (see `lib/helix_rack.rb`) is a blocking call, so it
  # has to run on a background thread here; this waits for it to actually
  # accept connections before yielding, and re-raises whatever exception
  # the server thread raised instead of letting the example see a
  # confusing "connection refused" from the client side.
  #
  # As of this branch, `HelixRack.serve` is not implemented -- it exists
  # only to give this gate a real entry point to call (see PLAN.md's Phase
  # 2 section and `lib/helix_rack.rb`'s doc comment) and always raises
  # `NotImplementedError`. Every example using this helper is expected to
  # fail with that error until the real Phase 2 wiring lands.
  module ServerHelper
    BOOT_TIMEOUT_SECONDS = 2

    def with_helix_rack_server(app)
      port = free_local_port
      boot_error = nil
      server_thread = boot_server_thread(app, port) { |error| boot_error = error }

      wait_until_ready!(port, server_thread) { boot_error }

      yield Phase2::TestClient.new(port: port)
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

    def boot_server_thread(app, port)
      Thread.new do
        Thread.current.report_on_exception = false
        HelixRack.serve(app, port)
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
