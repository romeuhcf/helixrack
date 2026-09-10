# frozen_string_literal: true

require "socket"

module Phase5
  # Boots a real `HelixRack.serve` instance on a background Ruby `Thread`,
  # in the *same process* as the example itself -- not a subprocess (contrast
  # `spec/support/phase3_server_helper.rb`) -- because Phase 5's gate (see
  # `PLAN.md`, Phase 5 "What this phase's gate should actually prove
  # instead") needs a genuinely separate, same-process Ruby `Thread` to
  # observe GVL access while one request's `Handler::call` is itself
  # blocked; that only works if the server and the observing thread share
  # one Ruby VM, which a subprocess wouldn't give us.
  #
  # Deliberately a new file rather than reusing or editing
  # `spec/support/phase2_server_helper.rb`, matching Phase 3's precedent of
  # not reusing another phase's helper -- even though the boot/wait/cleanup
  # technique below is otherwise the same as Phase 2's.
  #
  # Yields the bound `port` (not a client object): Phase 5's gate needs to
  # fire the one request from its own background `Thread` and keep the main
  # example thread free to drive the rest of the assertion, so building the
  # request is left to the example itself rather than hidden behind a
  # shared client class.
  module ServerHelper
    BOOT_TIMEOUT_SECONDS = 2

    def with_helix_rack_server(app)
      port = free_local_port
      boot_error = nil
      server_thread = boot_server_thread(app, port) { |error| boot_error = error }

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
