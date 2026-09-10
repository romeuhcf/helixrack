# frozen_string_literal: true

require "socket"

module Fixtures
  # Fixture Rack app for the Phase 8 gate (see `PLAN.md`, Phase 8 "Gate"): a
  # single route that blocks on a genuinely GVL-yielding I/O primitive --
  # a `TCPSocket` connected back to the *test process's* own barrier
  # `TCPServer` -- until that process writes to (and closes) the other end.
  #
  # A `TCPSocket`/`IO` blocking read releasing the GVL is not assumed here;
  # it's the same class of primitive Phase 5's gate already verified
  # (`spec/fixtures/apps/phase5_blocking_app.rb`, an `IO.pipe` read end) --
  # the mechanism (a blocking read syscall) is the same, only the concrete
  # `IO` subclass differs, needed here because Phase 8's gate runs the
  # server as a real OS *subprocess* (so its own PID and exit code/timing
  # are observable, see `spec/support/phase8_server_helper.rb`'s doc
  # comment), not a same-process background `Thread` like Phase 5's gate --
  # an anonymous `IO.pipe` has no way to be handed to a separate process
  # without passing a file descriptor across `Process.spawn`, while a
  # loopback `TCPSocket` connecting back to a port the test process already
  # owns needs nothing more than that port number, passed via the
  # `HELIX_RACK_GATE_BARRIER_PORT` environment variable
  # (`spec/support/phase8_server_helper.rb` sets it).
  #
  # The connection itself, not any data sent over it, is the first half of
  # the synchronization this gate needs: the test process's own blocking
  # `TCPServer#accept` on the barrier port returns as soon as this method
  # below calls `TCPSocket.new` -- that's what proves (without polling or a
  # fixed sleep) that this handler has actually reached the blocking read,
  # not just that the HTTP request was sent.
  class Phase8ShutdownApp
    def call(env)
      return [200, { "content-type" => "text/plain", "content-length" => "2" }, ["ok"]] \
        unless env["PATH_INFO"] == "/blocking"

      barrier_port = Integer(ENV.fetch("HELIX_RACK_GATE_BARRIER_PORT"))
      socket = TCPSocket.new("127.0.0.1", barrier_port)
      begin
        socket.read # blocks until the test process writes+closes its end
      ensure
        socket.close
      end

      [200, { "content-type" => "text/plain", "content-length" => "8" }, ["released"]]
    end
  end
end
