# frozen_string_literal: true

require "net/http"
require "socket"
require "timeout"
require_relative "../support/phase8_server_helper"

# Phase 8 gate (see `PLAN.md`, Phase 8 "Gate" and its "Architecture note"/
# "Resolution"; read those, and `ext/helix_rack/src/lib.rs`'s `cancelled`
# doc comment, before touching this file -- this gate's ordering looks odd
# next to PLAN.md's original job description on purpose, and that section
# explains why).
#
# What this proves, in the order this architecture actually allows (**not**
# PLAN.md's original literal ordering -- see the Resolution note): (a)
# releasing the one in-flight request's barrier lets its response arrive
# intact (status + exact body); (b) once that in-flight request has
# finished, a **new** connection attempt is refused promptly (bounded, not
# instant-or-fail -- see `wait_until_new_connections_are_refused!`'s doc
# comment); (c) the process exits with code 0 within `grace_period +
# epsilon`. All three are exact/bounded assertions, no `sleep`-based race in
# what actually decides pass/fail -- the barrier is a real cross-process TCP
# synchronization point (see `spec/fixtures/apps/phase8_shutdown_app.rb`'s
# doc comment), and (b)/(c) are bounded polls/timeouts, not fixed-duration
# guesses.
#
# Why not PLAN.md's original ordering (send SIGTERM, assert refusal
# *before* releasing the barrier): this runtime is single-OS-thread (PRD.md
# RNF01, already the reason Phase 5's own gate can't claim two connections
# are ever served concurrently). `Handler::call` is a plain synchronous
# Rust function with no `.await` in it, so while *any* connection's handler
# is executing, the entire Tokio reactor -- accept loop included -- cannot
# make progress, full stop, regardless of what triggered the desire to
# (SIGTERM included). Asserting "refused" *before* releasing the
# deliberately-still-blocked in-flight request would be asserting something
# this architecture cannot do; that's not this gate's bug to paper over,
# it's a real, load-bearing architectural fact worth an accurate gate
# instead of a flaky or impossible one.
# rubocop:disable Metrics/BlockLength -- one real-server integration example
# plus its own well-documented helper methods reads better kept together in
# one gate file, matching spec/integration/phase3_streaming_body_spec.rb's
# precedent, than split across files for a line-count target.
RSpec.describe "Phase 8: graceful shutdown gate (RNF05)" do
  include Phase8::ServerHelper

  # rubocop:disable Lint/ConstantDefinitionInBlock -- deliberately real
  # constants (matching spec/integration/phase6_preemption_spec.rb's
  # CPU_TIME_SLICE_MS and friends), not `let`s: fixed configuration for the
  # whole gate, not per-example state.
  SHUTDOWN_APP_PATH = File.expand_path("../fixtures/apps/phase8_shutdown.ru", __dir__)

  # Short on purpose: this gate's own fixture always releases its barrier
  # well within this window (the test drives that release directly, not a
  # real slow handler), so a short grace period keeps assertion (c)'s
  # timeout tight without ever being at risk of legitimately needing more
  # time -- and if a regression ever made draining hang, this fails in ~5s
  # rather than the CLI's own 30s default.
  GRACE_PERIOD_SECONDS = 5

  # How much slack assertion (c)'s "exits within grace_period + epsilon"
  # check allows beyond GRACE_PERIOD_SECONDS. Not just process-teardown
  # jitter: see this file's own top comment and `ext/helix_rack/src/lib.rs`'s
  # `cancelled` doc comment for why "notice the signal at all" is a
  # separate, real cost on top of `GRACE_PERIOD_SECONDS` (which only bounds
  # the *drain* step, once noticed) -- this gate's own barrier release
  # happens immediately after `Process.kill`, so in practice that "notice"
  # delay should be small here, but the epsilon still needs real margin for
  # scheduling jitter on a loaded CI box.
  EPSILON_SECONDS = 10
  # rubocop:enable Lint/ConstantDefinitionInBlock

  it "drains the in-flight request, then stops accepting new connections and exits 0" do
    barrier_server = TCPServer.new("127.0.0.1", 0)
    barrier_port = barrier_server.addr[1]

    begin
      with_helix_rack_subprocess(
        SHUTDOWN_APP_PATH, barrier_port: barrier_port, grace_period_seconds: GRACE_PERIOD_SECONDS
      ) do |pid, port|
        blocking_request = fire_blocking_request(port)

        # Blocks until `Fixtures::Phase8ShutdownApp#call` actually connects
        # back -- i.e. until the request has genuinely reached the point of
        # blocking inside the handler, not just "the socket write
        # returned". No timeout arg needed beyond this Timeout wrapper: a
        # real bug here should fail loudly, not hang the suite.
        barrier_connection = Timeout.timeout(5) { barrier_server.accept }

        sigterm_sent_at = Time.now
        Process.kill("TERM", pid)

        # Released immediately, not after asserting anything else: this
        # architecture cannot notice the signal at all while this one
        # request is still blocked (see this file's own top comment) --
        # releasing it now is what lets the accept loop, and the signal
        # check, run again at all.
        barrier_connection.write("go")
        barrier_connection.close

        response = blocking_request.value
        expect(response.code).to eq("200")
        expect(response.body).to eq("released")

        wait_until_new_connections_are_refused!(port)

        assert_process_exits_cleanly_within_grace_period!(pid, since: sigterm_sent_at)
      end
    ensure
      barrier_server.close
    end
  end

  def fire_blocking_request(port)
    Thread.new do
      Thread.current.report_on_exception = false
      Net::HTTP.get_response("127.0.0.1", "/blocking", port)
    end
  end

  def assert_process_exits_cleanly_within_grace_period!(pid, since:)
    remaining = (since + GRACE_PERIOD_SECONDS + EPSILON_SECONDS) - Time.now
    _pid, status = Timeout.timeout([remaining, 0.1].max) { Process.waitpid2(pid) }

    expect(status.exitstatus).to eq(0)
  rescue Timeout::Error
    raise "helix_rack subprocess (pid #{pid}) did not exit within " \
          "#{GRACE_PERIOD_SECONDS + EPSILON_SECONDS}s of SIGTERM"
  end
end
# rubocop:enable Metrics/BlockLength
