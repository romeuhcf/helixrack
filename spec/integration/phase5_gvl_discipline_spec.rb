# frozen_string_literal: true

require "net/http"
require_relative "../support/phase5_server_helper"
require_relative "../fixtures/apps/phase5_blocking_app"

# Phase 5 gate (see `PLAN.md`, Phase 5 -- especially "What this phase's gate
# should actually prove instead"; read that section before touching this
# file, it explains why the phase's original gate description was replaced
# rather than implemented as originally written).
#
# What this proves: while one request's `Handler::call` is blocked inside
# the Rack app's own code on a genuinely GVL-yielding I/O primitive (an
# empty `IO.pipe`'s read end -- verified by a standalone script before this
# gate was written, see the branch's report, not assumed), a *separate*,
# same-process Ruby `Thread` the test itself spawns (standing in for a Rack
# app's own background thread, or any other Ruby work sharing this process)
# must make real, observable progress, and that progress must be observed
# *before* the test releases the blocked request's barrier.
#
# This is deliberately an ordering assertion (a bounded `Queue#pop`, never
# `sleep`-then-check) rather than a timing one, so it passes or fails the
# same way regardless of machine speed. It is explicitly **not** a claim
# that a second HTTP connection gets served concurrently -- this server is
# single-OS-thread (PRD.md RNF01) and cannot do that; see PLAN.md's Phase 5
# section for why that would be a different, impossible gate.
# rubocop:disable Metrics/BlockLength -- one real-server integration example
# plus its own well-documented helper methods reads better kept together in
# one gate file, matching spec/integration/phase3_streaming_body_spec.rb's
# precedent, than split across files for a line-count target.
RSpec.describe "Phase 5: GVL release discipline gate" do
  include Phase5::ServerHelper

  # rubocop:disable Lint/ConstantDefinitionInBlock -- deliberately real
  # constants (matching spec/integration/phase3_streaming_body_spec.rb's
  # RSS_DELTA_BUDGET_KB and friends), not `let`s: fixed configuration for the
  # whole gate, not per-example state.

  # How long to wait for a signal that should arrive promptly once the
  # thing it depends on has actually happened, before concluding it will
  # never come. Generous on purpose -- this is a correctness bound (did the
  # signal arrive at all), not a precision measurement, so it only needs to
  # comfortably outlast scheduling jitter on a loaded CI box.
  SIGNAL_TIMEOUT_SECONDS = 5

  # How many increments the "separate Ruby thread" performs as its
  # observable work: large enough that finishing it isn't a fluke of a
  # single VM instruction happening to interleave in, small enough to run
  # near-instantly once actually scheduled. Matches the magnitude of the
  # standalone Step 1 verification script.
  OTHER_THREAD_ITERATIONS = 200_000
  # rubocop:enable Lint/ConstantDefinitionInBlock

  it "lets a separate same-process Ruby Thread make progress while a request is blocked inside Handler::call" do
    read_pipe, write_pipe = IO.pipe
    started = Queue.new
    app = Fixtures::Phase5BlockingApp.new(read_pipe, started)

    begin
      with_helix_rack_server(app) do |port|
        request_thread = fire_blocking_request(port)

        raise "Handler::call never reached the blocking pipe read within #{SIGNAL_TIMEOUT_SECONDS}s" \
          unless started.pop(timeout: SIGNAL_TIMEOUT_SECONDS)

        other_thread_done = Queue.new
        other_thread = spawn_observable_work(other_thread_done)

        # The actual assertion under test: does the separate thread's work
        # complete (observed via a bounded, non-sleep wait) while request A
        # is still blocked -- i.e. before this example itself releases the
        # barrier below.
        progress_observed = !other_thread_done.pop(timeout: SIGNAL_TIMEOUT_SECONDS).nil?

        write_pipe.write("x") # release the barrier regardless of the outcome above
        response = request_thread.value
        other_thread.join

        expect(progress_observed).to be(true),
                                     "a separate same-process Ruby Thread's pure-Ruby work never completed " \
                                     "while Handler::call was blocked on the pipe read -- Phase 2's coarse " \
                                     "GVL release does not give other Ruby threads GVL access during a " \
                                     "blocked request"
        expect(response.code).to eq("200")
        expect(response.body).to eq("released")
      end
    ensure
      read_pipe.close
      write_pipe.close
    end
  end

  def fire_blocking_request(port)
    Thread.new do
      Thread.current.report_on_exception = false
      Net::HTTP.get_response("127.0.0.1", "/", port)
    end
  end

  def spawn_observable_work(done_queue)
    Thread.new do
      Thread.current.report_on_exception = false
      counter = 0
      OTHER_THREAD_ITERATIONS.times { counter += 1 }
      done_queue << counter
    end
  end
end
# rubocop:enable Metrics/BlockLength
