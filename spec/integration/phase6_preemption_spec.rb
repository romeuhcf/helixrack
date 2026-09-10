# frozen_string_literal: true

require "net/http"
require_relative "../support/phase6_server_helper"
require_relative "../fixtures/apps/phase6_busy_loop_app"

# Phase 6 gate (see `PLAN.md`, Phase 6 "Gate"; read that section, and
# `ext/helix_rack/src/lib.rs`'s `watchdog` module doc comment, before
# touching this file).
#
# What this proves: a fixture handler that busy-loops past the configured
# `--cpu-time-slice` makes the postponed-job counter (`HelixRack.
# postponed_job_count`) increase; a fixture handler that finishes well under
# the slice does not move it at all. Counter-based, not latency-based, per
# PLAN.md's own wording -- this gate does not (and, per the `watchdog`
# module's doc comment, could not honestly) assert that the event loop
# actually served another connection during the slow handler's run; it
# proves only that the trigger mechanism itself fires correctly.
#
# Both examples read `HelixRack.postponed_job_count` before and after their
# request and assert on the *delta*, never on an absolute value: that
# counter is a process-global, monotonically increasing total (see the
# `watchdog` module's doc comment) that does not reset between examples, so
# an absolute "== 0" assertion would be order-dependent -- a delta isn't.
RSpec.describe "Phase 6: preemption / time-slicing gate" do
  include Phase6::ServerHelper

  # rubocop:disable Lint/ConstantDefinitionInBlock -- deliberately real
  # constants (matching spec/integration/phase5_gvl_discipline_spec.rb's
  # SIGNAL_TIMEOUT_SECONDS and friends), not `let`s: fixed configuration for
  # the whole gate, not per-example state.

  # The `--cpu-time-slice` value this gate configures explicitly (see
  # `spec/support/phase6_server_helper.rb`'s doc comment for why it's passed
  # rather than left at `lib/helix_rack.rb`'s default) -- PRD.md section
  # 6.2's own default (5ms), used here rather than a synthetic value so this
  # gate exercises a realistic configuration, not an artificially easy one.
  CPU_TIME_SLICE_MS = 5

  # How many `while`-loop iterations the "finishes well under the slice"
  # fixture runs. Measured on this project's own Ruby 4.0.6 build (`ruby -e
  # '...'`, a plain counting `while` loop, `Process.clock_gettime`
  # start/end): 10 iterations took ~0.001ms. 100 leaves roughly two orders
  # of magnitude of headroom under `CPU_TIME_SLICE_MS` (5ms) -- comfortably
  # immune to scheduling jitter on a loaded CI box, while the loop body
  # itself stays trivial to read.
  FAST_ITERATIONS = 100

  # How many iterations the "runs past the slice" fixture runs. Measured the
  # same way: 5_000_000 iterations took ~61.6ms on this machine -- roughly
  # 12x `CPU_TIME_SLICE_MS` (5ms), comfortable margin for the deadline to be
  # crossed well before the loop finishes (leaving room for at least one
  # more VM interrupt-check point -- a loop backward-branch -- to actually
  # invoke the postponed job's callback) without making this example slow
  # enough to matter for the suite's total run time.
  SLOW_ITERATIONS = 5_000_000
  # rubocop:enable Lint/ConstantDefinitionInBlock

  it "fires the preemption signal when a handler runs past the configured slice" do
    app = Fixtures::Phase6BusyLoopApp.new(SLOW_ITERATIONS)

    with_helix_rack_server(app, cpu_time_slice_ms: CPU_TIME_SLICE_MS) do |port|
      baseline = HelixRack.postponed_job_count
      response = Net::HTTP.get_response("127.0.0.1", "/", port)

      expect(response.code).to eq("200")
      expect(HelixRack.postponed_job_count - baseline).to be > 0
    end
  end

  it "does not fire the preemption signal when a handler finishes well under the configured slice" do
    app = Fixtures::Phase6BusyLoopApp.new(FAST_ITERATIONS)

    with_helix_rack_server(app, cpu_time_slice_ms: CPU_TIME_SLICE_MS) do |port|
      baseline = HelixRack.postponed_job_count
      response = Net::HTTP.get_response("127.0.0.1", "/", port)

      expect(response.code).to eq("200")
      expect(HelixRack.postponed_job_count - baseline).to eq(0)
    end
  end
end
