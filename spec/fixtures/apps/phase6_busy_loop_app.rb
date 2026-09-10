# frozen_string_literal: true

module Fixtures
  # Fixture Rack app for the Phase 6 gate (see `PLAN.md`, Phase 6 "Gate"):
  # on `#call`, busy-loops a fixed, deterministic number of iterations of a
  # trivial Ruby `while` loop (a genuine VM backward-branch checkpoint --
  # exactly the kind of point `rb_postponed_job_trigger`'s callback is
  # documented to run at, per `ext/helix_rack/src/lib.rs`'s `watchdog`
  # module doc comment) before responding.
  #
  # `iterations` is fixed per instance rather than derived from wall-clock
  # timing, matching PLAN.md's "fixed, deterministic number of VM
  # instructions" wording -- the caller (`spec/integration/
  # phase6_preemption_spec.rb`) chooses a value that reliably finishes well
  # under, or well past, whatever `--cpu-time-slice` the gate configures;
  # see that spec for the measured calibration.
  class Phase6BusyLoopApp
    def initialize(iterations)
      @iterations = iterations
    end

    def call(_env)
      i = 0
      i += 1 while i < @iterations
      body = "iterations=#{@iterations}"
      # `content-length` is required here, not cosmetic -- same reasoning as
      # `spec/fixtures/apps/phase5_blocking_app.rb`: without it, this
      # server's persistent-by-default connections (`PLAN.md`'s Phase 4
      # "Architecture note") would leave the client waiting for EOF instead
      # of returning as soon as the body arrives.
      [200, { "content-type" => "text/plain", "content-length" => body.bytesize.to_s }, [body]]
    end
  end
end
