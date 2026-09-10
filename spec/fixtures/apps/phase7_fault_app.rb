# frozen_string_literal: true

module Fixtures
  # Fixture Rack app for the Phase 7 gate (see `PLAN.md`, Phase 7 "Gate"): a
  # table of fault modes, each reachable through a distinct path, so one
  # server process (booted once by the gate spec) can exercise all of them.
  #
  # `/raise_standard_error` and `/raise_system_stack_error` are ordinary
  # Ruby-side faults -- `RackAppHandler::handle`
  # (`ext/helix_rack/src/lib.rs`) already turns any exception a Rack app
  # raises into `Err(magnus::Error)`, which `Handler::call` maps to a `500`.
  # `#recurse` (unbounded mutual self-recursion) is how PLAN.md's gate asks
  # for `SystemStackError`: Ruby's own stack-overflow guard raises it as a
  # regular exception once the VM's C stack limit is hit, not a process
  # signal or a Rust panic -- it takes the same `Err(magnus::Error)` path as
  # `StandardError` above, not `Handler::call`'s `catch_unwind`.
  #
  # There's no third *path* for the "ext function that panics deliberately"
  # fixture row PLAN.md's gate calls for: that one is triggered by a request
  # header (`X-HelixRack-Debug-Panic: 1`, checked directly inside
  # `RackAppHandler::handle`, see that method's doc comment) rather than
  # routed through this Ruby app at all -- deliberately so, since routing it
  # through a Ruby-callable native function (an earlier version of this
  # fixture's design) turned out not to exercise `Handler::call`'s
  # `catch_unwind`: magnus already wraps every Ruby-callable function in its
  # own panic-to-exception conversion before a panic there could ever reach
  # this crate's own fault handling. The gate spec sends that header
  # directly; this app doesn't need to know about it.
  class Phase7FaultApp
    def call(env)
      case env["PATH_INFO"]
      when "/raise_standard_error"
        raise StandardError, "deliberate StandardError for the Phase 7 fault-containment gate"
      when "/raise_system_stack_error"
        recurse
      else
        [200, { "content-type" => "text/plain", "content-length" => "2" }, ["ok"]]
      end
    end

    private

    # Unbounded self-recursion -- the standard way to trigger a real
    # `SystemStackError` from pure Ruby, no native code involved.
    def recurse
      recurse
    end
  end
end
