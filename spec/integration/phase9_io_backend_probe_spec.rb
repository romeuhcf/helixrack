# frozen_string_literal: true

# Phase 9 gate (see `PLAN.md`, Phase 9 "Architecture note"/"Gate (actual,
# narrowed)"; read those before touching this file -- this gate's shape
# looks nothing like PLAN.md's original literal wording ("run the entire
# Phase 1-8 test suite twice ... assert an identical pass/fail matrix") on
# purpose, and that section explains why: there is only one real I/O
# backend implemented (Tokio's own, epoll-based reactor), so there is
# nothing to run the suite twice *against*. What's actually verified here
# is the capability probe itself -- `engine/src/io_backend.rs`'s real,
# syscall-based io_uring detection, relayed to Ruby as
# `HelixRack.io_backend` -- not a claim that HelixRack's networking
# switches behavior based on it.
RSpec.describe "Phase 9: I/O backend capability probe" do
  it "returns a real, defined backend without raising" do
    expect(%w[io_uring epoll]).to include(HelixRack.io_backend)
  end

  it "is stable across repeated calls in the same process" do
    # The underlying kernel capability cannot change mid-process, so two
    # calls in the same run must agree -- a cheap, deterministic sanity
    # check that the probe isn't doing anything read-once/stateful in a way
    # that would make its result meaningless on a second call.
    first = HelixRack.io_backend
    second = HelixRack.io_backend

    expect(second).to eq(first)
  end
end
