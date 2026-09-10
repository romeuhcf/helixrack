# HelixRack

Native HTTP/1.1 server in Rust for Ruby Rack/Grape applications, embedding the CRuby VM via
rb-sys/magnus. A single-threaded Tokio event loop (epoll-based; see PLAN.md's Phase 9 Architecture
note for why not io_uring), built to minimize P99 latency and memory under tight CPU limits (e.g. 1
vCPU Kubernetes pods).

See [PRD.md](PRD.md) for the full product requirement document, and [PLAN.md](PLAN.md) for the
phased implementation plan and the reasoning behind every non-obvious decision along the way.

## Status

All 14 phases in PLAN.md (Phase 0 through Phase 13) are implemented and evaluated: the Rust engine,
GVL discipline, cooperative preemption, fault containment, graceful shutdown, an I/O-backend
capability probe, the mimalloc global allocator, full Rack/Grape compliance, precompiled native gem
packaging, and a comparative benchmark harness against Puma and Falcon. Implemented is not the same
as passing every gate: Phase 13's own benchmark fails its P99 latency threshold against Puma on the
`hello_world` and `io_mixed` scenarios (`cpu_bound` passes) -- a real, understood single-OS-thread
queueing tradeoff, not a bug (see PLAN.md's Phase 13 Resolution note for the numbers and why). A
post-Phase-13 fix (missing `TCP_NODELAY`) cut that gap sharply -- HelixRack now has the lowest
absolute p99 of all three servers in both remeasured scenarios -- without closing it enough to flip
the Gate;
see PLAN.md's "Post-Phase-13 follow-up" note for the before/after numbers. See PLAN.md for each
phase's own Resolution notes.

## Installation

Not yet released to RubyGems.org.

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then run `rake spec` to run
the tests. `bin/console` opens an interactive prompt. `rake bench` runs the Phase 13 benchmark
harness under `bench/` (real minutes, needs Docker -- see `bench/run.rb`'s own top comment).

## Contributing

This repository is public for visibility, but is not open to unreviewed external contributions.
Issues and discussions are welcome. Pull requests are only merged after review and approval from
the maintainer; see [CODEOWNERS](.github/CODEOWNERS).

## License

The gem is available as open source under the terms of the [MIT License](LICENSE.txt).
