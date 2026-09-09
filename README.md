# HelixRack

Native HTTP/1.1 server in Rust for Ruby Rack/Grape applications, embedding the CRuby VM via
rb-sys/magnus. Single-threaded Tokio event loop with io_uring, built to minimize P99 latency and
memory under tight CPU limits (e.g. 1 vCPU Kubernetes pods).

See [PRD.md](PRD.md) for the full product requirement document.

## Status

Early stage, pre-implementation. The gem skeleton exists; the Rust engine described in the PRD is
not built yet.

## Installation

Not yet released to RubyGems.org.

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then run `rake spec` to run
the tests. `bin/console` opens an interactive prompt.

## Contributing

This repository is public for visibility, but is not open to unreviewed external contributions.
Issues and discussions are welcome. Pull requests are only merged after review and approval from
the maintainer; see [CODEOWNERS](.github/CODEOWNERS).

## License

The gem is available as open source under the terms of the [MIT License](LICENSE.txt).
