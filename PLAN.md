# HelixRack build plan — phased, with deterministic gates

Each phase has a **deliverable** (what ships) and a **gate** (a pass/fail check anyone can rerun
and get the same answer — no eyeballing latency graphs). Phases are ordered so each one is
buildable and testable in isolation before the next depends on it. Perf/benchmark work is pushed
to the very end and explicitly marked non-deterministic, with a methodology to make it as
reproducible as the numbers allow.

## Phase 0 — Scaffolding baseline (done)

**Deliverable:** gem skeleton, `Cargo.toml`/`ext/helix_rack`, CI workflow, rubocop config.

**Gate:** `bundle exec rake` (rubocop + spec) exits 0 in CI. `cargo build --manifest-path
ext/helix_rack/Cargo.toml` exits 0. `gem build helix_rack.gemspec` exits 0.

## Phase 1 — Raw HTTP/1.1 engine, no Ruby

Rust-only: Tokio `current_thread` runtime, TCP listener, `httparse`-based zero-copy request
parsing, a hardcoded static response. No CRuby involvement yet — proves the network/protocol
layer independently.

**Deliverable:** standalone Rust binary answering `GET /` with a fixed 200 response, HTTP/1.1
keep-alive.

**Gate (deterministic):**

- A fixed corpus of raw request fixtures (method/path/query/header edge cases: empty query
  string, repeated headers, `Host` casing, trailing slash) each map to a byte-exact expected
  response, diffed with `assert_eq!` in a Rust integration test. Any diff fails the build.
- Keep-alive proof: a test client opens **one** TCP connection, sends N sequential requests,
  asserts N responses come back on that same socket (no reconnect) and that a connection counter
  the test server exposes shows exactly 1 connection accepted for the whole run.

## Phase 2 — Rack `env` + CRuby invocation

Wire `rb-sys`/`magnus`: build the Rack `env` Hash from the parsed request, acquire the GVL, call
`.call(env)` on a loaded `config.ru` app, translate `[status, headers, body]` back to bytes.

**Deliverable:** `helix_rack -a config.ru -p 8080` serving a real Rack app end-to-end.

**Gate:**

- **Rack::Lint** wraps the test app. Lint raises on any spec violation — pass/fail is binary, no
  interpretation needed.
- An "echo" fixture app returns `env` as JSON. Run a table of crafted requests (varying verb,
  path, query string, headers) and assert the returned JSON matches an expected fixture exactly
  for every mandated key (`REQUEST_METHOD`, `PATH_INFO`, `QUERY_STRING`, `SERVER_NAME`,
  `SERVER_PORT`, `rack.version`, `rack.input`, `rack.errors`, `rack.url_scheme`).

## Phase 3 — Streaming response body

Bodies are written to the socket incrementally (via `each`), not buffered fully in RAM.

**Deliverable:** large/streaming Rack bodies (e.g. an `Enumerator` yielding chunks) serve
correctly.

**Gate:**

- **Correctness:** serve a large fixture payload (e.g. 200 MB of known content), client computes
  SHA-256 of the received bytes, compare to the source's precomputed SHA-256 — exact match
  required.
- **Memory bound:** poll the server process's RSS from `/proc/<pid>/status` during the transfer;
  assert peak RSS stays under a fixed ceiling (e.g. payload_size / 10) — a threshold assertion,
  not a benchmark, so it's pass/fail, not "faster/slower."

## Phase 4 — Keep-Alive lifecycle

`--keep-alive-timeout`, `--max-keepalive` flags become functional.

**Deliverable:** idle-timeout close and max-requests-per-connection close.

**Gate:**

- Open a connection, send `max-keepalive` requests, assert response N+1 either gets `Connection:
  close` or the socket is refused/closed — deterministic count-based assertion, no timing
  involved.
- Idle timeout: set timeout to a small fixed value (e.g. 500 ms), open a connection, send
  nothing, assert the socket receives EOF within `[timeout, timeout + fixed epsilon]` measured by
  a monotonic clock in the test — bounded-tolerance, still deterministic pass/fail.

## Phase 5 — GVL release discipline (RF06)

GVL held only for `.call(env)`; released during socket read/write and idle wait.

**Deliverable:** correct `rb_thread_call_without_gvl` usage around I/O.

**Gate — avoid sleep-based flakiness, use synchronization instead:**
A fixture app blocks on a controlled barrier (e.g. reading from a pipe/socket that the test holds
open) instead of `sleep`. While request A is blocked on that barrier, the test fires request B
and asserts B's response arrives **before** the test releases A's barrier. This proves the GVL
wasn't held across A's I/O wait — an ordering assertion, not a timing one, so it's reproducible on
any machine speed.

## Phase 6 — Preemption / time-slicing (RF07)

`--cpu-time-slice`; `rb_postponed_job` fires when a handler runs CPU-bound past the threshold.

**Deliverable:** long-running CPU-bound handlers don't fully starve the event loop.

**Gate:** instrument the postponed-job hook to increment a counter exposed back to the test (e.g.
a Ruby `$postponed_job_count` global, or an FFI counter). Run a fixture handler that busy-loops
for a fixed, deterministic number of VM instructions past the slice threshold → assert counter >
0. Run one that finishes under the threshold → assert counter == 0. Counter-based, not
latency-based.

## Phase 7 — Fault containment (RNF04)

Ruby exceptions → HTTP 500. Rust panics caught (`catch_unwind`), never crash the process.

**Deliverable:** no exception or panic can bring the server down.

**Gate:** a fixture app with a table of fault modes (raises `StandardError`, raises
`SystemStackError` from deep recursion, an ext function that panics deliberately) — for each
fixture, assert (a) the response is exactly `500`, (b) the process PID is unchanged afterward, (c)
the very next unrelated request still succeeds with `200`. All three are exact assertions.

## Phase 8 — Graceful shutdown (RNF05)

SIGTERM/SIGINT: stop accepting, drain in-flight requests within a grace period, exit.

**Deliverable:** clean shutdown behavior under Kubernetes-style termination.

**Gate:** spawn the server as a subprocess. Open a connection whose request blocks on a barrier
(same technique as Phase 5, not `sleep`). Send SIGTERM. Assert, in order: (1) a **new** connection
attempt is refused immediately, (2) releasing the barrier lets the in-flight request's response
arrive intact, (3) the process exits with code 0 within `grace_period + epsilon`. All
boolean/bounded assertions.

## Phase 9 — I/O backend parity (io_uring / epoll fallback)

**Deliverable:** runtime capability probe selects io_uring when available, epoll otherwise.

**Gate:** run the **entire Phase 1-8 test suite twice** — once with a forced-epoll env flag, once
with io_uring (skipped, not faked, on kernels without support, detected via an actual
`io_uring_setup` probe). Assert an identical pass/fail matrix between the two runs. This is a
parity check, not a speed check.

## Phase 10 — Allocator integration (RNF03)

**Deliverable:** binary links jemalloc or mimalloc.

**Gate:** static/binary inspection — `nm`/`ldd` (whichever applies on the build platform) on the
compiled artifact confirms the allocator's symbols are present and the default `malloc` is not
what's linked. Deterministic yes/no, no runtime measurement needed.

## Phase 11 — Full Rack compliance + Grape integration

**Deliverable:** a fixture Grape app (nested routes, param validation, error middleware, JSON
serialization) served through HelixRack.

**Gate:** the official Rack::Lint suite green; a request-spec table (route x params x expected
status/JSON body) asserted exactly, one row per scenario from PRD section 7.1.

## Phase 12 — Packaging

**Deliverable:** static binary, native gem built via `rake-compiler-dock`/`rb_sys` for target
platforms.

**Gate:** in a clean container (no dev toolchain), `gem install ./helix_rack-<version>.gem` exits
0, then `helix_rack --version` and `helix_rack --help` produce expected exact output. No build
tools present in that container — proves it's truly precompiled, not silently falling back to
source compilation.

## Phase 13 — Benchmarking (PRD section 7.2) — explicitly NOT deterministic, treat differently

Wall-clock P99/RSS/RPS numbers vary by machine, kernel, and noise. This phase is a **threshold
gate under a fixed methodology**, not a single deterministic assertion:

- Fixed environment: Docker container pinned via `cpuset`/`cpus: "1.0"`, `memory: "512Mi"`, CPU
  frequency scaling and turbo boost disabled if the host allows it.
- N repeated runs (e.g. N=10) per scenario (Hello World / I/O-mixed with Postgres / CPU-bound
  JSON), report median and a confidence interval, not a single sample.
- Gate: median P99 <= 60% of Puma's median P99 (the PRD's ">=40% reduction"), median RSS <= 50%
  of Puma's, across the same N-run methodology for both. Record the exact command lines and
  container digest so the run is reproducible even though the *numbers* will drift machine to
  machine.

## Dependency order

Phases 0-4 are strictly sequential (each is the substrate for the next). 5, 6, 7, 8 can happen in
parallel once Phase 4 lands, since they touch distinct mechanisms (GVL, preemption, faults,
signals). 9 and 10 depend on everything before them being stable. 11 depends on 2-4. 12 depends on
everything functional being green. 13 is last, always.
