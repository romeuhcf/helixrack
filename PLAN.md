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
- Bounded read buffer: a client that sends header bytes without ever completing a request (no
  terminating blank line) must not grow the server's per-connection buffer without limit. Past a
  fixed cap, the server responds `431 Request Header Fields Too Large` and closes the connection —
  asserted by byte-exact response and a subsequent EOF, not by watching memory use.

## Phase 2 — Rack `env` + CRuby invocation

Wire `rb-sys`/`magnus`: build the Rack `env` Hash from the parsed request, call `.call(env)` on a
loaded `config.ru` app, translate `[status, headers, body]` back to bytes.

**Architecture note (GVL — revised from this phase's original plan):** the CLI entrypoint calls
into a magnus function that runs the Tokio `current_thread` runtime via `block_on` on the *same* OS
thread Ruby called it from, so `.call(env)` can happen directly without leaving it — `Handler::call`
is a synchronous, blocking Rust function, and while it runs the single-threaded runtime makes no
progress on any other connection (expected and correct, since the GVL would serialize Ruby
execution anyway). This phase was originally planned with *no* GVL release at all (that was to be
Phase 5/RF06's job), on the assumption that a real deployment has no other Ruby thread competing
for it. Real implementation found that assumption incomplete: the GVL is cooperative, and a thread
that never releases it also starves Ruby's own signal-handling checkpoints and `Thread#kill` — both
needed for Phase 8's graceful shutdown, and needed right now just to make the Phase 2 gate's test
harness (which boots the server on a background `Thread` and drives/kills it from another) work at
all without hanging forever. So Phase 2 ships a coarse GVL release: released for the idle/accept
portion of the run (`rb_thread_call_without_gvl`, magnus 0.8.2 doesn't wrap it, called directly via
raw `rb-sys`), reacquired (`rb_thread_call_with_gvl`) only around each request's `Handler::call`.
This is *not* Phase 5's deliverable — no per-read/write granularity, no drain/grace-period shutdown
(Phase 8), a polled (20ms) rather than instant cancellation — Phase 5's job narrows to upgrading
this coarse release to fine-grained I/O-boundary release plus the RF07 preemption mechanism, not
introducing GVL release from scratch. See `ext/helix_rack/src/lib.rs`'s `gvl` module for the full
reasoning and the direct reproduction that found the original no-release plan hangs indefinitely.

**Architecture note (`Rc`, not `Arc`; `spawn_local`, not `spawn`):** a magnus-backed `Handler` holds
a Ruby `Value` (the loaded app) so it can call `.call(env)` on it -- and `magnus::Value` is not
`Send`/`Sync` (Ruby values can't cross threads without the VM's involvement, and magnus enforces
this at the type level). `engine`'s original `Handler: Send + Sync` bound, `Arc<dyn Handler>`, and
`tokio::spawn` per connection were written for Phase 1's single hardcoded response, which had
nothing to make non-`Send`. Since this whole engine only ever runs on one OS thread anyway (PRD.md
RNF01), there was never real cross-thread sharing to justify atomics or `Send`. The real Phase 2
implementation drops `Send + Sync` from `Handler`, switches `Arc<dyn Handler>` to `Rc<dyn Handler>`,
and switches `serve`'s per-connection `tokio::spawn` to `tokio::task::spawn_local` inside a
`tokio::task::LocalSet` (the caller -- both the test harness and the real magnus entry point --
wraps its `block_on` in `LocalSet::new().run_until(...)`). `ConnectionCounter` stays `Arc<AtomicUsize>`
unchanged; nothing about it needed the relaxation.

**Deliverable:** `helix_rack -a config.ru -p 8080` serving a real Rack app end-to-end, via a new
pluggable `Handler` trait in `engine/` (Phase 1's `serve()` gains a handler parameter; Phase 1's
three existing gates must still pass, byte-for-byte, using a trivial fixed-response `Handler` —
this phase must not regress Phase 1).

**Gate:**

- **Rack::Lint** wraps the test app. Lint raises on any spec violation — pass/fail is binary, no
  interpretation needed.
- An "echo" fixture Rack app returns `env` as JSON, with two necessary adjustments since Rack env
  values aren't all JSON-native: report `env['rack.input'].read` (the request body string) in
  place of the `rack.input` IO object itself, and report `true` (env key present and responds to
  `#puts`) in place of the `rack.errors` IO object. Run a table of crafted requests (varying verb,
  path, query string, headers, and at least one request with a body) through the real compiled
  server (a real TCP client against a booted `helix_rack` instance, not a mocked call) and assert
  the returned JSON matches an expected fixture exactly for every mandated key
  (`REQUEST_METHOD`, `PATH_INFO`, `QUERY_STRING`, `SERVER_NAME`, `SERVER_PORT`, `rack.version`,
  the derived `rack.input` value above, the derived `rack.errors` value above, `rack.url_scheme`).
- Bounded request body: a client that declares a `Content-Length` far larger than a fixed cap must
  not make the server attempt to allocate a buffer that size. Past the cap, the server responds
  `413 Payload Too Large` and closes the connection — before reading or allocating for the body,
  not after. This is a different hazard from Phase 1's header-only Slowloris guard: a single
  request with one oversized header value is enough here, not a sustained slow drip.

## Phase 3 — Streaming response body

Bodies are written to the socket incrementally (via `each`), not buffered fully in RAM.

**Architecture decision (spill-to-tempfile past a size threshold, chosen over two riskier
alternatives):** three designs
were considered for how a Ruby response body's `#each` (push-based) gets to the socket (async,
tokio) without buffering the whole thing in RAM:

1. **Chosen: spool to a tempfile only once a body is *proven* large, stream that async.**
   `collect_chunk`-style code accumulates yielded chunks in memory as `#each` produces them, same
   as today -- up to a fixed threshold (e.g. a few hundred KiB to low single-digit MiB; the exact
   number is an implementation choice, not re-litigated here). Only once that threshold is crossed
   does it spill to a Rust-managed tempfile: write what's accumulated so far, then every subsequent
   chunk, straight to the file instead of growing the in-memory buffer further. A body that never
   crosses the threshold stays `ResponseBody::InMemory` and never touches disk at all -- the common
   case (small API/HTML responses) pays no tempfile cost; only the rare large body does. Writing to
   the tempfile is safe because `Handler::call` already blocks the whole runtime synchronously for
   its entire duration (Phase 2's established, accepted model), so a blocking `File::write_all` here
   is no riskier than what already happens for the rest of the Ruby call. `connection::handle` then
   reads a spooled file back out to the socket in bounded chunks via normal async tokio file I/O --
   a well-trodden pattern, no interference with tokio's internal socket I/O-driver bookkeeping. Real
   tradeoffs, now scoped to only the large-body path: disk I/O overhead for large bodies, and a
   tempfile lifecycle to manage (create, guarantee cleanup on every exit path including error/panic,
   and disk space is itself a finite resource that could be exhausted -- typically far larger than
   RAM, but not infinite, and not yet bounded by any cap the
   way `MAX_BUF_CAPACITY`/`MAX_BODY_CAPACITY` bound memory).
2. Rejected for now: raw-fd synchronous writes straight to the socket (convert the tokio
   `TcpStream` to its raw fd temporarily, write each chunk directly, bypassing tokio's async write
   path for the body entirely). No disk I/O, closest to true incremental streaming to the client --
   but doing raw writes behind tokio's back while it still owns the socket's I/O driver/epoll
   registration is a correctness trap that's hard to fully verify by review.
3. Rejected: pull-based Fiber/`Enumerator` iteration, matching Rack's body contract most naturally
   (pull one chunk at a time from a lazy producer). This is the same family of approach Phase 2
   already tried for a different purpose and hit an unexplained hang with a `Rack::Lint::Wrapper`
   body -- not attempted again without first understanding that hang.

**Flagged as a future improvement candidate, not a final answer:** option 1's disk I/O overhead is
a real cost this project's own KPIs (PRD.md section 2.2: P99 latency, RSS) will eventually care
about. Revisit once there's a concrete, measured perf need (Phase 13's benchmarking, or later) --
either by understanding and fixing option 3's hang (the more idiomatic long-term design), or by a
more carefully-verified version of option 2. Do not silently swap this out later without updating
this note and re-running the safety review that approved it.

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

**Narrowed scope (Phase 2 shipped a coarse version early):** Phase 2's real implementation already
had to add `rb_thread_call_without_gvl`/`_with_gvl` release around the whole idle/accept portion of
the run, for reasons unrelated to this phase (see Phase 2's GVL architecture note) — coarse-grained,
released once per `_serve_native` call rather than per read/write. This phase's job is narrower than
originally scoped: upgrade that release to per-I/O-operation granularity (so a slow client blocked
mid-read doesn't hold the GVL any longer than Phase 1-4's connection-handling loop actually needs
it), not introduce GVL release from scratch.

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

**Narrowed scope (Phase 2 shipped a minimal version early):** `RackAppHandler::handle`
(`ext/helix_rack/src/lib.rs`) already turns any magnus/Ruby-side `Result::Err` (a raised exception,
a failed type conversion) into a bare `500` with no body — otherwise nothing in `engine`'s `Handler`
trait had anywhere to put an error, since it has no `Result` in its signature. This phase's real
deliverable is the parts that minimal version doesn't cover: a real error body/logging story, and
real panic *recovery*: today a panic inside `RackAppHandler::handle` does get caught, by
`ext/helix_rack/src/lib.rs`'s `gvl::trampoline` (every `gvl::with_gvl`/`without_gvl` call is wrapped
in `catch_unwind`), but the only safe thing that boundary can do with a caught panic is abort the
whole process — unwinding further across the `extern "C"` frame back into Ruby is unsound. This
phase's job is to add an *inner* `catch_unwind` around just `RackAppHandler::handle`'s Ruby-calling
logic (inside the FFI boundary, not at it), so a panic there can become a `500` instead of an abort.

**Deliverable:** no exception or panic can bring the server down.

**Gate:** a fixture app with a table of fault modes (raises `StandardError`, raises
`SystemStackError` from deep recursion, an ext function that panics deliberately) — for each
fixture, assert (a) the response is exactly `500`, (b) the process PID is unchanged afterward, (c)
the very next unrelated request still succeeds with `200`. All three are exact assertions.

## Phase 8 — Graceful shutdown (RNF05)

SIGTERM/SIGINT: stop accepting, drain in-flight requests within a grace period, exit.

**Builds on Phase 2's `gvl::without_gvl` cancellation:** `_serve_native` already races `serve`
against a polled (20ms) `cancel` flag flipped by an unblock function (`ext/helix_rack/src/lib.rs`)
-- built so `Thread#kill` could stop the server during tests, not for SIGTERM handling. This phase
wires a real `Signal.trap("TERM")`/`"INT"` (Ruby-side, in `lib/helix_rack.rb` or `exe/helix_rack`)
to the same cancellation path, then adds the actual deliverable: stop accepting new connections
immediately on cancellation while letting in-flight ones finish within the grace period, rather than
today's cancellation, which races the whole `serve` future (including in-flight requests) and drops
them the instant `cancel` flips.

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
