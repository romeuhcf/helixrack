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

**Architecture note (the engine owns the `Connection` header, not the `Handler`):** `Connection` is
a hop-by-hop HTTP header describing this specific TCP connection's lifecycle -- the Rack
app/`Handler` has no business setting it, and HTTP/1.1 connections are persistent by default unless
either side says otherwise (RFC 7230), so the engine only needs to *add* `Connection: close` when
it is forcing one, never `Connection: keep-alive` on the normal path. `connection::handle` sets it
on the response it's about to send right before serializing, after getting the `HandlerResponse`
back from `Handler::call` -- not something `Handler`/`ext/helix_rack` needs to know about at all.

**Deliverable:** idle-timeout close and max-requests-per-connection close.

**Gate:**

- Open one connection, send `max-keepalive` requests on it in sequence. The response to the
  `max-keepalive`-th request must carry `Connection: close` (the server signals this is the last
  one it will answer on this connection), and the server must close the connection right after
  sending it — a `(max-keepalive + 1)`-th request attempted on the same (now-closed) connection
  must find it refused/reset, not answered. Deterministic count-based assertion, no timing
  involved.
- Idle timeout: set timeout to a small fixed value (e.g. 500 ms), open a connection, send
  nothing, assert the socket receives EOF within `[timeout, timeout + fixed epsilon]` measured by
  a monotonic clock in the test — bounded-tolerance, still deterministic pass/fail. Applies only
  to a genuinely idle connection (no bytes of a new request buffered yet). A client mid-request,
  trickling header/body bytes slowly, is **not** covered by this timeout or fully covered by
  anything else: Phase 1's `MAX_BUF_CAPACITY`/`MAX_BODY_CAPACITY` bound how much such a client can
  make the connection *buffer*, not how long it can take to send it — a client sending one byte
  then going silent trips neither cap. Confirmed as a real gap by this phase's safety review, left
  open deliberately: PRD.md's `--keep-alive-timeout` is specified as bounding idle connections, not
  slow ones, so this is its own future hardening item, not something to fold into this flag.

## Phase 5 — GVL release discipline (RF06)

GVL held only for `.call(env)`; released during socket read/write and idle wait.

**Re-scoped after re-examining what's actually left (the original gate below described something
architecturally impossible in this project's design — corrected here, not carried forward):**

Phase 2's real implementation already releases the GVL (`rb_thread_call_without_gvl`, coarse-
grained: once per `_serve_native` call, covering the *entire* Tokio runtime's lifetime) for
everything except the synchronous span of each request's `Handler::call`. Since Rust's own
parsing/socket I/O never touches Ruby, this already means the GVL is free during all of it —
`engine`'s side of RF06 ("release during socket read/write and idle wait") is already satisfied by
Phase 2's architecture, not something left for this phase to add.

What is genuinely open: whether the GVL is actually available to *other Ruby threads in the same
process* while one request's `Handler::call` is itself blocked inside the Rack app's own code
(e.g. a slow synchronous DB query, or literally `sleep`). This is a real, useful property (a Rack
app that spawns its own background `Thread`s, or a future multi-connection design, would starve
without it) — but it is **not** "request B on a different HTTP connection gets served while
request A is blocked": this server is single-OS-thread (PRD.md RNF01), and Tokio's own event loop
lives on that *same* thread. A synchronous blocking call inside the Rack app's Ruby code doesn't
hand control back to Tokio no matter what happens to the GVL — the OS thread itself is stuck
inside that call. Proving "another connection gets serviced concurrently" during that window would
require a different OS-thread model than this project has (or Phase 6's preemption mechanism,
which is a different, narrower tool: interrupting a *long-running Ruby computation*, not unblocking
I/O). The original gate below asked for exactly that impossible thing and must not be reused
as-is.

**What this phase's gate should actually prove instead:** while request A's `Handler::call` is
blocked inside the Rack app on a controlled barrier (a genuinely GVL-yielding blocking call —
verify empirically, don't assume, which Ruby I/O primitive actually yields the GVL for this
purpose; a plain busy-loop does not), a *separate Ruby `Thread`* the test itself spawns (standing
in for a Rack app's own background thread, or any other Ruby work sharing this process) makes real
progress and that progress is observed **before** the test releases A's barrier — an ordering
assertion against a same-process Ruby thread, not a second HTTP request. If empirical verification
finds this is already true given Phase 2's existing coarse release (plausible: Ruby's own blocking
I/O primitives are documented to release the GVL internally, independent of magnus's own
with_gvl/without_gvl nesting), this phase's deliverable becomes a regression gate proving it, not
new engine code — a valid, complete outcome, not a sign the investigation was incomplete.

**Deliverable:** either confirmation (with a gate) that Phase 2's existing coarse release already
gives other same-process Ruby threads GVL access during a blocked `Handler::call`, or, if that
turns out false, the fix that makes it true.

## Phase 6 — Preemption / time-slicing (RF07)

`--cpu-time-slice`; `rb_postponed_job` fires when a handler runs CPU-bound past the threshold.

**Architecture note (a watchdog thread — the first deviation from strict single-OS-thread, and
why):** the Rust side cannot notice a long-running `Handler::call` on its own — it has handed
control to Ruby's VM synchronously and gets none back until the Ruby call returns. `rb_postponed_
job_trigger` is documented as callable from any thread (or a signal handler) without holding the
GVL, specifically for this "someone else notices and interrupts" pattern (this needs verifying
against real Ruby source/docs before relying on it, not assumed from memory). That means the only
way to detect "this request has run past `cpu_time_slice`" is a second, dedicated OS thread — a
watchdog — whose only job is: track the current request's deadline (set by the main thread right
before each `Handler::call`, cleared right after), and if that deadline passes while still set,
call `rb_postponed_job_trigger`. This is a real exception to PRD.md RNF01 ("sem criação de thread
pools adicionais"): one watchdog thread is not a *pool* (it never processes a request, never
touches a connection, never calls into Ruby directly), but it is an additional OS thread, and that
distinction is being drawn deliberately here, not glossed over.

**Open question, investigate empirically before committing to full scope (matching Phase 5's
successful pattern — don't assume, measure):** PRD.md's RF07 wording asks for the postponed job's
callback to let "the Event Loop process pending I/O events on the socket" — actually driving
Tokio's reactor forward from inside a callback invoked synchronously by Ruby's own bytecode
dispatch, nested inside the original (still-technically-in-progress) `Handler::call`'s `with_gvl`
scope. Whether that's safely achievable (re-entrancy into the Tokio runtime from that nested
position, without risking a second nested call into Ruby, or corrupting the outer call's state) is
genuinely unverified. This phase's *gate* only requires proving the trigger mechanism itself fires
correctly (a counter, per PLAN.md's original wording below) — attempt the fuller "actually drains
Tokio" capability only if investigation shows it's safe and tractable; if not, ship the verified
counter-based mechanism and document the gap plainly (no other phase in this plan revisits it, so
say so rather than imply it's covered elsewhere).

**Deliverable:** long-running CPU-bound handlers don't fully starve the event loop, or, if that
fuller capability isn't safely achievable within this phase, a verified, correctly-firing
preemption *signal* (the mechanism RF07 asks for) with the "does it actually unstarve the loop"
gap documented, not silently claimed.

**Gate:** instrument the postponed-job hook to increment a counter exposed back to the test (e.g.
a Ruby `$postponed_job_count` global, or an FFI counter). Run a fixture handler that busy-loops
for a fixed, deterministic number of VM instructions past the slice threshold → assert counter >
0. Run one that finishes under the threshold → assert counter == 0. Counter-based, not
latency-based.

**Resolution (Step 1 verified; the open question investigated and answered "counter only" — see
`ext/helix_rack/src/lib.rs`'s `watchdog` module doc comment for the full detail behind both):**

- The API: `rb_postponed_job_trigger`'s "callable from any thread... without the GVL" claim is
  confirmed, not assumed — verified two ways: this project's own generated bindgen output
  (`target/debug/build/rb-sys-*/out/bindings-0.9.130-mri-x86_64-linux-4.0.6.rs`) and this machine's
  actually-installed Ruby 4.0.6 header (`ruby/debug.h`), word-for-word identical doc comments on
  both. Uses the current, non-deprecated `rb_postponed_job_preregister`/`rb_postponed_job_trigger`
  pair, not the older `rb_postponed_job_register`/`_register_one` (that header itself documents
  those as deprecated for real race conditions). magnus 0.8.2 wraps neither pair — confirmed by
  grepping its own "C Function Index" and its full source tree — so this module calls raw `rb-sys`
  FFI directly, same as the `gvl` module.
- The open question: investigated empirically (a throwaway standalone Tokio `current_thread` +
  `LocalSet` crate, not reasoned from memory) whether a postponed job's callback, nested inside the
  still-in-progress `Handler::call`, could safely drive the runtime forward for other connections.
  A nested `Handle::block_on` from inside an already-executing task's poll panicked immediately with
  Tokio's own "Cannot start a runtime from within a runtime" reentrancy guard, and no safe, public,
  lower-level Tokio API exists to do a partial "just drive the reactor" step instead. **Answer: no —
  this capability is NOT implemented.** This phase ships only the verified, correctly-firing
  counter-based trigger signal (`HelixRack._postponed_job_count`). A long-running CPU-bound handler
  still fully occupies this server's one OS thread until it returns or yields the GVL on its own;
  nothing in this codebase drains other connections' I/O during that pause. No later phase in this
  plan revisits this gap.
- The watchdog itself: a `Mutex`+`Condvar`-guarded deadline, armed/disarmed by `RackAppHandler::call`
  around each `Handler::call` via an RAII guard, spawned once per `_serve_native` call and shut
  down+joined before that call returns — including the `Thread#kill` path a background-`Thread`
  test harness uses (`spec/support/phase6_server_helper.rb`, matching Phase 2/5's precedent).
  That last part needed its own fix during implementation, worth recording: an earlier version put
  the shutdown/join *after* the `gvl::without_gvl` call returned, which looked correct but leaked
  one watchdog OS thread per `Thread#kill`'d server (confirmed via `/proc/self/task` thread-name
  inspection across repeated boot/kill cycles) — `rb_thread_call_without_gvl` reacquires the GVL
  before returning to its Rust caller, and GVL reacquisition is itself one of Ruby's interrupt
  checkpoints, so a pending `Thread#kill` at that point performs a non-local exit that skips any
  Rust code placed after the call. The fix: do the shutdown/join *inside* the `without_gvl` closure,
  before it returns (see that function's doc comment for the full account).
- A dedicated safety-review pass on the above found three further issues, since fixed: (1) the
  watchdog thread could also leak on a genuine early Rust-level error between spawning it and
  reaching the `without_gvl` closure (e.g. the Tokio runtime failing to build) — fixed with an RAII
  guard (`WatchdogGuard`) instead of a single manual call; (2) a `Condvar` spurious wakeup after
  firing could re-fire for the same still-in-progress request, contradicting the "fires once"
  design — fixed by re-checking the generation in a loop instead of a single `wait`; (3)
  `rb_postponed_job_preregister`'s documented failure sentinel (the 32-slot table full) was never
  checked — fixed with an explicit assertion at registration time. **A fourth, residual gap was
  found while re-verifying (1) and is left open, not silently fixed**: the RAII guard doesn't
  protect against the *same* non-local-exit mechanism the `Thread#kill`-after-`without_gvl` bug
  above already found — a pending kill delivered during any Ruby/magnus call skips `Drop` too, not
  just manually-placed cleanup code. Reordering `_serve_native` so the one Ruby call it needs
  (`RackAppHandler::prepare_app`) happens *before* spawning the watchdog measurably narrowed this
  (re-tested: 1 leak in 20 boot/kill iterations, down from roughly 1 in 2) but did not eliminate
  it — something can still deliver a kill into that stretch of plain Rust/OS code with no Ruby call
  in it, not fully root-caused. Low severity (one idle thread, bounded per occurrence, only
  reachable during server *startup*, not steady-state operation) but real; worth a second look
  before trusting this mechanism under a supervisor that rapidly boots/kills HelixRack servers.

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

**Resolution:**

- The inner `catch_unwind` this phase's job description calls for lives in `Handler::call`
  (`ext/helix_rack/src/lib.rs`), wrapped around the whole call to `self.handle(req)`: `Ok(Ok(_))` is
  the normal response, `Ok(Err(_))` (a Ruby-side failure `handle` already turned into `Err`) and
  `Err(_)` (a genuine Rust panic caught here) both map to the same `error_response()` (`500`, a real
  `text/plain` body with a correct `Content-Length` — the "real error body" half of this phase's
  narrowed scope) after being logged once via `log_fault`. Sound to catch here rather than only at
  `gvl::trampoline`'s outer boundary — but this soundness argument took **two** rounds of
  safety-review correction to get right, both worth recording since the mistake pattern is the same
  both times (a blanket claim standing in for a checked one): the first version claimed "no Rust code
  in `self.handle` ever sits under a live Ruby/C frame", which is false for `read_body`'s
  `body.block_call("each", ...)` (magnus's own re-entrant callback shape, live above the per-chunk
  Rust closure while it runs). The fix identified that magnus wraps `block_call`'s closure in its
  *own* `catch_unwind` already, converting a panic caught there into a raised Ruby exception before
  it reaches this crate's frames — so it surfaces as `Ok(Err(_))`, not `Err(_)` — and restated the
  claim as "exactly one kind of re-entrant call, and magnus already wraps it". A second
  re-verification pass on *that* restated claim found it was **also** too narrow: Phase 6's
  `watchdog::postponed_job_callback` is a second, genuinely re-entrant Ruby-to-Rust callback live
  inside `self.handle`'s dynamic extent (Ruby dispatches it at an interrupt checkpoint that can land
  inside `app.funcall("call", env)` while the watchdog is armed) — and it is *not* one of magnus's
  wrapped shapes, it's a raw `unsafe extern "C" fn` registered by hand. Sound anyway, but for a
  different reason than `block_call`'s: that callback's entire body is one atomic `fetch_add`, no
  allocation, no Ruby call, no `unwrap`/`expect` — panic-free by construction, not panic-caught by
  magnus. The final, currently-accurate version names both callbacks and both of their distinct
  soundness arguments explicitly, in `Handler::call`'s own doc comment and in
  `postponed_job_callback`'s (which now states its own panic-freedom as a maintained invariant, not
  an implementation detail) — read those rather than this note for the full text, since a third
  callback could make this note stale again without a third correction landing here.
- `log_fault` logs to the process's real stderr, deliberately never back through Ruby's own
  `$stderr`/`rack.errors`: safe to do for the `Err(magnus::Error)` case (any Ruby call that led to it
  had already returned before `handle` produced the error), but far less certain for the caught-panic
  case (the panic could in principle fire mid-way through establishing some magnus/Ruby-side
  invariant), and this one call site has no way to tell which case it's in once it only holds a
  formatted `String` — so it never calls back into Ruby from either branch. Writes via a discarded
  `io::Result` (`let _ = writeln!(io::stderr(), ...)`), not `eprintln!` — a safety-review finding,
  confirmed by reproduction, that `eprintln!` itself panics if the underlying write fails (e.g. a
  full disk), and this call runs *after* `Handler::call`'s own `catch_unwind` has already returned,
  so such a panic would have escaped to `gvl::trampoline`'s outer boundary and aborted the whole
  process — the fault-logging path becoming a bigger crash risk than the fault it logs. Dropping a
  log line on a full disk is the correct tradeoff; crashing the server over it is not.
- **A real bug found and fixed while building this phase's gate, worth recording because it was
  silent (degraded gracefully, not a crash) rather than loud:** the first version of the caught-panic
  branch passed `&payload` (`payload: Box<dyn Any + Send>`) to a helper expecting `&(dyn Any + Send)`,
  relying on implicit deref coercion. That compiles, but produces the *wrong* answer: `Box<dyn Any +
  Send>` is itself `'static`, so it satisfies `Any`'s own blanket impl, and Rust silently prefers
  unsize-coercing the outer `Box` itself into the trait object over deref-coercing to the boxed
  *contents* — every `downcast_ref::<&str>()`/`::<String>()` then misses, even for a plain string-
  literal panic, with no compiler warning. Confirmed with a standalone reproduction outside this
  crate (`&payload` → "no match" vs. `&*payload` → "matched &str", same payload, same Rust 1.95)
  before touching the real code — this was not an assumption about Rust's coercion rules, it was
  checked. Fixed by having the helper take the `Box` by value and call `downcast_ref` on it directly
  (method-call auto-deref resolves unambiguously there); the fallback-to-placeholder path this bug
  was silently hitting is real product behavior (a caught panic's actual message was never getting
  logged, just `"<non-string panic payload>"`), so this was a genuine, if low-severity (no crash, no
  wrong HTTP response, only a degraded log line), correctness bug in this phase's own new code, not
  hypothetical.
- The gate's third fixture row ("an ext function that panics deliberately") went through one design
  change worth recording too, because the first version didn't actually test what it looked like it
  tested: it exposed a separate Ruby-callable `HelixRack._debug_panic` module function for the
  fixture Rack app to call. Verified by direct smoke test against the compiled binary (not assumed)
  that this **does not exercise `Handler::call`'s `catch_unwind` at all** — magnus already wraps
  every `define_module_function`/`method!`-registered function in its own `catch_unwind`
  (`magnus-0.8.2/src/method.rs`'s `call_handle_error`, `Error::from_panic`), which converts a panic
  caught *there* straight into a raised Ruby exception before it ever reaches this crate's own code.
  So that fixture's panic was landing in `Handler::call`'s `Ok(Err(err))` branch, not its `Err(_)`
  one — a real (pre-existing, not introduced by this phase) safety net, but the wrong one for this
  gate row to be exercising, since it left the actual Phase 7 deliverable (the *inner* `catch_unwind`
  this phase adds) completely untested by that fixture. Fixed by moving the trigger inside
  `RackAppHandler::handle`'s own body instead: a request carrying `X-HelixRack-Debug-Panic: 1` panics
  directly there, with no intervening Ruby-callable-function hop, which does reach the code path this
  phase is actually testing (confirmed via the same smoke-testing technique, both before and after the
  fix, with the server's own stderr output as the evidence — see this section's git history for the
  exact before/after log lines if useful).
- `SystemStackError` needed no special handling beyond what `handle` already does: Ruby's own
  stack-overflow guard raises it as a regular exception once the VM's C stack limit is hit (not a
  process signal, not something that bypasses `funcall`'s normal error return), so it takes the same
  `Err(magnus::Error)` path as any other raised exception — confirmed via the gate spec itself
  passing, not assumed from how Ruby is generally understood to behave.
- **Two further safety-review findings, both fixed, both on the gate's third fixture row (the
  deliberate debug-header panic inside `handle`):** first, that header check was originally
  unconditional — reachable by any client, in the actual release build this gem ships (verified:
  `exe/helix_rack` loads the release `.so`, not a debug one) — which is a real problem on its own (a
  free remote panic-amplification knob, worse with `RUST_BACKTRACE=1` set, all of it costed on this
  server's one event-loop thread) and a much worse one if this crate's build profile ever gains
  `panic = "abort"` (nothing pins `panic = "unwind"` today), at which point the same header stops
  being "a caught panic" and becomes a one-request remote process kill. Fixed by gating it behind
  `debug_panic_header_enabled()`, reading `HELIX_RACK_DEBUG_PANIC=1` once via a `OnceLock`, off by
  default — `spec/support/phase7_server_helper.rb` is the one legitimate caller that sets it. Second,
  the gate itself (before this fix) could not actually tell the panic row's `500` apart from the two
  Ruby-exception rows' `500`s — identical response, identical "process still alive", identical "next
  request works" — which is exactly the blind spot that let the row's *first* design (the
  `HelixRack._debug_panic` module-function one, described above) silently land in the wrong code path
  without failing. Fixed by having `spec/support/phase7_server_helper.rb` capture the subprocess's
  real stderr to a file instead of discarding it (`File::NULL`), and the gate spec now asserts each
  row's `log_fault` output too (`"request handler panicked"` for the panic row, `"request failed"` for
  the other two) — the response alone is no longer treated as sufficient proof.
- **One finding, originally deferred, fixed instead after CodeRabbit flagged it independently as
  Major on the same PR:** `engine::connection::handle` never special-cased `HEAD` requests — it wrote
  a response's body unconditionally regardless of method — so a `HEAD` request that hit
  `error_response()`'s new 22-byte body (where the pre-Phase-7 error path returned an empty one) wrote
  bytes a `HEAD` response must not carry (RFC 9110 section 9.3.2), desyncing keep-alive framing for
  the next request on that connection. The gap itself is general (any `HEAD` request to any handler
  with a non-empty body hits it, not just the fault path) and this diff is only what made it easy to
  hit in practice, not its root cause — genuinely `connection::handle`'s to fix, and this Resolution
  note first said so, deferring it to Phase 11. Fixed here anyway once a second, independent reviewer
  (CodeRabbit) flagged the same gap unprompted and called it Major: two independent findings on one
  contained, well-understood bug outweighed the "narrowed scope" argument for leaving it. The fix
  (`connection::handle`): keep `ensure_framing`'s `Content-Length` computation unchanged, skip the
  `write_body` call for `HEAD`. New regression test,
  `engine/tests/head_request_body_suppression.rs`, sends a `HEAD` request that would carry a non-empty
  body followed by a `GET` on the *same* connection — the second response only comes back byte-exact
  if the first one didn't desync the connection, which is the actual failure mode this bug produces,
  not just "the HEAD response looked wrong in isolation".

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

**Gate (as originally written):** spawn the server as a subprocess. Open a connection whose request
blocks on a barrier (same technique as Phase 5, not `sleep`). Send SIGTERM. Assert, in order: (1) a
**new** connection attempt is refused immediately, (2) releasing the barrier lets the in-flight
request's response arrive intact, (3) the process exits with code 0 within `grace_period + epsilon`.
All boolean/bounded assertions. **See "Resolution" below — the actual gate reorders (1) and (2)**,
for a real, verified architectural reason, not a convenience.

**Resolution:**

- **Architecture note, discovered by this phase, not assumed going in:** this runtime is
  single-OS-thread (PRD.md RNF01) and `Handler::call` (`RackAppHandler`, `ext/helix_rack/src/lib.rs`)
  is a plain synchronous Rust function with no `.await` point in it — so while *any* connection's
  handler is executing, the entire Tokio reactor (accept loop included) cannot make progress on
  anything else, full stop, regardless of what triggered a desire to. This is the same tradeoff
  Phase 5's own gate already documents for concurrent request handling ("not a claim that a second
  HTTP connection gets served concurrently"); Phase 8 is where its consequence for *shutdown*
  specifically became unavoidable to confront. Practical result: "stop accepting new connections"
  cannot be instantaneous relative to the exact moment a signal arrives while a request is already
  in flight — it only becomes possible again once that one in-flight `Handler::call` returns. The
  gate's own ordering was corrected to match this reality: release the barrier *first*, then assert
  refusal, not the other way around — PLAN.md's original ordering asked for something this
  architecture cannot do, and that is the gate's error to fix, not a bug to work around.
- **A second, more serious finding, verified with a 90-second total-silence measurement, not
  assumed:** the originally-planned cancellation mechanism (racing `serve` against a `cancel`
  `AtomicBool` flipped only by `rb_thread_call_without_gvl`'s own unblock function, the same
  mechanism `Thread#kill` already used for `spec/support/phase*_server_helper.rb`'s test harnesses)
  is not just slow but can become **permanently** unreliable for a real signal: if `SIGTERM` arrives
  while a handler is blocked inside *nested* Ruby-level blocking I/O (this phase's fixture's
  `TCPSocket#read`, itself internally GVL-releasing, invoked from inside this crate's own
  `gvl::with_gvl` reacquisition, itself inside the outer `gvl::without_gvl` span the unblock function
  is registered against), `cancel` can fail to ever flip — confirmed by sending `SIGTERM` in exactly
  that window, then leaving the process completely idle (no further requests, no polling at all) for
  90 full seconds: it never flipped once. Two control experiments — `SIGTERM` sent after handling one
  *fast* request first, and one sent after a request that merely `sleep`s (also GVL-releasing, but not
  nested Ruby I/O) — resolved promptly and reliably every time, ruling out "any prior GVL release" or
  general system load as the cause; the specific nested-I/O-at-signal-time window is what reproduces
  it. The precise MRI-internal mechanism was not fully root-caused (a plausible, unconfirmed
  candidate: the inner blocking read's own separately-registered unblock function never yields the
  "active for signal delivery" slot back to the outer one once it returns) — this note states what
  was *measured*, not a confirmed CRuby-internals explanation.
- **The fix, not a workaround for the above:** stop depending on the unblock-function path for the
  signal case at all. A new process-global `SHUTDOWN_REQUESTED` flag (`ext/helix_rack/src/lib.rs`) is
  set directly by a new `HelixRack._request_shutdown` native function, called from the
  `SIGTERM`/`SIGINT` trap `lib/helix_rack.rb`'s `install_shutdown_traps` installs — a plain
  magnus-registered function call from Ruby code that (confirmed, every test run) always does run
  promptly even in the exact window where the unblock-function path was shown to break. `cancelled`
  now resolves on *either* `cancel` (still correct and exercised for `Thread#kill`, never observed to
  have this failure mode) or `SHUTDOWN_REQUESTED` becoming true. `SHUTDOWN_REQUESTED` is a `static`
  (unlike `cancel`, which is freshly allocated per call), so a same-process test harness booting
  several servers in a row needs it reset between runs, or a stale flag from an earlier run's signal
  would immediately cancel the next one — see the safety-review findings below for exactly where that
  reset runs and why (an earlier version of this note described resetting from inside
  `_serve_native`, which a later safety-review pass found was itself the wrong place, for a reason
  worth reading there rather than re-described here twice).
- Re-verified after the fix: the corrected gate (release barrier, then assert refusal) passed
  consistently in under half a second across five consecutive runs, down from never resolving at all
  within a 90-second budget beforehand — not a marginal improvement, a correctness fix.
- `--grace-period`/`grace_period_seconds` (PRD.md section 6.2, default 30s, matching Kubernetes'
  `terminationGracePeriodSeconds` default) only bounds the *drain* step once shutdown is noticed —
  the architecture-note latency above (bounded by whatever's already in flight when the signal
  arrives) is additional, separate time on top of it, not counted against it. Operators sizing this
  flag should budget for that.
- **A dedicated safety-review pass on this whole diff found and fixed five further issues,** most
  severe first:
  1. **(Medium-High) `drain` was scoped to open connections, not in-flight requests** — measured
     concretely, not theorized: one fast request over a connection left open afterward (an ordinary
     keep-alive idle period, not a bug in the client) pushed a full shutdown from ~0.02s to the
     entire configured `grace_period` (8s in the reproduction, would be the full 30s default in
     production) — a keep-alive connection with nothing in flight on it was still counted as
     "active" until `keep_alive_timeout` or client disconnect. With PRD.md's own defaults (grace
     period 30s, matching Kubernetes' own `terminationGracePeriodSeconds`), this is the *normal*
     production shape (an ingress holding keep-alive connections open), not an edge case — the
     graceful path would have effectively never completed before `SIGKILL`. Fixed by rescoping
     `ConnectionCounter` to count in-flight *requests* (bracketed by a new RAII `InFlightGuard` in
     `engine/src/connection.rs`, covering `Handler::call` through the response being fully written,
     guaranteed to decrement even on an early-return-via-`?` write error) rather than open
     connections — an idle connection between requests is safe to drop once `drain` returns and the
     `LocalSet` tears down (it's parked on a plain `.await`'d read, nothing to clean up). Re-verified:
     the same reproduction now exits in ~0.01s.
  2. **(Medium) `warn` inside the shutdown trap could itself raise, turning a graceful shutdown into
     a non-graceful one** — confirmed live, not assumed: with `$stderr` replaced by something whose
     `write` uses a `Mutex` (an ordinary thing for an embedding app's own logger to do), `warn`
     inside a trap context raised `ThreadError: can't be called from trap context`, and that
     exception surfaced at an arbitrary point on the main thread — inside the very request `drain`
     was waiting on, in one reproduction. Fixed by rescuing `StandardError` around the `warn` call
     (and, once added, around the chained previous-handler call in finding 4, independently, so one
     raising doesn't stop the other).
  3. **(Medium) The `SHUTDOWN_REQUESTED` reset's own documentation had the race backwards** — the
     original version reset the flag from inside `_serve_native`, *after* `install_shutdown_traps`
     had already armed the traps in `HelixRack.serve`, and its own comment worried about a signal
     landing *after* that reset. Direct tracing of the actual code path found the real, silent gap
     ran the *other* direction: a signal landing between trap installation and the (later) reset
     had its effect immediately erased by that reset, with no trace and no delayed retry needed — a
     second signal was required to notice anything. Worse for a background-thread caller (this
     project's own same-process test harnesses included, `spec/support/phase{2,5,6}_server_helper.rb`)
     than a same-thread analysis suggests: MRI runs trap bodies on the *main* thread regardless of
     which thread called `HelixRack.serve`, making this a genuine cross-thread race, not a
     same-thread instruction gap. Fixed by moving the reset to run *before* `install_shutdown_traps`
     (a new `HelixRack._reset_shutdown_request`, called first thing in `HelixRack.serve`) instead of
     after, and removing the old reset from `_serve_native` entirely.
  4. **(Low) The shutdown traps were permanent and unchainable** — `Signal.trap` discards whatever
     handler was registered before it, with no way for an embedder to run their own `SIGTERM`
     cleanup (closing a DB pool, say); and the trap `HelixRack.serve` installed was never removed,
     confirmed to still be active (`Signal.trap` round-tripped a real `Proc`) even after the
     `HelixRack.serve` call itself had ended — in this repo's own test suite, that meant `Ctrl-C` on
     the RSpec process stopped working the instant any Phase 2/5/6 example had run. Fixed:
     `install_shutdown_traps` now captures and chains to each signal's previous handler (if it
     responds to `#call`), and `HelixRack.serve` restores the original handler in an `ensure` around
     `_serve_native` once it returns. Re-verified live: a previously-registered trap's block runs
     when `HelixRack.serve`'s own trap fires, and the original trap is back in place once `serve`
     returns.
  5. **(Low) `spec/support/phase8_server_helper.rb`'s force-kill reap had no timeout**, unlike Phase
     7's equivalent helper. Fixed to match Phase 7's `Timeout.timeout`-bounded `wait_for_exit`
     pattern rather than a bare `Process.waitpid`.
  6. **(Major, left as a narrowed claim, not forced into a fragile fix) the gate's own proof
     strength for `drain` itself is weaker than it looks.** Because the fixture's barrier is released
     immediately after `Process.kill`, the gate can't distinguish "the response arrived intact
     because `drain` waited for it" from "the request simply finished on its own before shutdown's
     `select!` even had a chance to resolve" (which, per the architecture note above, can only happen
     once that same in-flight request has already finished anyway) — a server with `drain` deleted
     outright could plausibly still pass this specific example. `drain`'s own real effect was verified
     separately and directly instead (safety-review finding 1's own reproduction: an idle keep-alive
     connection with nothing in flight must not make shutdown wait, while a connection with a response
     still being written must), not folded into this gate — doing so deterministically would need a
     response large/slow enough to create a real, reliably-hittable write-in-progress window, which
     this gate's tiny fixture body doesn't provide. Narrowing the gate's own doc comment to state this
     explicitly, per CodeRabbit's own suggested fallback ("if this architecture cannot create that
     state, limit this gate's claim"), rather than building new test infrastructure under time
     pressure for a window that may not even be reliably hittable on this architecture.

## Phase 9 — I/O backend parity (io_uring / epoll fallback)

**Deliverable (as originally written):** runtime capability probe selects io_uring when available,
epoll otherwise.

**Gate (as originally written):** run the **entire Phase 1-8 test suite twice** — once with a
forced-epoll env flag, once with io_uring (skipped, not faked, on kernels without support, detected
via an actual `io_uring_setup` probe). Assert an identical pass/fail matrix between the two runs.
This is a parity check, not a speed check. **See "Architecture note" below — this deliverable, as
worded, is not achievable without an engine rewrite, and both it and the gate were narrowed for a
verified, not assumed, reason.**

**Architecture note (researched before writing any code, not assumed):** "selects io_uring when
available, epoll otherwise" reads as if the two were interchangeable backends behind one runtime,
switchable by a flag. They are not, for this project or for the Rust async ecosystem generally as
of this phase's research (a web search against current sources, cross-checked against this
project's own dependency tree, not recalled from training data): Tokio's own reactor (what
`engine`'s `current_thread` runtime uses today, via `mio`) has no io_uring backend at all — nothing
in stock `tokio` can be pointed at io_uring by a feature flag or an env var. The one real
Tokio-compatible io_uring integration, `tokio-uring` (the same team, tokio-rs/tokio-uring), is a
**separate runtime** with an incompatible programming model: its I/O calls take *owned* buffers
(`read(buf: Vec<u8>) -> (io::Result<usize>, Vec<u8>)`, handed back and forth across the io_uring
submission/completion queues) rather than borrowing into a caller-owned buffer the way
`tokio::io::AsyncReadExt::read` does — which is exactly the shape `engine::connection::handle`'s
zero-copy, reusable-buffer design (PRD.md RF01, every phase since Phase 1) is built around. Adopting
`tokio-uring` for real network I/O would mean rewriting `connection.rs`'s read/parse/write loop from
the ground up, not swapping a reactor underneath it — an engine rewrite, not a Phase 9-sized task,
and one that would need to happen *twice* if "parity" still meant keeping a genuinely interchangeable
epoll path working too. (Tokio's own, separately unstable io_uring work, gated behind
`--cfg tokio_unstable`, is scoped to *file* I/O — `fs::write`/`OpenOptions::open` — not network
sockets, so it doesn't help the actual bottleneck here either, and would mean shipping the gem
against an explicitly-unstable upstream API surface for a secondary code path.)

**Deliverable (actual, narrowed):** a genuine, correct io_uring **capability probe** — real kernel
detection via an actual `io_uring_setup(2)` syscall attempt (not a `/proc`/`uname` version guess,
and not hand-rolled `io_uring_params` struct bytes either: that struct's ABI is exactly the kind of
thing worth getting from the same crate family `tokio-uring` itself is built on rather than
re-deriving by hand, so this uses the `io-uring` crate's `IoUring::new(1)`, which performs the real
syscall and returns `Ok`/`Err` correctly). Exposed for diagnostic purposes (`HelixRack.io_backend`,
logged once at `exe/helix_rack` startup) rather than actually changing what the engine does: network
I/O always runs on Tokio's own (epoll, on Linux) reactor today, regardless of what the probe reports
-- this phase reports the capability honestly rather than silently claiming to act on it.

**Gate (actual, narrowed):** the original gate's premise (two real, interchangeable backends to run
the whole suite against and diff) doesn't hold with only one real backend implemented. What's
actually verified instead: (1) the probe performs a real syscall and returns a value consistent with
this machine's actual kernel capability, verified directly against the compiled binary, not just
through the RSpec suite (this development machine's kernel does support io_uring, confirmed via a
standalone throwaway script using the same `io-uring` crate before any project code was written);
(2) the probe never panics or crashes the server regardless of what the kernel reports, on a kernel
that has it or one that doesn't; (3) the full Phase 1-8 regression suite continues to pass unchanged
with the probe/dependency present, proving its addition doesn't regress anything already working
(the actual, still-meaningful shape of a "parity" claim, for the one dimension this narrowed scope
lets it mean: adding capability detection didn't change existing behavior).

## Phase 10 — Allocator integration (RNF03)

**Deliverable:** binary links jemalloc or mimalloc.

**Gate:** static/binary inspection — `nm`/`ldd` (whichever applies on the build platform) on the
compiled artifact confirms the allocator's symbols are present and the default `malloc` is not
what's linked. Deterministic yes/no, no runtime measurement needed.

**Resolution:**

- **Chose mimalloc over jemalloc**, for a reason grounded in Phase 9's own experience, not a coin
  flip: Phase 9's safety review found an unconditional Linux-only dependency (`io-uring`) would have
  broken this gem's cross-platform builds, caught only by an actual cross-compile check, not by
  reading the crate's description. Verified (web search, not assumed) that the `mimalloc` crate
  genuinely supports Linux/macOS/Windows, unlike the more Linux-centric `jemalloc`/`tikv-jemallocator`
  ecosystem. A local `cargo check --target x86_64-apple-darwin` for `mimalloc` itself failed here --
  but for a different, non-architectural reason than Phase 9's: a missing macOS C cross-toolchain in
  this sandbox (`cc: error: unrecognized command-line option '-arch'`), not a missing API the way
  `io-uring`'s failure was. Real cross-compilation for non-Linux gem platforms happens through
  `rake-compiler-dock`'s own cross-toolchain images (Phase 12), which this sandbox doesn't have --
  **not independently verified end-to-end**, flagged honestly rather than assumed clean.
- **Scope, stated plainly:** `#[global_allocator]` (`ext/helix_rack/src/lib.rs`) replaces the
  allocator for this extension's *own* Rust-side heap traffic (the `Vec`/`String`/`Box` churn
  `engine::connection`'s HTTP parsing, buffering, and response construction does -- exactly where
  this project's own allocation activity happens, and RNF03's stated fragmentation concern). It does
  **not** replace Ruby's own object allocator (CRuby's VALUE heap is its own GC-managed arena system,
  not a thin `malloc` wrapper this attribute intercepts) and does not attempt a process-wide `malloc`
  symbol override (which a `dlopen`'d extension *can* attempt via symbol interposition, but doing that
  soundly for a shared library loaded into an already-running process -- not a standalone binary, the
  usual case this technique targets -- was judged a meaningfully larger, riskier change than this
  phase's actual, achievable deliverable).
- **Verified the gate proves something real, not a tautology** -- a real risk for a "symbols are
  present" check, since mimalloc's C code could plausibly get linked in as an unused transitive
  dependency while actual allocations still went through the platform default. Checked by hand first,
  against the real compiled `.so`, before writing any gate code: `nm` confirms `mi_malloc_aligned`
  and friends are defined (not just referenced); `objdump -d` on `__rust_alloc` (the Rust ABI hook
  every `Vec`/`Box`/`String` allocation compiles down to) shows a single indirect `jmp *offset(%rip)`;
  `objdump -R` resolves that GOT offset's relocation to an address that is, byte for byte, one of
  `mi_malloc_aligned`'s own -- proof of actual wiring, not just co-presence. The gate spec encodes
  exactly this, and was itself verified to actually catch a regression: temporarily removed
  `#[global_allocator]`, rebuilt, confirmed both examples failed for the expected reason, then
  restored it and confirmed both pass again.
- **Safety review (mandatory `ext/`-touching pass) findings, second round:**
  1. **[Medium, fixed]** `-ftls-model=initial-exec`: mimalloc's vendored C code compiles with this TLS
     model by default via `libmimalloc-sys`'s own `build.rs`, consuming a small, artifact-wide static-TLS
     slot shared with every other `dlopen`'d library in the host Ruby process -- capable of making a
     *later, unrelated* `require`/`dlopen` in the same process fail outright with "cannot allocate memory
     in static TLS block," not just degrade. Fixed by enabling `mimalloc`'s own `local_dynamic_tls`
     feature (`ext/helix_rack/Cargo.toml`), which forwards straight through to `libmimalloc-sys`'s
     feature of the same name -- an initial version of this fix added `libmimalloc-sys` as an
     unnecessary explicit direct dependency, on the mistaken assumption that `mimalloc`'s own
     `[features]` table only re-exposed `debug`/`debug_in_debug`; a CodeRabbit review comment on this
     PR caught it, and checking the actual vendored 0.1.52 `Cargo.toml` confirmed `local_dynamic_tls =
     ["libmimalloc-sys/local_dynamic_tls"]` is there, so the extra direct dependency was removed.
     Independently re-verified against the rebuilt `.so` with `readelf -Wr`: `TPOFF64` relocation count
     went from the reviewer's own measured nonzero baseline to 0; 8 `DTPMOD64`/`DTPOFF64` relocations
     remain (the safe general-dynamic-model kind); `nm | grep -c mi_malloc` confirmed mimalloc is still
     linked (unchanged at 2).
  2. **[Low, addressed]** Recommended actually exercising `.github/workflows/build-gems.yml` via
     `workflow_dispatch` now, to de-risk cross-platform/cross-compiled mimalloc builds before a real
     release rather than discovering a break then -- directly motivated by Phase 9's own precedent (a
     Linux-only dependency that broke non-Linux builds, caught only by an actual cross-compile attempt).
     Judged safe to trigger without separate confirmation: the workflow only builds gems and uploads them
     as private, auto-expiring GitHub Actions artifacts (`actions/upload-artifact`), it publishes nothing
     external, and it's the same kind of repo-internal CI this project already runs on every PR.
     Worth doing: the first-ever run of this workflow (it had only ever fired on a tag push or manual
     dispatch before, neither of which had happened yet) immediately found a real, pre-existing gap
     unrelated to mimalloc -- `source-gem` and every `cross-gem` matrix job's `ruby/setup-ruby@v1` step
     omitted `ruby-version`, which needs a committed `.ruby-version`/`.tool-versions` file to resolve a
     "default" version; this repo has neither (`main.yml`'s own equivalent step always passed an explicit
     `ruby-version: ${{ matrix.ruby }}`, so this codepath was never exercised there). Fixed by pinning
     `ruby-version: '4.0.6'` explicitly in both steps, matching `main.yml`'s own pinned version.
     The re-run past that fix hit its default `fail-fast: true` matrix behavior: one platform
     (`arm-linux-musl`) failed for a reason with nothing to do with this project -- `docker pull
     rbsys/arm-linux-musl:0.9.128` itself 404s (`manifest unknown`), an upstream `oxidize-rb`/`rb_sys`
     tooling image gap -- and that single failure cancelled every other platform's job before they even
     started, hiding whether mimalloc itself cross-compiles cleanly anywhere. Added `fail-fast: false` to
     let each platform run independently (a safe, generally-good change regardless of this phase). The
     resulting full run was worth the cost: 8 of 10 platforms succeeded outright, including every platform
     that actually matters for this project's own deployment target -- `x86_64-linux`, `aarch64-linux`,
     `x86_64-linux-musl`, `aarch64-linux-musl`, `x86_64-darwin`, `arm64-darwin`, `aarch64-mingw-ucrt` --
     and it surfaced a second, genuine (not infra-flake) finding: `x64-mingw-ucrt` (64-bit Windows via the
     GNU mingw cross-toolchain) fails to compile mimalloc's vendored C source at all --
     `error: 'ERROR_COMMITMENT_MINIMUM' undeclared` in `libmimalloc-sys`'s `prim/windows/prim.c`. Verified
     this is a real, currently-unresolved upstream issue, not something specific to this project or a
     stale local toolchain: the identical error, same undeclared symbol, same file, reproduces in an
     unrelated project (`egui`) cross-compiling to `x86_64-pc-windows-gnu` with an earlier `libmimalloc-sys`
     version (0.1.42) too (github.com/emilk/egui/issues/7033, open, no workaround posted) -- the mingw-w64
     headers this cross-toolchain ships are missing a Windows SDK constant mimalloc's Windows code expects.
     `aarch64-mingw-ucrt` succeeding regardless points to a different cross-compiler/header set for that
     target, not a fix for this one. Not chased further here: this project's real deployment target is a
     Linux/macOS server, `x64-mingw-ucrt` is one line in a cross-gem release-platform list, and Phase 12
     ("Packaging") is the phase actually responsible for the native-gem release pipeline this finding
     belongs to -- flagged there rather than solved piecemeal inside Phase 10's own allocator-integration
     scope.
  3. **[Low, documented]** `Cargo.lock` is gitignored (`.gitignore:21`, a pre-existing, unmodified
     project choice -- matching how `rb_sys`-based extension gems typically avoid pinning dependents'
     resolution), so a fresh `cargo build` re-resolves `mimalloc` (and its `libmimalloc-sys` dependency)
     each time rather than reusing a locked graph. The direct dependency declaration already narrows this
     in practice -- `mimalloc = "0.1.52"` (`ext/helix_rack/Cargo.toml`) is a Cargo caret requirement, which
     on a pre-1.0 crate only admits patch-level bumps (`0.1.z`, `z` >= 52), not a silent jump to a
     different vendored mimalloc *major* -- confirmed against the
     actual resolved graph via `cargo tree -p mimalloc -p libmimalloc-sys`, which shows exactly
     `libmimalloc-sys v0.1.49` / `mimalloc v0.1.52` today, matching the `Cargo.toml` minimums exactly.
     Not pinning `Cargo.lock` itself, or switching to exact (`=`) version requirements, was judged a
     project-wide dependency-policy change out of proportion to this one phase's deliverable -- recorded
     here as a known, accepted drift surface rather than silently left undocumented.
  4. **[Low, fixed]** The gate (`spec/integration/phase10_allocator_spec.rb`) is x86-64 Linux/GNU-
     binutils specific (a literal `jmp *offset(%rip)` disassembly pattern, `objdump -R`'s `*ABS*+0x...`
     relocation format, a `.so` filename), and originally *raised* rather than skipped everywhere else --
     CI (`main.yml`) is `ubuntu-latest` x86-64 only so this never fired there, but it would fail loudly
     and unhelpfully for a contributor on e.g. Apple Silicon running `bundle exec rake` locally. Fixed
     with a `before` guard that `skip`s (not raises) when `RUBY_PLATFORM` isn't Linux/x86-64, or when
     `nm`/`objdump` aren't on `PATH` -- matching the shape Phase 9's own gate note already established for
     a platform-specific check.
  5. **[Informational, documented]** Two accepted gaps, neither a regression this diff introduces --
     both are inherent to adding *any* `#[global_allocator]` to a Ruby C extension: no `pthread_atfork`
     handler is registered, so a process that `fork()`s while another thread holds an internal mimalloc
     lock (CRuby's own `Process.fork`, or a pre-fork server) could leave the child with a wedged
     allocator -- out of scope since this project's own test/serve paths never fork, and a real fix needs
     auditing every deployment topology this extension could load into; and mimalloc's own double-
     free/corruption detection is compiled out at `MI_DEBUG=0` (this build's default), trading that
     diagnostic for release-mode speed -- the `debug`/`debug_in_debug` Cargo features exist for turning it
     back on when investigating a suspected corruption bug. Both documented directly in
     `GLOBAL_ALLOCATOR`'s doc comment (`ext/helix_rack/src/lib.rs`), not just here.
  6. **[Nit, fixed]** `GLOBAL_ALLOCATOR`'s doc comment said "process-wide" where "artifact-wide" is what's
     actually true -- a second, unrelated Rust extension `dlopen`'d into the same Ruby process keeps its
     own allocator; `#[global_allocator]` binds once per linked artifact, not once per OS process.

## Phase 11 — Full Rack compliance + Grape integration

**Deliverable:** a fixture Grape app (nested routes, param validation, error middleware, JSON
serialization) served through HelixRack.

**Gate:** the official Rack::Lint suite green; a request-spec table (route x params x expected
status/JSON body) asserted exactly, one row per scenario from PRD section 7.1.

**Resolution:**

- **One gate does both halves of PRD.md 7.1's ask, on purpose.** `spec/fixtures/apps/phase11_grape.ru`
  wraps the fixture Grape app in the real `Rack::Lint` middleware (`use Rack::Lint`) for the whole life
  of the one server subprocess `spec/integration/phase11_rack_grape_spec.rb` boots. Every request the
  gate's scenario table drives through that process is therefore *also* a live conformance check of
  HelixRack's own env construction and response handling: a `Rack::Lint::LintError` takes the identical
  path through `RackAppHandler::handle` (`ext/helix_rack/src/lib.rs`) that Phase 7's fixture app's plain
  `StandardError` row exercises, surfacing as a `500` and a `"request failed"` line in the server's
  stderr fault log -- indistinguishable from any other app-level exception by HTTP status alone. Every
  example therefore asserts the fault log gained *nothing*, not just the expected status/body, so a
  regression that happened to still produce the "right-looking" status code for the wrong reason (a Lint
  violation rather than Grape's own logic) would still fail the gate.
- **Verified this isn't tautological**, the same discipline this project has applied to every prior
  phase's gate (Phase 9's io-backend distinction, Phase 10's allocator-routing check): temporarily
  injected a genuine Rack::Lint violation into the fixture app (a non-numeric `Content-Length` header on
  the `/widgets/:id` route) and confirmed, against the real running subprocess, that the affected example
  failed for exactly the expected reason (`500` instead of `200`) -- then reverted it. Confirmed
  separately, with a minimal standalone script, that `Rack::Lint` itself does raise `Rack::Lint::LintError`
  on a real spec violation (a string status code) before relying on it inside the gate.
- **Every status code in the fixture app is explicit** (`status 201`, `status 200`), not left to Grape's
  own per-verb default -- this gate's whole point is asserting *exact* statuses, so it shouldn't depend on
  an unstated, version-specific Grape default. The 404 row is the one exception: Grape's own not-found
  response shape (plain-text `"404 Not Found"`, no `content-type`, confirmed by direct observation against
  the real running app before writing the assertion) isn't pinned to an exact body, only the status and
  the fault-log-is-empty assertion -- that shape isn't this project's own contract to guarantee.
- **`/boom` and the missing-`name` row produce non-2xx statuses but are not HelixRack-side faults**: both
  are caught entirely inside Grape's own `rescue_from` error middleware
  (`spec/fixtures/apps/phase11_grape_app.rb`), which returns a clean `[status, headers, body]` triple from
  *inside* Grape's dispatch -- no exception ever reaches `RackAppHandler::handle`, so the fault log stays
  empty for those rows too, same as every other row.
- **`grape` (`~> 4.0`) added to the `Gemfile` only**, not the gemspec -- it drives this gate's fixture app,
  it is not a runtime dependency of the gem itself (any Rack-compliant app works unmodified, Grape
  included, exactly the property this phase is verifying), matching how `rspec`/`rubocop` are already
  `Gemfile`-only in this project.
- **New `spec/support/phase11_server_helper.rb`**, not a reuse/edit of Phase 7's, matching this project's
  established per-phase-own-helper convention -- same boot/wait/reap subprocess technique as Phase 7's,
  minus the `HELIX_RACK_DEBUG_PANIC` env var and PID-liveness plumbing neither of which this gate needs.
- **No `ext/helix_rack/` or `engine/` changes this phase** -- pure Ruby/fixture/spec work, so the mandatory
  `ext/`-touching safety-review pass (this repo's own `CLAUDE.md` rule) does not apply here.

## Phase 12 — Packaging

**Deliverable:** static binary, native gem built via `rake-compiler-dock`/`rb_sys` for target
platforms.

**Gate:** in a clean container (no dev toolchain), `gem install ./helix_rack-<version>.gem` exits
0, then `helix_rack --version` and `helix_rack --help` produce expected exact output. No build
tools present in that container — proves it's truly precompiled, not silently falling back to
source compilation.

**Known input from Phase 10:** a `workflow_dispatch` run of `build-gems.yml` (Phase 10's Resolution
note, second safety-review round, finding 2) found `x64-mingw-ucrt` fails to cross-compile at all --
mimalloc's vendored C source hits a genuine, currently-open upstream mingw-w64/Windows-SDK-header
incompatibility (`ERROR_COMMITMENT_MINIMUM` undeclared), unrelated to this project's own code. Every
other real deployment platform (both Linux libc flavors and both glibc/musl variants, both macOS
architectures) built cleanly. This phase needs to decide: exclude `x64-mingw-ucrt` from the shipped
platform list (matching the existing `exclude: ["arm-linux", "x64-mingw32"]` precedent in
`build-gems.yml`'s `ci-data` job), or revisit once upstream fixes it -- not decided yet.

**Resolution:**

- **`x64-mingw-ucrt` excluded** (`.github/workflows/build-gems.yml`'s `ci-data` job, alongside the
  existing `arm-linux`/`x64-mingw32` entries) -- the "Known input" above, now decided: the upstream
  mimalloc/mingw-w64 header issue is currently open with no workaround, confirmed reproducing in an
  unrelated project too (`github.com/emilk/egui/issues/7033`), and every other real deployment platform
  builds cleanly. Revisit once upstream fixes it.
- **Three real, previously-undiscovered bugs, all found by actually attempting this phase's deliverable
  rather than assuming it would work** (the same "de-risk by trying it for real" discipline Phase 10's
  `workflow_dispatch` run established, applied here via a local `rake gem:native` build against the real
  `rbsys/x86_64-linux` rake-compiler-dock image, plus an actual clean-container install/run):
  1. **`exe/helix_rack --version` was broken.** `OptionParser` recognizes `--version` unconditionally, but
     without `parser.version` set it prints `"helix_rack: version unknown"` and exits `1` -- confirmed by
     running it directly before touching anything. `--help` already worked (`OptionParser` provides that
     one without any configuration). Fixed with one line, `parser.version = HelixRack::VERSION`
     (`exe/helix_rack`).
  2. **`helix_rack.gemspec`'s `required_ruby_version = ">= 3.2.0"` was wrong.** A real multi-Ruby-version
     cross-compile (the `rbsys/x86_64-linux` image bundles Ruby 3.0 through 4.0) failed outright for Ruby
     3.2.11 with `cannot find function 'rb_postponed_job_preregister'` -- Phase 6's preemption mechanism
     (`ext/helix_rack/src/lib.rs`) calls that C API, and Ruby's own C headers, unconditionally, no
     version-gated fallback. Verified (web search, not assumed) that `rb_postponed_job_preregister`/
     `_trigger` were added in Ruby 3.3.0. Fixed by correcting `required_ruby_version` to `>= 3.3.0`
     (`helix_rack.gemspec`, with the reasoning recorded directly in that file's own comment) rather than
     attempting a Ruby-version-conditional compile path for Phase 6's already-shipped, safety-reviewed
     mechanism -- out of proportion for what this phase actually needed. Ruby 3.2 itself reached its own
     end of life on 2026-03-31 (web-search-verified), already past by the time this was caught, so this
     costs nothing currently supported. `build-gems.yml`'s `ci-data` job's `stable-ruby-versions` exclude
     list updated to match (`"3.0", "3.1", "3.2"` added -- 3.0/3.1 are older still and already EOL too).
     `.rubocop.yml`'s `TargetRubyVersion` corrected from `3.2` to `3.3` to match -- rubocop's own
     `Gemspec/RequiredRubyVersion` cop caught the drift between the two once the gemspec changed.
  3. **The compiled extension couldn't be `require`d from a real installed native gem at all.** A
     precompiled, multi-Ruby-ABI native gem packages the compiled extension under
     `lib/helix_rack/<major.minor>/helix_rack.so` (one per target Ruby version), not a single unversioned
     `lib/helix_rack/helix_rack.so` -- `lib/helix_rack.rb`'s plain `require "helix_rack/helix_rack"`
     can't find that, reproduced directly as `LoadError: cannot load such file -- helix_rack/helix_rack`
     the first time the packaged gem was installed and run in a clean container. Fixed by matching
     Nokogiri's own well-established convention for this exact problem (read directly from the installed
     `nokogiri` gem's `lib/nokogiri/extension.rb`, not guessed): `RUBY_VERSION =~ /(\d+\.\d+)/;
     require_relative "helix_rack/#{Regexp.last_match(1)}/helix_rack"`, with a `rescue LoadError` fallback
     to the plain, unversioned `require` -- this project's own `bundle exec rake compile` dev loop (every
     phase before this one) produces exactly that unversioned `.so` and needed to keep working unchanged;
     verified it still does.
- **`rake gem:native`** (new `Rakefile` task) builds a real, precompiled, platform-tagged native gem for
  `x86_64-linux` via `rake-compiler-dock`, restricted to the corrected `>= 3.3` Ruby versions
  (`RUBY_CC_VERSION` set inside the container's own shell command -- `RakeCompilerDock.sh` doesn't forward
  arbitrary host env vars into the container it runs, confirmed by reading the actual `docker run` command
  it printed on a first attempt). `RCD_IMAGE` set internally to `rbsys/x86_64-linux:<the loaded rb_sys
  gem's own version>`, not hardcoded: `RakeCompilerDock` never auto-detects `rb_sys` -- its own *default*
  image has no Rust toolchain at all (confirmed directly: no `cargo` binary anywhere in it), and the
  `rbsys/<platform>` image family (the one `oxidize-rb/actions/cross-gem@v1` already uses in CI) has to be
  requested explicitly.
- **A real regression, caught by dispatching `build-gems.yml` before merging, not after** (the same
  discipline Phase 10 established): a first version of `gem:native` set `ext.cross_platform =
  ["x86_64-linux"]` directly on the shared `RbSys::ExtensionTask` in `Rakefile`, reasoning (wrongly) that
  merely *defining* the `cross`/`native:x86_64-linux` tasks couldn't affect anything else. Dispatching
  `build-gems.yml` on this branch showed otherwise: `x86_64-linux` built fine, but every *other* platform's
  `cross-gem` matrix job failed with `Don't know how to build task 'native:aarch64-linux'` (and similar) --
  `RbSys::ExtensionTask#init` computes `cross_platform` dynamically from `ENV["RUBY_TARGET"]`
  (`[ENV["RUBY_TARGET"]].compact`, read directly from `rb_sys`'s own source), which is exactly what
  `oxidize-rb/actions/cross-gem@v1` relies on for every platform it builds (setting `RUBY_TARGET` itself,
  per invocation) -- a static override in the Rakefile shadows that unconditionally, for every platform at
  once, not just the one `gem:native` cared about. Fixed by removing the override entirely and having
  `gem:native` opt into `x86_64-linux` the same way `cross-gem` itself does -- setting `RUBY_TARGET=
  x86_64-linux` in the container's own shell command and invoking the specific `native:x86_64-linux gem`
  task, not the generic `cross native gem` (which had only worked because of the now-reverted static
  override). Re-dispatched `build-gems.yml` after this fix and confirmed: `x86_64-linux`, `aarch64-linux`,
  `aarch64-mingw-ucrt`, `arm64-darwin`, `x86_64-darwin`, `x86_64-linux-musl`, and `aarch64-linux-musl` all
  built cleanly (`arm-linux-musl` still fails, but that's Phase 10's already-documented, unrelated missing-
  upstream-Docker-image issue, not this regression).
- **A second, independent bug in `gem:native` itself**, found by the very same re-dispatch, still on
  `verify-native-install`: `rake aborted! LoadError: cannot load such file -- rspec/core/rake_task`
  (`Rakefile:4`, this project's pre-existing top-level `require`) -- right after the preceding `bundle`
  (install) step reported success into `./vendor/bundle`. Root cause: the container's `rbenv` shim resolves
  a *plain* `rake` invocation against whichever cross-Ruby version it defaults to, not necessarily the one
  `bundle install` just populated -- so the immediately-following `rake native:x86_64-linux gem` (chained
  with `&&`, not `bundle exec`) never saw the bundled gems. Fixed by making that `bundle exec rake ...`
  explicitly, removing the ambiguity outright. This project's Rakefile requiring `rspec`/`rubocop`'s rake
  tasks unconditionally at the top (not something this phase changed) is what made the gap visible -- a
  Rakefile without those requires wouldn't hit this, plausibly why `rake_compiler_dock`'s own documented
  example (`RakeCompilerDock.sh 'bundle && rake cross native gem'`) shows the plain form working. Re-ran
  `bundle exec rake gem:native` and `bundle exec rspec spec/integration/phase12_native_gem_spec.rb` locally
  after this fix and confirmed both pass; a third `build-gems.yml` dispatch confirmed `verify-native-install`
  itself now passes in real CI too, alongside every other platform (`x86_64-linux`, `aarch64-linux`,
  `aarch64-mingw-ucrt`, `arm64-darwin`, `x86_64-darwin`, `x86_64-linux-musl`, `aarch64-linux-musl`) --
  `arm-linux-musl` is the only job that still fails, still for Phase 10's already-documented, unrelated
  reason.
- **The gate itself is `spec/integration/phase12_native_gem_spec.rb`**, a real `docker run` against a
  plain `ruby:3.4-slim` image (confirmed to have no `gcc`/`cc`/`cargo`/`rustc`/`make`), asserting the exact
  `gem install` exit code, `helix_rack --version`/`--help` output and exit codes, all inside that
  toolchain-less container -- not a check reimplemented or simulated, the real CLI run for real. Not
  synthetically injected-and-reverted the way Phase 9/10/11's gates were verified non-tautological: this
  one caught all three bugs above *for real*, on the first three attempts, before any of them were fixed --
  stronger evidence than a synthetic injection would have been.
- **Deliberately skipped, not run, in `bundle exec rspec`/`main.yml`'s routine per-PR job**: building the
  gem takes real minutes (a rake-compiler-dock container pull plus a genuine cross-compile) and needs
  Docker and network access neither routine job provides or should pay for on every commit -- matching
  Phase 10's own `workflow_dispatch`-not-`main.yml` scoping for the same class of cost. The spec's own
  `before` guards `skip` (not raise/fail) when Docker isn't on `PATH` or no prebuilt gem exists at the
  expected path, matching the shape Phase 9/10's own gate notes already established for this.
  `.github/workflows/build-gems.yml`'s new `verify-native-install` job is what actually runs it for real:
  `rake gem:native` then `bundle exec rspec spec/integration/phase12_native_gem_spec.rb`, on the same
  `workflow_dispatch`/tag-push triggers as the rest of that workflow.

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
