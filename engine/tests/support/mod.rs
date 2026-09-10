//! Shared test scaffolding for the Phase 1 gate (see `PLAN.md`, Phase 1).
//!
//! Not part of the crate's public API -- this lives under `tests/` and is
//! pulled into each integration test binary via `mod support;`.

use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use helixrack_engine::{serve, ConnectionCounter, Handler, HandlerResponse, ParsedRequest, ResponseBody};

/// The exact bytes the Phase 1 fixed response is expected to be, for every
/// request in the fixture corpus and every non-final request in a
/// keep-alive run.
///
/// PLAN.md / PRD.md do not pin down the literal response bytes -- only that
/// `GET /` gets "a fixed 200 response". This constant is the gate's concrete
/// choice for what that response is; the phase-builder implementation must
/// match it byte-for-byte.
///
/// No `Connection` header (unlike earlier phases' version of this constant):
/// since Phase 4 (see `PLAN.md`'s Phase 4 "Architecture note"), the engine
/// is the sole source of truth for that header, stripping anything a
/// `Handler` sets -- `FixedResponseHandler` below no longer sets one, and
/// the engine adds none on a normal (non-final, non-idle-timeout) response.
///
/// Unused by `request_size_limit.rs` (it expects a 431, not this); since
/// this module is compiled once per integration-test binary, that binary
/// sees it as unused.
#[allow(dead_code)]
pub const EXPECTED_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK";

/// `spawn_server()`'s default `max_keepalive` -- large enough that none of
/// the Phase 1-3 gates (which send at most a handful of requests per
/// connection and know nothing about this phase's max-keepalive behavior)
/// could ever hit it.
const DEFAULT_MAX_KEEPALIVE: usize = 1_000_000;

/// `spawn_server()`'s default `keep_alive_timeout` -- long enough that none
/// of the Phase 1-3 gates (which run in well under a second) could ever hit
/// it.
const DEFAULT_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(3600);

/// A `Handler` that ignores the request and always returns the exact
/// response `EXPECTED_RESPONSE` is made of.
///
/// Phase 1's gate tests were written before `engine`'s `Handler` trait
/// existed (Phase 2, see `PLAN.md`) and assert on `EXPECTED_RESPONSE`
/// byte-for-byte; this handler is what lets `spawn_server` keep producing
/// exactly that response through the now-pluggable `serve`/`handle`, so
/// those gates keep passing unchanged. Since Phase 4, it no longer sets its
/// own `Connection` header -- see `EXPECTED_RESPONSE`'s doc comment.
struct FixedResponseHandler;

impl Handler for FixedResponseHandler {
    fn call(&self, _req: &ParsedRequest<'_>) -> HandlerResponse {
        HandlerResponse {
            status: 200,
            headers: vec![("Content-Length".to_string(), "2".to_string())],
            body: ResponseBody::InMemory(b"OK".to_vec()),
        }
    }
}

/// A handle to a Phase 1 engine running on a background OS thread, bound to
/// an ephemeral localhost port.
///
/// `connections` is only read by the keep-alive gate
/// (`engine/tests/keep_alive.rs`); since this module is compiled once per
/// integration-test binary, the fixture-corpus binary sees it as unused.
#[allow(dead_code)]
pub struct TestServer {
    pub addr: SocketAddr,
    pub connections: Arc<ConnectionCounter>,
}

/// Binds an ephemeral TCP port, hands it to [`helixrack_engine::serve`] on a
/// dedicated OS thread running its own Tokio `current_thread` runtime (per
/// PRD.md section 5's single-threaded-runtime architecture), and returns
/// immediately with the address to connect to.
///
/// Uses [`DEFAULT_MAX_KEEPALIVE`]/[`DEFAULT_KEEP_ALIVE_TIMEOUT`] -- see
/// [`spawn_server_with`] for a variant that lets a Phase 4 gate configure
/// either knob.
///
/// The spawned thread is intentionally not joined: `serve` never returns
/// for the life of the test process, so there's nothing to join on.
#[allow(dead_code)]
pub fn spawn_server() -> TestServer {
    spawn_server_with(DEFAULT_MAX_KEEPALIVE, DEFAULT_KEEP_ALIVE_TIMEOUT)
}

/// Same as [`spawn_server`], but lets the caller configure `max_keepalive`
/// and `keep_alive_timeout` (see `PLAN.md`'s Phase 4 "Gate") instead of
/// getting the Phase 1-3-safe defaults.
#[allow(dead_code)]
pub fn spawn_server_with(max_keepalive: usize, keep_alive_timeout: Duration) -> TestServer {
    let std_listener = StdTcpListener::bind("127.0.0.1:0").expect("bind ephemeral test port");
    let addr = std_listener
        .local_addr()
        .expect("read local_addr of freshly bound test listener");
    std_listener
        .set_nonblocking(true)
        .expect("set test listener non-blocking for adoption into Tokio");

    let connections = Arc::new(ConnectionCounter::new());
    let connections_for_thread = Arc::clone(&connections);

    thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .expect("build current_thread Tokio runtime for test server");

        // `TcpListener::from_std` registers the socket with the runtime's
        // I/O driver immediately, so it must run inside the runtime's
        // context -- hence adopting the listener from within the same
        // `block_on` future rather than before it.
        //
        // `serve` now takes `Rc<dyn Handler>` and spawns per-connection
        // tasks with `tokio::task::spawn_local` (see `PLAN.md`'s Phase 2
        // "Architecture note"), so the future has to be polled from inside
        // a `LocalSet` for those spawns to have anywhere to schedule onto.
        //
        // Deliberately ignoring the result: a correct `serve` runs forever
        // and only returns on a real I/O error, which would just end this
        // background thread with nothing to report it to.
        let local_set = tokio::task::LocalSet::new();
        let _ = runtime.block_on(local_set.run_until(async move {
            let tokio_listener = tokio::net::TcpListener::from_std(std_listener)
                .expect("adopt std TcpListener into Tokio runtime");
            serve(
                tokio_listener,
                connections_for_thread,
                Rc::new(FixedResponseHandler),
                max_keepalive,
                keep_alive_timeout,
            )
            .await
        }));
    });

    TestServer { addr, connections }
}
