//! Shared test scaffolding for the Phase 1 gate (see `PLAN.md`, Phase 1).
//!
//! Not part of the crate's public API -- this lives under `tests/` and is
//! pulled into each integration test binary via `mod support;`.

use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::sync::Arc;
use std::thread;

use helixrack_engine::{serve, ConnectionCounter, Handler, HandlerResponse, ParsedRequest};

/// The exact bytes the Phase 1 fixed response is expected to be, for every
/// request in the fixture corpus and every request in the keep-alive run.
///
/// PLAN.md / PRD.md do not pin down the literal response bytes -- only that
/// `GET /` gets "a fixed 200 response". This constant is the gate's concrete
/// choice for what that response is; the phase-builder implementation must
/// match it byte-for-byte.
///
/// Unused by `request_size_limit.rs` (it expects a 431, not this); since
/// this module is compiled once per integration-test binary, that binary
/// sees it as unused.
#[allow(dead_code)]
pub const EXPECTED_RESPONSE: &[u8] =
    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nOK";

/// A `Handler` that ignores the request and always returns the exact
/// response `EXPECTED_RESPONSE` is made of.
///
/// Phase 1's gate tests were written before `engine`'s `Handler` trait
/// existed (Phase 2, see `PLAN.md`) and assert on `EXPECTED_RESPONSE`
/// byte-for-byte; this handler is what lets `spawn_server` keep producing
/// exactly that response through the now-pluggable `serve`/`handle`, so
/// those gates keep passing unchanged.
struct FixedResponseHandler;

impl Handler for FixedResponseHandler {
    fn call(&self, _req: &ParsedRequest<'_>) -> HandlerResponse {
        HandlerResponse {
            status: 200,
            headers: vec![
                ("Content-Length".to_string(), "2".to_string()),
                ("Connection".to_string(), "keep-alive".to_string()),
            ],
            body: b"OK".to_vec(),
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
/// The spawned thread is intentionally not joined: `serve` never returns
/// for the life of the test process, so there's nothing to join on.
pub fn spawn_server() -> TestServer {
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
            .build()
            .expect("build current_thread Tokio runtime for test server");

        // `TcpListener::from_std` registers the socket with the runtime's
        // I/O driver immediately, so it must run inside the runtime's
        // context -- hence adopting the listener from within the same
        // `block_on` future rather than before it.
        //
        // Deliberately ignoring the result: a correct `serve` runs forever
        // and only returns on a real I/O error, which would just end this
        // background thread with nothing to report it to.
        let _ = runtime.block_on(async move {
            let tokio_listener = tokio::net::TcpListener::from_std(std_listener)
                .expect("adopt std TcpListener into Tokio runtime");
            serve(
                tokio_listener,
                connections_for_thread,
                Arc::new(FixedResponseHandler),
            )
            .await
        });
    });

    TestServer { addr, connections }
}
