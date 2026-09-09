//! The HelixRack engine (see `PLAN.md` at the repo root): a raw HTTP/1.1
//! network/protocol layer, pluggable since Phase 2 via the [`Handler`]
//! trait.
//!
//! Per PRD.md section 5, the implementation is a single-threaded Tokio
//! `current_thread` runtime that accepts TCP connections and parses
//! requests with `httparse` (zero-copy). Phase 1 answered every request
//! with the same hardcoded response; from Phase 2 on, [`serve`] takes a
//! caller-supplied [`Handler`] and calls it once per parsed request to
//! produce the response instead.
//!
//! The actual request parsing and response serialization lives in
//! [`connection`]; this file owns the accept loop and the
//! [`ConnectionCounter`] the Phase 1 gate (`engine/tests/`) checks.

use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::net::TcpListener;

mod connection;
mod handler;

pub use handler::{Handler, HandlerResponse, ParsedRequest};

/// Counts how many TCP connections the engine has *accepted* (not how many
/// requests it has served) since a given `serve` call started.
///
/// The Phase 1 keep-alive gate opens a single connection, sends several
/// sequential requests over it, and asserts this counter reads exactly `1`
/// for the whole run -- proving the requests were multiplexed over one
/// accepted connection rather than triggering a reconnect per request.
#[derive(Debug, Default)]
pub struct ConnectionCounter(AtomicUsize);

impl ConnectionCounter {
    /// Creates a counter starting at zero.
    pub fn new() -> Self {
        Self(AtomicUsize::new(0))
    }

    /// Number of connections accepted so far.
    pub fn accepted(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }

    /// Records that one more connection was accepted. Called once per
    /// accepted connection by [`serve`]'s accept loop.
    pub fn record_accept(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Runs the engine loop against an already-bound `listener`, answering each
/// parsed request with whatever `handler` returns.
///
/// Accepts connections in a loop on whatever (single-threaded, per PRD.md
/// section 5) Tokio runtime this future is polled on, calls
/// [`ConnectionCounter::record_accept`] once per accepted connection, then
/// hands the connection and a cloned `handler` to [`connection::handle`],
/// which reads and `httparse`-parses HTTP/1.1 requests zero-copy from a
/// reusable buffer, calls `handler.call` synchronously for each one, and
/// writes the serialized response (HTTP/1.1 keep-alive: the connection
/// stays open for the next request unless the client closes it).
///
/// Each accepted connection is handled in its own `tokio::spawn`ed task so
/// one slow or idle connection doesn't block the accept loop from taking
/// the next one -- this schedules concurrent tasks on the same OS thread,
/// it does not spawn an OS thread or a multi-thread runtime.
pub async fn serve(
    listener: TcpListener,
    connections: Arc<ConnectionCounter>,
    handler: Arc<dyn Handler>,
) -> io::Result<()> {
    loop {
        let (socket, _peer_addr) = listener.accept().await?;
        connections.record_accept();
        let handler = Arc::clone(&handler);

        tokio::spawn(async move {
            // A single connection's read/write error must not affect any
            // other connection or the accept loop, so it's swallowed here.
            let _ = connection::handle(socket, handler).await;
        });
    }
}
