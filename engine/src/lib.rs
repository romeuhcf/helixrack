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
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;

/// Backoff before retrying `accept()` after it returns an error.
///
/// A single failed accept -- a transient per-connection error (e.g. the
/// peer reset the connection before the kernel finished the handshake), or
/// temporary resource pressure (e.g. the process is out of file
/// descriptors) -- must not end the whole accept loop; the next client's
/// connection attempt has nothing to do with why the last one failed. The
/// backoff exists so a *persistently* failing `accept()` (e.g. sustained
/// fd exhaustion) retries at a bounded rate instead of spinning the single
/// OS thread this runtime owns at 100% CPU.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(10);

mod connection;
mod handler;

pub use handler::{Handler, HandlerResponse, ParsedRequest, ResponseBody};

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
/// `max_keepalive` and `keep_alive_timeout` implement `PLAN.md`'s Phase 4
/// (see `connection::handle`'s doc comment for the exact semantics of
/// each): the former bounds how many requests a single connection may be
/// answered before the engine adds `Connection: close` to the final
/// response and closes it; the latter bounds how long a connection may sit
/// with no new request arriving before the engine closes it with no
/// response (nothing to answer -- the client isn't mid-request).
///
/// Each accepted connection is handled in its own `tokio::task::spawn_local`
/// task so one slow or idle connection doesn't block the accept loop from
/// taking the next one -- this schedules concurrent tasks on the same OS
/// thread, it does not spawn an OS thread or a multi-thread runtime.
/// `spawn_local` (rather than `tokio::spawn`) is required because `handler`
/// is `Rc<dyn Handler>`, not `Send` (see `Handler`'s doc comment); the
/// caller must poll this future from inside a `tokio::task::LocalSet` for
/// `spawn_local` to have anywhere to schedule onto.
///
/// A failed `accept()` never ends this loop -- see [`ACCEPT_ERROR_BACKOFF`].
/// The only way `serve` returns is if `listener` itself is dropped out from
/// under a concurrent `accept()` call, which does not happen in this
/// crate's own callers.
pub async fn serve(
    listener: TcpListener,
    connections: Arc<ConnectionCounter>,
    handler: Rc<dyn Handler>,
    max_keepalive: usize,
    keep_alive_timeout: Duration,
) -> io::Result<()> {
    loop {
        let (socket, _peer_addr) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(_) => {
                tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                continue;
            }
        };
        connections.record_accept();
        let handler = Rc::clone(&handler);

        tokio::task::spawn_local(async move {
            // A single connection's read/write error must not affect any
            // other connection or the accept loop, so it's swallowed here.
            let _ = connection::handle(socket, handler, max_keepalive, keep_alive_timeout).await;
        });
    }
}
