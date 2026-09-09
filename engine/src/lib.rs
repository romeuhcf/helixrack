//! Phase 1 of the HelixRack engine (see `PLAN.md` at the repo root): a raw
//! HTTP/1.1 network/protocol layer with **no** CRuby involvement.
//!
//! This crate proves the network + protocol layer independently of the Ruby
//! VM. Per PRD.md section 5, the eventual implementation is a single-threaded
//! Tokio `current_thread` runtime that accepts TCP connections, parses
//! requests with `httparse` (zero-copy), and answers with a fixed 200
//! response, keeping the connection open for HTTP/1.1 keep-alive.
//!
//! This file is a skeleton: it defines the public API shape the Phase 1 gate
//! (`engine/tests/`) is written against, but the request-parsing/response
//! logic itself is intentionally unimplemented (`todo!()`). Filling it in is
//! separate follow-up work (the phase-builder step), not part of writing the
//! gate.

use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::net::TcpListener;

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

    /// Records that one more connection was accepted.
    ///
    /// Not called anywhere yet -- the accept loop that would call this is
    /// part of the real Phase 1 implementation, not this skeleton.
    pub fn record_accept(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Runs the Phase 1 engine loop against an already-bound `listener`.
///
/// Intended real behavior (not implemented here): accept connections in a
/// loop on a Tokio `current_thread` runtime, call
/// [`ConnectionCounter::record_accept`] once per accepted connection, then on
/// that connection read and `httparse`-parse HTTP/1.1 requests zero-copy from
/// a reusable buffer, writing the same fixed 200 response for each one
/// (HTTP/1.1 keep-alive: the connection stays open for the next request
/// unless the client closes it).
///
/// This skeleton body is deliberately unimplemented so the Phase 1 gate in
/// `engine/tests/` compiles and fails for the right reason (this `todo!()`)
/// until the phase-builder step fills in the real logic.
pub async fn serve(_listener: TcpListener, _connections: Arc<ConnectionCounter>) -> io::Result<()> {
    todo!("Phase 1: accept loop + httparse request parsing + fixed 200 response, see PLAN.md")
}
