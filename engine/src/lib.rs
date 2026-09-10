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
//!
//! [`io_backend`] is a Phase 9 (`PLAN.md`, Phase 9) addition: a diagnostic
//! io_uring capability probe. It does not change what [`serve`] actually
//! does -- see that module's own doc comment for why.

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
mod io_backend;

pub use handler::{Handler, HandlerResponse, ParsedRequest, ResponseBody};
pub use io_backend::{probe as probe_io_backend, IoBackend};

/// Counts how many TCP connections the engine has *accepted* since a given
/// `serve` call started, and -- since Phase 8 (`PLAN.md`, Phase 8/RNF05) --
/// how many HTTP *requests* are currently being handled (not how many
/// connections are open), for graceful shutdown's drain step.
///
/// The Phase 1 keep-alive gate opens a single connection, sends several
/// sequential requests over it, and asserts [`accepted`](Self::accepted)
/// reads exactly `1` for the whole run -- proving the requests were
/// multiplexed over one accepted connection rather than triggering a
/// reconnect per request. That field is monotonic and never decrements.
///
/// [`in_flight_requests`](Self::in_flight_requests) is deliberately scoped
/// to *requests*, not connections -- an earlier version of this type
/// tracked "connections accepted but not yet finished" instead, and a
/// safety-review finding on this phase caught the real, measured
/// consequence: an idle keep-alive connection (no request in flight, just
/// waiting on its next read, per `connection::handle`'s
/// `keep_alive_timeout` logic) stayed "active" under that definition for as
/// long as `keep_alive_timeout` (default 15s), forcing [`drain`](Self::drain)
/// to wait out the *entire* `grace_period` for a connection with nothing
/// actually in flight on it -- measured concretely: one fast request over a
/// connection left open afterward pushed `drain` from ~0.02s to the full
/// configured grace period, every time. With PRD.md section 6.2's defaults
/// (grace period 30s, matching Kubernetes' own `terminationGracePeriodSeconds`),
/// that is the normal production shape (an ingress holding keep-alive
/// connections open), not an edge case -- the graceful path would have
/// effectively never completed before `SIGKILL`. Scoping this counter to
/// requests instead fixes it directly: an idle connection safely gets
/// dropped once the surrounding `LocalSet` is torn down after `drain`
/// returns (it's blocked on a plain `.await`'d socket read at that point,
/// not holding anything that needs orderly cleanup), exactly matching
/// real-world graceful-shutdown semantics (finish in-flight work, don't
/// wait for idle keep-alive connections to time out on their own).
#[derive(Debug, Default)]
pub struct ConnectionCounter {
    accepted: AtomicUsize,
    in_flight_requests: AtomicUsize,
    /// Fired (via `notify_waiters`) every time
    /// [`record_request_finish`](Self::record_request_finish) runs, so
    /// [`drain`](Self::drain) can wait for `in_flight_requests` to reach
    /// zero without polling on a fixed interval. Deliberately
    /// `notify_waiters`, not `notify_one`: `drain` is the only real caller
    /// today, but `notify_waiters` costs nothing extra here (this only
    /// fires during shutdown, a bounded and infrequent event) and doesn't
    /// silently drop a wakeup if more than one task were ever waiting.
    drained: tokio::sync::Notify,
}

impl ConnectionCounter {
    /// Creates a counter starting at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of connections accepted so far. Monotonic -- never
    /// decrements.
    pub fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }

    /// Number of HTTP requests currently being handled -- from
    /// [`record_request_start`](Self::record_request_start) (just before
    /// `Handler::call`) through
    /// [`record_request_finish`](Self::record_request_finish) (just after
    /// the response has been fully written to the socket) -- *not* the
    /// number of open connections; see this type's own doc comment for why
    /// that distinction is load-bearing.
    pub fn in_flight_requests(&self) -> usize {
        self.in_flight_requests.load(Ordering::SeqCst)
    }

    /// Records that one more connection was accepted. Called once per
    /// accepted connection by [`serve`]'s accept loop.
    pub fn record_accept(&self) {
        self.accepted.fetch_add(1, Ordering::SeqCst);
    }

    /// Records that a request has started being handled -- called by
    /// `connection::handle` just before calling into `Handler::call`.
    pub fn record_request_start(&self) {
        self.in_flight_requests.fetch_add(1, Ordering::SeqCst);
    }

    /// Records that a request has finished -- called by `connection::handle`
    /// once its response has been fully written to the socket (or the
    /// connection failed while trying to write it; either way, nothing
    /// about this request needs `drain` to keep waiting on it anymore).
    pub fn record_request_finish(&self) {
        self.in_flight_requests.fetch_sub(1, Ordering::SeqCst);
        self.drained.notify_waiters();
    }

    /// Waits for [`in_flight_requests`](Self::in_flight_requests) to reach
    /// zero, or for `grace_period` to elapse, whichever comes first --
    /// Phase 8's actual drain step (`PLAN.md`, Phase 8 "Deliverable":
    /// "waits for active HTTP requests to finish within a limit").
    ///
    /// Notify-driven, not polled on a fixed interval: `notified()` is
    /// registered *before* each `in_flight_requests() == 0` check inside
    /// the loop (never after), the same defensive shape this project's
    /// watchdog (`ext/helix_rack/src/lib.rs`'s `watchdog` module, Phase 6)
    /// uses for the same class of `Condvar`/`Notify` wakeup race --
    /// registering after the check would risk missing a
    /// `record_request_finish` that runs between the check and the
    /// registration, hanging this function until `grace_period` instead of
    /// returning as soon as it could. The loop (not a single wait) matters
    /// for a different reason than Phase 6's spurious-wakeup one:
    /// `notify_waiters` fires on *every* `record_request_finish`, not only
    /// the one that brings the count to zero, so this must re-check after
    /// each wakeup rather than assume the first one means "done".
    pub async fn drain(&self, grace_period: Duration) {
        let wait_for_zero = async {
            loop {
                let notified = self.drained.notified();
                if self.in_flight_requests() == 0 {
                    return;
                }
                notified.await;
            }
        };
        let _ = tokio::time::timeout(grace_period, wait_for_zero).await;
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
        let connections = Arc::clone(&connections);

        tokio::task::spawn_local(async move {
            // A single connection's read/write error must not affect any
            // other connection or the accept loop, so it's swallowed here.
            // `connections` is passed in (Phase 8, `PLAN.md`, Phase 8/RNF05)
            // so `connection::handle` can record each *request's* start/end
            // for `ConnectionCounter::drain` -- not a connection-level
            // record here, see that type's own doc comment for why scoping
            // this to requests instead of connections was a real, measured
            // correctness fix, not a style choice.
            let _ = connection::handle(socket, handler, max_keepalive, keep_alive_timeout, connections)
                .await;
        });
    }
}
