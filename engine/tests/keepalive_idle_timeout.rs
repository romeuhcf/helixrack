//! Phase 4 gate, idle-timeout half (see `PLAN.md`, Phase 4 "Gate"):
//!
//! > Idle timeout: set timeout to a small fixed value (e.g. 500 ms), open a
//! > connection, send nothing, assert the socket receives EOF within
//! > `[timeout, timeout + fixed epsilon]` measured by a monotonic clock in
//! > the test -- bounded-tolerance, still deterministic pass/fail. Applies
//! > only to a genuinely idle connection (no bytes of a new request
//! > buffered yet).
//!
//! No `sleep`: the client blocks on a single read (with a generous
//! hang-guard timeout well above the configured idle timeout) and measures
//! how long that read actually took with `std::time::Instant`.

use std::io::Read;
use std::net::TcpStream;
use std::time::{Duration, Instant};

mod support;
use support::spawn_server_with;

/// The configured idle timeout for this test's server -- small, so the test
/// runs fast, but well above scheduler noise.
const IDLE_TIMEOUT: Duration = Duration::from_millis(300);

/// Fixed tolerance added to `IDLE_TIMEOUT` for the upper bound -- generous
/// enough to absorb CI scheduling jitter around `tokio::time::timeout`'s
/// deadline without being so wide it stops catching a broken (e.g. never
/// firing, or firing only on the next unrelated wakeup) implementation.
const EPSILON: Duration = Duration::from_millis(300);

/// Tolerance subtracted from `IDLE_TIMEOUT` for the lower bound. The
/// client's `start` timestamp is taken *after* `TcpStream::connect`
/// returns, but the server's `tokio::time::timeout` clock effectively
/// starts at `accept()`, which can complete (and so start counting down)
/// slightly before the client's local timestamp is captured -- a real,
/// correct implementation could otherwise measure `elapsed` a hair under
/// `IDLE_TIMEOUT` and fail this assertion for a reason that has nothing to
/// do with the server's actual timeout behavior. Much smaller than
/// `EPSILON`: this only needs to absorb connect/accept handshake skew, not
/// scheduling jitter on the close side.
const LOWER_SKEW_TOLERANCE: Duration = Duration::from_millis(50);

/// A very large `max_keepalive`: this test only exercises the idle timeout,
/// not max-keepalive (that's keepalive_max_requests.rs's job) -- nothing
/// here sends any request at all.
const MAX_KEEPALIVE: usize = 1_000_000;

/// Upper bound on how long the client will block waiting for the server to
/// close -- well above `IDLE_TIMEOUT + EPSILON`, so a broken implementation
/// that never closes fails with a clear read-timeout error instead of
/// hanging the test suite.
const HANG_GUARD: Duration = Duration::from_secs(5);

#[test]
fn idle_connection_is_closed_within_the_configured_timeout() {
    let server = spawn_server_with(MAX_KEEPALIVE, IDLE_TIMEOUT);

    let mut stream = TcpStream::connect(server.addr).expect("open test connection");
    stream
        .set_read_timeout(Some(HANG_GUARD))
        .expect("set hang-guard read timeout");

    // Send nothing -- the connection sits genuinely idle from the moment it
    // connects, which is exactly the case `keep_alive_timeout` applies to.
    let start = Instant::now();
    let mut buf = [0u8; 1];
    let result = stream.read(&mut buf);
    let elapsed = start.elapsed();

    match result {
        Ok(0) => {}
        Ok(n) => panic!("expected EOF from the idle-timeout close, got {n} byte(s) instead"),
        Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {}
        Err(e) => panic!("expected EOF or a connection reset from the idle timeout, got: {e}"),
    }

    assert!(
        elapsed >= IDLE_TIMEOUT.saturating_sub(LOWER_SKEW_TOLERANCE),
        "connection closed too early: waited {elapsed:?}, configured idle timeout is \
         {IDLE_TIMEOUT:?} (lower bound allows {LOWER_SKEW_TOLERANCE:?} of connect/accept skew)"
    );
    assert!(
        elapsed <= IDLE_TIMEOUT + EPSILON,
        "connection closed too late: waited {elapsed:?}, expected at most {:?} \
         (configured idle timeout {IDLE_TIMEOUT:?} + epsilon {EPSILON:?})",
        IDLE_TIMEOUT + EPSILON
    );
}
