//! Phase 4 gate, max-keepalive half (see `PLAN.md`, Phase 4 "Gate"):
//!
//! > Open one connection, send `max-keepalive` requests on it in sequence.
//! > The response to the `max-keepalive`-th request must carry
//! > `Connection: close` (the server signals this is the last one it will
//! > answer on this connection), and the server must close the connection
//! > right after sending it -- a `(max-keepalive + 1)`-th request attempted
//! > on the same (now-closed) connection must find it refused/reset, not
//! > answered. Deterministic count-based assertion, no timing involved.
//!
//! No `sleep`: every step is a blocking socket write/read against a
//! byte-exact expected response (or a hang-guard read timeout for the final
//! "must be refused/reset" step).

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

mod support;
use support::{spawn_server_with, EXPECTED_RESPONSE};

const REQUEST: &[u8] = b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n";
const MAX_KEEPALIVE: usize = 3;

/// The final (`MAX_KEEPALIVE`-th) response: same body/Content-Length as
/// every other response from `support::FixedResponseHandler`, but with
/// `Connection: close` -- the engine's own addition (see `PLAN.md`'s Phase 4
/// "Architecture note"), not something the handler set.
const CLOSING_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK";

#[test]
fn connection_closes_after_max_keepalive_requests_with_connection_close_on_the_last_one() {
    // A very long idle timeout: this test is only exercising max_keepalive,
    // not the idle-timeout behavior (that's keepalive_idle_timeout.rs's
    // job) -- nothing here should ever come close to tripping it.
    let server = spawn_server_with(MAX_KEEPALIVE, Duration::from_secs(3600));

    let mut stream = TcpStream::connect(server.addr).expect("open the single test connection");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set read timeout");

    // Every request before the last one: answered normally, no Connection
    // header at all (the engine only adds Connection: close on the final
    // allowed request -- see PLAN.md's Phase 4 "Architecture note").
    for request_index in 0..MAX_KEEPALIVE - 1 {
        stream
            .write_all(REQUEST)
            .unwrap_or_else(|e| panic!("request {request_index}: write failed: {e}"));

        let mut actual = vec![0u8; EXPECTED_RESPONSE.len()];
        stream
            .read_exact(&mut actual)
            .unwrap_or_else(|e| panic!("request {request_index}: read failed: {e}"));

        assert_eq!(
            actual, EXPECTED_RESPONSE,
            "request {request_index} (not yet the max-keepalive-th) must not carry Connection: close"
        );
    }

    // The MAX_KEEPALIVE-th request: the last one this connection will
    // answer -- must carry Connection: close.
    stream
        .write_all(REQUEST)
        .expect("write the max-keepalive-th request");

    let mut final_response = vec![0u8; CLOSING_RESPONSE.len()];
    stream
        .read_exact(&mut final_response)
        .expect("read the max-keepalive-th response");
    assert_eq!(
        final_response, CLOSING_RESPONSE,
        "the max-keepalive-th response must carry Connection: close"
    );

    // A (MAX_KEEPALIVE + 1)-th request attempted on the same, now-closed
    // connection must find it refused/reset, not answered. The write may
    // itself fail (server already closed and the OS noticed), or may
    // silently land in the kernel send buffer with the failure only
    // surfacing on the subsequent read -- both are "closed", so both are
    // accepted; only a real answered response would be wrong.
    let write_result = stream.write_all(REQUEST);
    let mut trailing = [0u8; 1];
    let read_result = stream.read(&mut trailing);

    match (write_result, read_result) {
        (Ok(()), Ok(0)) => {}
        (Ok(()), Err(e))
            if matches!(
                e.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
            ) => {}
        (Err(e), _)
            if matches!(
                e.kind(),
                std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
            ) => {}
        other => panic!(
            "expected the (max_keepalive + 1)-th request to be refused/reset on the closed \
             connection, got: {other:?}"
        ),
    }
}
