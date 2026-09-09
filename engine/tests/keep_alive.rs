//! Phase 1 gate, second half (see `PLAN.md`, Phase 1 "Gate"):
//!
//! > Keep-alive proof: a test client opens **one** TCP connection, sends N
//! > sequential requests, asserts N responses come back on that same socket
//! > (no reconnect) and that a connection counter the test server exposes
//! > shows exactly 1 connection accepted for the whole run.
//!
//! No `sleep` is used anywhere here: every step is a blocking socket
//! read/write against a fixed-size expected response, so the test only
//! proceeds once real bytes (or a real error) arrive.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

mod support;
use support::{spawn_server, EXPECTED_RESPONSE};

const REQUEST: &[u8] = b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n";
const REQUEST_COUNT: usize = 5;

#[test]
fn keep_alive_serves_sequential_requests_on_one_accepted_connection() {
    let server = spawn_server();

    // Exactly one TCP connection, opened once, reused for every request
    // below -- `stream` is never reconnected.
    let mut stream =
        TcpStream::connect(server.addr).expect("open the single test connection");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set read timeout");

    for request_index in 0..REQUEST_COUNT {
        stream
            .write_all(REQUEST)
            .unwrap_or_else(|e| panic!("request {request_index}: write failed: {e}"));

        let mut actual = vec![0u8; EXPECTED_RESPONSE.len()];
        stream
            .read_exact(&mut actual)
            .unwrap_or_else(|e| panic!("request {request_index}: read failed: {e}"));

        assert_eq!(
            actual, EXPECTED_RESPONSE,
            "request {request_index} on the shared connection did not get the expected response"
        );
    }

    assert_eq!(
        server.connections.accepted(),
        1,
        "expected exactly one accepted connection for {REQUEST_COUNT} sequential requests on one socket, got {}",
        server.connections.accepted()
    );
}
