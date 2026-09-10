//! Regression gate for a bug a dedicated safety review (on the Phase 7 PR)
//! and CodeRabbit, independently, both caught: `connection::handle` wrote a
//! response's body unconditionally, regardless of request method. HTTP/1.1
//! (RFC 9110 section 9.3.2) requires a `HEAD` response to carry the same
//! header fields a `GET` would, but never the body -- and Phase 7's new
//! `error_response()` (`ext/helix_rack/src/lib.rs`, a real, non-empty `500`
//! body) is what made this reachable in practice: a `HEAD` request that hits
//! a fault now got body bytes anyway, which the client then misreads as the
//! start of the *next* response on the same persistent connection -- a
//! silent desync, not a clean error.
//!
//! No `sleep`: real socket reads against byte-exact expected responses, on
//! the SAME connection across two requests (`HEAD` then `GET`) -- the second
//! request is what actually proves there's no desync, not just that the
//! first response's bytes happened to look right in isolation.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use helixrack_engine::{Handler, HandlerResponse, ParsedRequest, ResponseBody};

mod support;
use support::spawn_server_with_handler;

/// A `Handler` that always returns a non-empty body, regardless of request
/// method -- deliberately not method-aware itself, matching
/// `ext/helix_rack`'s `error_response()` (which has no idea what HTTP method
/// the request used) and every other real `Handler` in this engine: nothing
/// about the `Handler` trait or contract asks an implementation to special-
/// case `HEAD` -- that's `connection::handle`'s job alone, which is exactly
/// what this gate is checking.
struct AlwaysBodyHandler;

impl Handler for AlwaysBodyHandler {
    fn call(&self, _req: &ParsedRequest<'_>) -> HandlerResponse {
        HandlerResponse {
            status: 200,
            headers: Vec::new(),
            body: ResponseBody::InMemory(b"hello".to_vec()),
        }
    }
}

const HEAD_REQUEST: &[u8] = b"HEAD / HTTP/1.1\r\nHost: localhost\r\n\r\n";
const GET_REQUEST: &[u8] = b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n";

// `Content-Length: 5` (matching the body's real length, per `ensure_framing`)
// but no body bytes -- the whole point of this gate.
const EXPECTED_HEAD_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n";
const EXPECTED_GET_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";

#[test]
fn a_head_response_carries_no_body_and_the_next_request_on_the_connection_stays_in_sync() {
    let server = spawn_server_with_handler(AlwaysBodyHandler, 1_000_000, Duration::from_secs(3600));

    let mut stream = TcpStream::connect(server.addr).expect("open test connection");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set read timeout");

    stream.write_all(HEAD_REQUEST).expect("write HEAD request");
    let mut head_response = vec![0u8; EXPECTED_HEAD_RESPONSE.len()];
    stream
        .read_exact(&mut head_response)
        .expect("read HEAD response (would time out if the server also sent body bytes here)");
    assert_eq!(
        head_response, EXPECTED_HEAD_RESPONSE,
        "HEAD response must carry Content-Length but no body bytes"
    );

    // The actual regression check: if the server ever again sent `hello`
    // after a HEAD response, those bytes would already be sitting in the
    // socket ahead of this second response, and `read_exact` below would
    // read stale body bytes instead of (or mixed with) the real one --
    // this GET request/response pair only comes out byte-exact if the
    // connection stayed synchronized.
    stream.write_all(GET_REQUEST).expect("write GET request");
    let mut get_response = vec![0u8; EXPECTED_GET_RESPONSE.len()];
    stream
        .read_exact(&mut get_response)
        .expect("read GET response");
    assert_eq!(
        get_response, EXPECTED_GET_RESPONSE,
        "GET response after a HEAD response on the same connection must be byte-exact, not desynced"
    );
}
