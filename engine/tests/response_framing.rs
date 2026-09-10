//! Regression gate for a bug a safety review (CodeRabbit, on the Phase 4
//! PR) caught: a response with a body but neither `Content-Length` nor
//! `Transfer-Encoding` left the client with no way to know where the body
//! ends. Harmless on the final (`Connection: close`) response -- EOF marks
//! the end -- but on every other one it hangs the client waiting for more
//! bytes while the server waits for the next request on the same socket,
//! broken only by `keep_alive_timeout` eventually closing it. Not caught by
//! any earlier gate since every fixture app used so far always set
//! `Content-Length` itself.
//!
//! No `sleep`: real socket reads against byte-exact expected responses.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use helixrack_engine::{Handler, HandlerResponse, ParsedRequest, ResponseBody};

mod support;
use support::spawn_server_with_handler;

/// A `Handler` that always returns a body but never sets `Content-Length`
/// or `Transfer-Encoding` -- exactly the gap `ensure_framing`
/// (`engine/src/connection.rs`) exists to fill.
struct NoFramingHandler;

impl Handler for NoFramingHandler {
    fn call(&self, _req: &ParsedRequest<'_>) -> HandlerResponse {
        HandlerResponse {
            status: 200,
            headers: Vec::new(),
            body: ResponseBody::InMemory(b"hello".to_vec()),
        }
    }
}

const REQUEST: &[u8] = b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n";
const EXPECTED_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";

#[test]
fn a_response_missing_framing_gets_content_length_injected_so_keep_alive_still_works() {
    // A large max_keepalive/timeout: this test only exercises framing
    // injection, not Phase 4's own keep-alive limits.
    let server = spawn_server_with_handler(NoFramingHandler, 1_000_000, Duration::from_secs(3600));

    let mut stream = TcpStream::connect(server.addr).expect("open test connection");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set read timeout");

    // Two requests on the SAME connection: if Content-Length weren't
    // injected, the client would have no way to know where the first
    // response ends, and this second request/response pair would hang or
    // desync -- the whole point of this gate.
    for request_index in 0..2 {
        stream
            .write_all(REQUEST)
            .unwrap_or_else(|e| panic!("request {request_index}: write failed: {e}"));

        let mut actual = vec![0u8; EXPECTED_RESPONSE.len()];
        stream
            .read_exact(&mut actual)
            .unwrap_or_else(|e| panic!("request {request_index}: read failed: {e}"));

        assert_eq!(
            actual, EXPECTED_RESPONSE,
            "request {request_index}: expected Content-Length to have been injected"
        );
    }
}
