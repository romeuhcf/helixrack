//! Phase 1 gate, first half (see `PLAN.md`, Phase 1 "Gate"):
//!
//! > A fixed corpus of raw request fixtures (method/path/query/header edge
//! > cases: empty query string, repeated headers, `Host` casing, trailing
//! > slash) each map to a byte-exact expected response, diffed with
//! > `assert_eq!` in a Rust integration test. Any diff fails the build.
//!
//! Phase 1's deliverable is a single hardcoded 200 response for `GET /`
//! (PLAN.md: "a hardcoded static response") -- there is no per-route
//! behavior yet (that arrives with Rack in Phase 2). So every fixture here
//! maps to the *same* expected response: the point of the corpus is that the
//! zero-copy `httparse` parser must correctly accept each of these
//! syntactically-varied-but-valid requests and still produce the exact fixed
//! response, rather than mishandling the edge case (e.g. crashing, hanging,
//! or emitting a different status).

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

mod support;
use support::{spawn_server, EXPECTED_RESPONSE};

/// One raw-bytes-on-the-wire request paired with a human-readable label for
/// failure messages.
struct Fixture {
    label: &'static str,
    raw_request: &'static [u8],
}

const FIXTURES: &[Fixture] = &[
    Fixture {
        label: "basic_get_root",
        raw_request: b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n",
    },
    Fixture {
        label: "empty_query_string",
        raw_request: b"GET /? HTTP/1.1\r\nHost: localhost\r\n\r\n",
    },
    Fixture {
        label: "repeated_headers",
        raw_request: b"GET / HTTP/1.1\r\nHost: localhost\r\nX-Custom: a\r\nX-Custom: b\r\n\r\n",
    },
    Fixture {
        label: "host_header_casing",
        raw_request: b"GET / HTTP/1.1\r\nhOsT: LocalHost\r\n\r\n",
    },
    Fixture {
        label: "trailing_slash_path",
        raw_request: b"GET /foo/ HTTP/1.1\r\nHost: localhost\r\n\r\n",
    },
];

#[test]
fn fixture_corpus_maps_byte_exact_to_expected_response() {
    for fixture in FIXTURES {
        let server = spawn_server();

        let mut stream = TcpStream::connect(server.addr)
            .unwrap_or_else(|e| panic!("[{}] connect to test server failed: {e}", fixture.label));
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set read timeout");

        stream
            .write_all(fixture.raw_request)
            .unwrap_or_else(|e| panic!("[{}] write request failed: {e}", fixture.label));

        let mut actual = vec![0u8; EXPECTED_RESPONSE.len()];
        stream
            .read_exact(&mut actual)
            .unwrap_or_else(|e| panic!("[{}] read response failed: {e}", fixture.label));

        assert_eq!(
            actual, EXPECTED_RESPONSE,
            "fixture `{}` did not map to the byte-exact expected response",
            fixture.label
        );
    }
}
