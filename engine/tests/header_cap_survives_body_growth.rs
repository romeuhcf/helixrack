//! Regression gate for a bug a safety review caught in Phase 2 (see
//! `PLAN.md`, Phase 2 "Gate", header-cap bullet): the per-connection read
//! buffer never shrinks back down after growing to hold a large body, and
//! the header-only Slowloris cap (`MAX_BUF_CAPACITY`, 64 KiB) was checked
//! against the buffer's allocated capacity rather than how many bytes the
//! *current* incomplete header parse had actually buffered. That silently
//! raised the effective header cap to whatever the buffer had last grown
//! to (up to `MAX_BODY_CAPACITY`, 10 MiB) for the rest of a keep-alive
//! connection.
//!
//! This test proves the cap holds at exactly 64 KiB even right after a
//! legitimate multi-megabyte body on the same connection -- not just on a
//! fresh one (that's `request_size_limit.rs`'s job).
//!
//! No `sleep`: everything is a single `write_all` per step, then a bounded
//! read.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

mod support;
use support::{spawn_server, EXPECTED_RESPONSE};

const HEADER_TOO_LARGE_RESPONSE: &[u8] =
    b"HTTP/1.1 431 Request Header Fields Too Large\r\nConnection: close\r\n\r\n";

#[test]
fn header_cap_is_not_widened_by_a_prior_large_body_on_the_same_connection() {
    let server = spawn_server();

    let mut stream = TcpStream::connect(server.addr).expect("open test connection");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set read timeout");

    // Step 1: a legitimate request with a 1 MB body (well under
    // MAX_BODY_CAPACITY, well over MAX_BUF_CAPACITY) -- grows the
    // connection's read buffer past the header cap.
    let body = vec![b'a'; 1024 * 1024];
    let mut first_request =
        format!("POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n", body.len())
            .into_bytes();
    first_request.extend_from_slice(&body);
    stream
        .write_all(&first_request)
        .expect("write first request with a 1 MB body");

    let mut first_response = vec![0u8; EXPECTED_RESPONSE.len()];
    stream
        .read_exact(&mut first_response)
        .expect("read response to the first request");
    assert_eq!(
        first_response, EXPECTED_RESPONSE,
        "the 1 MB-body request should succeed normally"
    );

    // Step 2: on the SAME connection, a second request whose headers never
    // complete, well past the 64 KiB header cap but still well under the 1
    // MB the buffer already grew to in step 1. If the cap were still being
    // checked against the buffer's capacity instead of bytes actually
    // buffered for this parse, this would sail through uncapped.
    let mut oversized_headers = b"GET / HTTP/1.1\r\nX-Filler: ".to_vec();
    oversized_headers.extend(std::iter::repeat_n(b'a', 200 * 1024));
    stream
        .write_all(&oversized_headers)
        .expect("write oversized, never-completing headers on the reused connection");

    let mut second_response = vec![0u8; HEADER_TOO_LARGE_RESPONSE.len()];
    stream
        .read_exact(&mut second_response)
        .expect("read 431 response before the connection closes");
    assert_eq!(
        second_response, HEADER_TOO_LARGE_RESPONSE,
        "the header cap must still trigger at 64 KiB, not at the buffer's grown capacity"
    );
}
