//! Phase 2 gate, bounded-body half (see `PLAN.md`, Phase 2 "Gate"):
//!
//! > Bounded request body: a client that declares a `Content-Length` far
//! > larger than a fixed cap must not make the server attempt to allocate a
//! > buffer that size. Past the cap, the server responds `413 Payload Too
//! > Large` and closes the connection -- before reading or allocating for
//! > the body, not after.
//!
//! Unlike Phase 1's header-only Slowloris gate (`request_size_limit.rs`),
//! this needs only a single request with one oversized declared length, not
//! a sustained drip of bytes -- the vector here is one header value causing
//! an unbounded allocation attempt, not a connection that never completes.
//!
//! No `sleep`: the client writes the (small) headers-plus-declaration in
//! one shot, then blocks on a read with a hang-guard timeout.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

mod support;
use support::spawn_server;

const PAYLOAD_TOO_LARGE_RESPONSE: &[u8] =
    b"HTTP/1.1 413 Payload Too Large\r\nConnection: close\r\n\r\n";

#[test]
fn oversized_content_length_gets_413_without_allocating_the_body() {
    let server = spawn_server();

    let mut stream = TcpStream::connect(server.addr).expect("open test connection");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set read timeout");

    // A tiny request declaring a 200 MB body -- far past any reasonable
    // cap (the fix below uses 10 MiB), but deliberately not an
    // astronomical value: allocating and zeroing a genuinely huge buffer
    // (or the allocator aborting the whole process on failure) is exactly
    // the failure mode this gate exists to prevent, so the test itself
    // must not risk triggering it against the current, unfixed code.
    let request =
        b"POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: 200000000\r\n\r\n";
    stream
        .write_all(request)
        .expect("write request with oversized declared Content-Length");

    let mut actual = vec![0u8; PAYLOAD_TOO_LARGE_RESPONSE.len()];
    stream
        .read_exact(&mut actual)
        .expect("read 413 response before the connection closes");
    assert_eq!(
        actual, PAYLOAD_TOO_LARGE_RESPONSE,
        "expected a byte-exact 413 Payload Too Large response"
    );

    // Same RST-vs-FIN nuance as the Phase 1 Slowloris gate: the server
    // closes without having read the (never-sent) declared body, so either
    // outcome means "closed".
    let mut trailing = [0u8; 1];
    match stream.read(&mut trailing) {
        Ok(0) => {}
        Ok(n) => panic!("expected the connection to be closed, but read {n} more byte(s)"),
        Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {}
        Err(e) => panic!("expected EOF or a connection reset, got: {e}"),
    }
}
