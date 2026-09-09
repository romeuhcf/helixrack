//! Phase 1 gate, bounded-buffer half (see `PLAN.md`, Phase 1 "Gate"):
//!
//! > Bounded read buffer: a client that sends header bytes without ever
//! > completing a request (no terminating blank line) must not grow the
//! > server's per-connection buffer without limit. Past a fixed cap, the
//! > server responds `431 Request Header Fields Too Large` and closes the
//! > connection.
//!
//! No `sleep`: the client writes past the cap in one shot, then blocks on a
//! read with a hang-guard timeout, so the test only proceeds once the
//! server's real response (or a real error) arrives.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

mod support;
use support::spawn_server;

const TOO_LARGE_RESPONSE: &[u8] =
    b"HTTP/1.1 431 Request Header Fields Too Large\r\nConnection: close\r\n\r\n";

#[test]
fn oversized_request_gets_431_and_connection_close() {
    let server = spawn_server();

    let mut stream = TcpStream::connect(server.addr).expect("open test connection");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set read timeout");

    // A request line plus a single header value large enough to blow past
    // any reasonable cap, with no terminating blank line -- the server must
    // never see a complete request here.
    let mut oversized = b"GET / HTTP/1.1\r\nX-Filler: ".to_vec();
    oversized.extend(std::iter::repeat_n(b'a', 200 * 1024));
    stream
        .write_all(&oversized)
        .expect("write oversized, never-completing request");

    let mut actual = vec![0u8; TOO_LARGE_RESPONSE.len()];
    stream
        .read_exact(&mut actual)
        .expect("read 431 response before the connection closes");
    assert_eq!(
        actual, TOO_LARGE_RESPONSE,
        "expected a byte-exact 431 Request Header Fields Too Large response"
    );

    // The server closes right after writing the 431 response, while the
    // client's oversized write left unread bytes in the kernel receive
    // buffer -- that makes the OS send RST rather than a clean FIN, so the
    // client sees either a clean EOF (`Ok(0)`) or a reset error. Both mean
    // "closed"; only a further successful read of real data would be wrong.
    let mut trailing = [0u8; 1];
    match stream.read(&mut trailing) {
        Ok(0) => {}
        Ok(n) => panic!("expected the connection to be closed, but read {n} more byte(s)"),
        Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {}
        Err(e) => panic!("expected EOF or a connection reset, got: {e}"),
    }
}
