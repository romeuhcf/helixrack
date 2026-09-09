//! Per-connection request/response loop for the Phase 1 engine (see
//! `PLAN.md`, Phase 1).
//!
//! Phase 1 has no routing yet (PLAN.md: "a hardcoded static response"), so
//! every successfully parsed request gets the same fixed response. Parsing
//! still has to be real: each request is read off the wire and parsed with
//! `httparse` directly out of the read buffer (no per-header `String`
//! allocation), handling requests that arrive split across more than one
//! TCP segment.

use std::io;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// The fixed HTTP/1.1 200 response every successfully parsed Phase 1
/// request gets. Must stay byte-for-byte identical to the gate's
/// `EXPECTED_RESPONSE` constant in `engine/tests/support/mod.rs`.
const RESPONSE: &[u8] =
    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nOK";

/// Initial size of the per-connection read buffer. Doubled if a request's
/// headers don't fit yet -- Phase 1's fixtures are all well under this.
const INITIAL_BUF_CAPACITY: usize = 8 * 1024;

/// Hard cap on the per-connection read buffer. Without this, a client that
/// sends header bytes without ever completing a request (no terminating
/// blank line) grows the buffer without limit -- a Slowloris-style memory
/// exhaustion vector. 64 KiB is generous for legitimate headers (well past
/// nginx's 8 KiB default) while keeping worst-case per-connection memory
/// bounded.
const MAX_BUF_CAPACITY: usize = 64 * 1024;

/// Sent, then the connection is closed, when a request would need a buffer
/// larger than `MAX_BUF_CAPACITY` to complete.
const REQUEST_HEADER_FIELDS_TOO_LARGE: &[u8] =
    b"HTTP/1.1 431 Request Header Fields Too Large\r\nConnection: close\r\n\r\n";

/// Maximum number of headers `httparse` parses per request.
const MAX_HEADERS: usize = 64;

/// Serves one accepted TCP connection: reads and parses HTTP/1.1 requests
/// off the wire with `httparse`, replying with the fixed Phase 1 response
/// to each, for as long as the client keeps the connection open (HTTP/1.1
/// keep-alive).
///
/// Returns once the client closes the connection (EOF) or a read/write
/// error occurs. The caller (the `serve` accept loop) runs this per
/// connection independently, so one connection's error doesn't affect
/// others.
pub(crate) async fn handle(mut socket: TcpStream) -> io::Result<()> {
    let mut buf = vec![0u8; INITIAL_BUF_CAPACITY];
    let mut filled = 0usize;

    loop {
        // Parse as many complete requests as are already buffered before
        // reading more off the wire -- handles requests that arrive
        // pipelined in the same TCP segment.
        loop {
            let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
            let mut request = httparse::Request::new(&mut headers);

            let parse_result = request
                .parse(&buf[..filled])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            match parse_result {
                httparse::Status::Complete(consumed) => {
                    socket.write_all(RESPONSE).await?;
                    // Shift any bytes after this request (start of the
                    // next pipelined request, if any) down to the front,
                    // without touching the buffer's allocated capacity.
                    buf.copy_within(consumed..filled, 0);
                    filled -= consumed;
                }
                httparse::Status::Partial => break,
            }
        }

        if filled == buf.len() {
            if buf.len() >= MAX_BUF_CAPACITY {
                // The request still isn't complete at the cap -- reject it
                // rather than growing the buffer without limit.
                socket.write_all(REQUEST_HEADER_FIELDS_TOO_LARGE).await?;
                return Ok(());
            }
            // The buffered partial request doesn't fit yet -- grow (capped)
            // and keep reading rather than failing a request that's merely
            // large.
            let new_capacity = (buf.len() * 2).min(MAX_BUF_CAPACITY);
            buf.resize(new_capacity, 0);
        }

        let read = socket.read(&mut buf[filled..]).await?;
        if read == 0 {
            // EOF: the client closed the connection.
            return Ok(());
        }
        filled += read;
    }
}
