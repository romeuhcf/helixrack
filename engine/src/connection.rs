//! Per-connection request/response loop for the HelixRack engine.
//!
//! Since Phase 2 (see `PLAN.md`), this module parses each request off the
//! wire with `httparse`, builds a zero-copy [`ParsedRequest`], calls the
//! caller-supplied [`Handler`] synchronously, and serializes whatever
//! [`HandlerResponse`] comes back into real HTTP/1.1 response bytes.
//! Phase 1's bounded-read-buffer / 431 behavior is unchanged.
//!
//! Since Phase 3 (see `PLAN.md`, Phase 3), the response body is written in
//! two possible ways depending on [`ResponseBody`]: an [`ResponseBody::InMemory`]
//! body is written in one `write_all`, same as always; a
//! [`ResponseBody::Spooled`] one is streamed back out to the socket in
//! bounded chunks (see [`write_body`]) instead of being read into memory
//! whole. The status-line-and-headers front matter (see [`serialize_head`])
//! doesn't change either way.

use std::io;
use std::rc::Rc;

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::handler::{Handler, HandlerResponse, ParsedRequest, ResponseBody};

/// Initial size of the per-connection read buffer. Doubled if a request's
/// headers don't fit yet -- Phase 1's fixtures are all well under this.
const INITIAL_BUF_CAPACITY: usize = 8 * 1024;

/// Hard cap on the per-connection read buffer *while headers are still
/// incomplete*. Without this, a client that sends header bytes without ever
/// completing a request (no terminating blank line) grows the buffer
/// without limit -- a Slowloris-style memory exhaustion vector. 64 KiB is
/// generous for legitimate headers (well past nginx's 8 KiB default) while
/// keeping worst-case per-connection memory bounded.
///
/// This cap does not apply once headers are complete and a `Content-Length`
/// body remains to be read (see [`handle`]'s body-buffering step below) --
/// that's `MAX_BODY_CAPACITY`'s job, a separate, larger cap since legitimate
/// bodies are commonly bigger than headers.
const MAX_BUF_CAPACITY: usize = 64 * 1024;

/// Sent, then the connection is closed, when a request would need a header
/// buffer larger than `MAX_BUF_CAPACITY` to complete.
const REQUEST_HEADER_FIELDS_TOO_LARGE: &[u8] =
    b"HTTP/1.1 431 Request Header Fields Too Large\r\nConnection: close\r\n\r\n";

/// Hard cap on a declared `Content-Length`. Without this, a single request
/// with one oversized header value makes the server attempt to allocate a
/// buffer of an entirely attacker-controlled size -- worse than the
/// header-only Slowloris gap `MAX_BUF_CAPACITY` guards against, since it
/// takes only one request, not a sustained slow drip. 10 MiB is generous
/// for typical API/form payloads while keeping worst-case per-connection
/// memory bounded; Phase 3's response-body streaming is a separate concern
/// (outbound, not this inbound cap) and doesn't cover this.
const MAX_BODY_CAPACITY: usize = 10 * 1024 * 1024;

/// Sent, then the connection is closed, when a declared `Content-Length`
/// exceeds `MAX_BODY_CAPACITY`. Sent before any attempt to allocate for or
/// read the body.
const PAYLOAD_TOO_LARGE_RESPONSE: &[u8] =
    b"HTTP/1.1 413 Payload Too Large\r\nConnection: close\r\n\r\n";

/// Sent, then the connection is closed, when a header value isn't valid
/// UTF-8. `ParsedRequest` exposes header values as `&str` (zero-copy, no
/// per-header allocation), so a value that isn't valid UTF-8 can't be
/// represented -- rather than silently lossy-converting it (which would
/// hide wire bytes from the handler), the connection is rejected.
const BAD_REQUEST_RESPONSE: &[u8] = b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n";

/// Maximum number of headers `httparse` parses per request.
const MAX_HEADERS: usize = 64;

/// Serves one accepted TCP connection: reads and parses HTTP/1.1 requests
/// off the wire with `httparse`, calls `handler.call` synchronously for
/// each one, and writes the serialized response, for as long as the client
/// keeps the connection open (HTTP/1.1 keep-alive).
///
/// Returns once the client closes the connection (EOF) or a read/write/
/// parse error occurs. The caller (the `serve` accept loop) runs this per
/// connection independently, so one connection's error doesn't affect
/// others.
pub(crate) async fn handle(mut socket: TcpStream, handler: Rc<dyn Handler>) -> io::Result<()> {
    let mut buf = vec![0u8; INITIAL_BUF_CAPACITY];
    let mut filled = 0usize;

    loop {
        // Parse as many complete requests as are already buffered before
        // reading more off the wire -- handles requests that arrive
        // pipelined in the same TCP segment.
        loop {
            // First pass: parse just the headers to learn where they end
            // (`consumed`) and how long the body is (`body_len`). Both are
            // plain `usize`s, so no borrow of `buf` survives past this
            // block -- required, since the body-buffering step right after
            // needs to mutate (resize/read into) `buf`.
            let (consumed, body_len) = {
                let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
                let mut request = httparse::Request::new(&mut headers);

                match request
                    .parse(&buf[..filled])
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                {
                    httparse::Status::Complete(consumed) => {
                        let body_len = content_length(request.headers)?;
                        (consumed, body_len)
                    }
                    httparse::Status::Partial => break,
                }
            };

            // Reject an oversized declared body *before* attempting to
            // allocate for it -- see `MAX_BODY_CAPACITY`'s doc comment.
            if body_len > MAX_BODY_CAPACITY {
                socket.write_all(PAYLOAD_TOO_LARGE_RESPONSE).await?;
                return Ok(());
            }

            // Make sure the full declared body is buffered before parsing
            // again below. Deliberately not subject to `MAX_BUF_CAPACITY`:
            // see that constant's doc comment.
            let body_end = consumed + body_len;
            if buf.len() < body_end {
                buf.resize(body_end, 0);
            }
            while filled < body_end {
                let read = socket.read(&mut buf[filled..body_end]).await?;
                if read == 0 {
                    // EOF mid-body: the client went away before finishing
                    // the request it declared. Nothing sensible to
                    // respond with.
                    return Ok(());
                }
                filled += read;
            }

            // Second pass: `buf`'s contents haven't changed since the read
            // loop above stopped (only grown/filled, never shifted), so
            // this reparses the exact same request -- it exists only to
            // get borrows of `buf` that are still valid now (the first
            // pass's borrows ended when its block did, before `buf` was
            // possibly resized).
            let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
            let mut request = httparse::Request::new(&mut headers);
            request
                .parse(&buf[..filled])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            let full_path = request.path.unwrap_or_default();
            let (path, query) = match full_path.split_once('?') {
                Some((path, query)) => (path, query),
                None => (full_path, ""),
            };

            let mut parsed_headers = Vec::with_capacity(request.headers.len());
            for header in request.headers.iter() {
                let value = match std::str::from_utf8(header.value) {
                    Ok(value) => value,
                    Err(_) => {
                        socket.write_all(BAD_REQUEST_RESPONSE).await?;
                        return Ok(());
                    }
                };
                parsed_headers.push((header.name, value));
            }

            let parsed_request = ParsedRequest {
                method: request.method.unwrap_or_default(),
                path,
                query,
                headers: parsed_headers,
                body: &buf[consumed..body_end],
            };

            let response = handler.call(&parsed_request);
            let head = serialize_head(&response);
            socket.write_all(&head).await?;
            write_body(&mut socket, &response.body).await?;

            // Shift any bytes after this request (start of the next
            // pipelined request, if any) down to the front, without
            // touching the buffer's allocated capacity.
            buf.copy_within(body_end..filled, 0);
            filled -= body_end;
        }

        // Checked against `filled` (bytes buffered for THIS incomplete
        // header parse), not `buf.len()` (the buffer's allocated capacity,
        // which a prior request's body may have grown well past
        // `MAX_BUF_CAPACITY` and which never shrinks back down). Comparing
        // against `buf.len()` instead would let a later request's headers
        // balloon up to that leftover capacity before being rejected,
        // silently defeating this cap for the rest of the connection.
        if filled >= MAX_BUF_CAPACITY {
            socket.write_all(REQUEST_HEADER_FIELDS_TOO_LARGE).await?;
            return Ok(());
        }
        if filled == buf.len() {
            // The buffered partial request doesn't fit yet -- grow (capped)
            // and keep reading rather than failing a request that's merely
            // large. `filled < MAX_BUF_CAPACITY` here (the check above
            // returned otherwise), so `buf.len() == filled` is too, and
            // this never shrinks the buffer.
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

/// Extracts `Content-Length` from parsed headers, defaulting to `0` (no
/// body) when absent.
///
/// A malformed value (non-numeric, or more than one differing value) is
/// treated as a parse error, the same way `httparse` treats other
/// malformed input in this module -- the connection is dropped rather than
/// guessing which value to trust.
fn content_length(headers: &[httparse::Header<'_>]) -> io::Result<usize> {
    let mut found: Option<usize> = None;
    for header in headers {
        if !header.name.eq_ignore_ascii_case("content-length") {
            continue;
        }
        let value = std::str::from_utf8(header.value)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let parsed: usize = value.trim().parse().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "malformed Content-Length header")
        })?;
        match found {
            None => found = Some(parsed),
            Some(existing) if existing == parsed => {}
            Some(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "conflicting Content-Length headers",
                ))
            }
        }
    }
    Ok(found.unwrap_or(0))
}

/// The reason phrase for a status line. Rack apps can return arbitrary
/// status codes, so this covers the common ones and falls back to a
/// placeholder rather than panicking or omitting the phrase (the HTTP/1.1
/// status line grammar requires one, even if servers rarely check it).
fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        204 => "No Content",
        301 => "Moved Permanently",
        302 => "Found",
        304 => "Not Modified",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        413 => "Payload Too Large",
        414 => "URI Too Long",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "Unknown Status",
    }
}

/// Serializes a [`HandlerResponse`]'s front matter into real HTTP/1.1 bytes:
/// a status line with a reason phrase, the given headers verbatim (no
/// injected `Content-Length` or similar -- the handler is responsible for
/// every header it wants sent), then a blank line. The body is a separate
/// step ([`write_body`]) since, as of Phase 3, it isn't always already a
/// byte slice sitting in memory to append here.
fn serialize_head(response: &HandlerResponse) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(128);
    bytes.extend_from_slice(
        format!(
            "HTTP/1.1 {} {}\r\n",
            response.status,
            reason_phrase(response.status)
        )
        .as_bytes(),
    );
    for (name, value) in &response.headers {
        bytes.extend_from_slice(name.as_bytes());
        bytes.extend_from_slice(b": ");
        bytes.extend_from_slice(value.as_bytes());
        bytes.extend_from_slice(b"\r\n");
    }
    bytes.extend_from_slice(b"\r\n");
    bytes
}

/// Chunk size used when streaming a [`ResponseBody::Spooled`] file back out
/// to the socket. 128 KiB: this whole path exists (`PLAN.md`'s Phase 3 gate)
/// to keep server RSS from scaling with body size, so the chunk buffer
/// itself must stay a fixed, small cost regardless of how large the spooled
/// file is -- 128 KiB is a rounding error next to `MAX_BODY_CAPACITY`'s 10
/// MiB inbound cap, let alone the multi-hundred-MB bodies this path is for.
/// It's also comfortably above typical filesystem/socket buffer and MTU
/// sizes (so the loop isn't dominated by per-chunk read()/write() syscall
/// and task-wakeup overhead the way a tiny e.g. 4 KiB chunk would be),
/// without being so large that one chunk read starves the event loop for
/// a noticeable stretch on a single-threaded runtime (PRD.md RNF01) --
/// 64 KiB-256 KiB is the generally reasonable range for this tradeoff, and
/// 128 KiB is the middle of it.
const SPOOLED_BODY_CHUNK_SIZE: usize = 128 * 1024;

/// Writes a [`HandlerResponse`]'s body to `socket`: [`ResponseBody::InMemory`]
/// in one `write_all` (unchanged Phase 1/2 behavior); [`ResponseBody::Spooled`]
/// read back out of its file and written in [`SPOOLED_BODY_CHUNK_SIZE`]-sized
/// chunks via async tokio file I/O, never reading the whole spooled file into
/// memory at once.
async fn write_body(socket: &mut TcpStream, body: &ResponseBody) -> io::Result<()> {
    match body {
        ResponseBody::InMemory(bytes) => socket.write_all(bytes).await,
        ResponseBody::Spooled(file) => {
            // `tokio::fs::File` doesn't wrap a borrowed `&std::fs::File` --
            // it owns its handle -- so a duplicate OS-level file descriptor
            // (sharing the same underlying file and, notably, seek
            // position) is what lets this read from the file without taking
            // ownership of the `std::fs::File` living in `body`/`response`.
            let duplicated = file.try_clone()?;
            let mut spooled = tokio::fs::File::from_std(duplicated);
            // `ResponseBody::Spooled`'s doc comment makes no promise about
            // the incoming file position (e.g. a freshly-written file's
            // cursor sits at EOF, not the start) -- rewind explicitly rather
            // than trust the caller.
            spooled.seek(io::SeekFrom::Start(0)).await?;

            let mut chunk = vec![0u8; SPOOLED_BODY_CHUNK_SIZE];
            loop {
                let read = spooled.read(&mut chunk).await?;
                if read == 0 {
                    return Ok(());
                }
                socket.write_all(&chunk[..read]).await?;
            }
        }
    }
}
