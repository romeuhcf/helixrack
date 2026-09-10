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
//!
//! Since Phase 4 (see `PLAN.md`, Phase 4), this module also owns the
//! connection's keep-alive lifecycle: [`handle`] counts requests served
//! against `max_keepalive`, adding `Connection: close` to (and closing the
//! connection right after) the final one it will answer; and it applies
//! `keep_alive_timeout` to the one read that's waiting for a brand-new
//! request to start arriving on an otherwise-idle connection. See Phase 4's
//! "Architecture note" for why the engine, not `Handler`, owns the
//! `Connection` header on every response.

use std::io;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::handler::{Handler, HandlerResponse, ParsedRequest, ResponseBody};
use crate::ConnectionCounter;

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

/// RAII guard around one request's [`ConnectionCounter::record_request_start`]/
/// [`record_request_finish`](ConnectionCounter::record_request_finish) pair
/// (Phase 8, `PLAN.md`, Phase 8/RNF05). `handle`'s loop below constructs one
/// as soon as a request's headers are fully parsed -- *before* buffering its
/// body, not just before `Handler::call` (a CodeRabbit finding on the
/// original PR caught that a slow client's still-arriving body, or the
/// 413/400 rejection responses generated while buffering it, weren't being
/// counted as in-flight at all) -- and explicitly `drop`s it right after the
/// response is fully written. The RAII part is what guarantees
/// `record_request_finish` still runs on every early-return-via-`?` path in
/// between (body-buffering I/O errors, `ensure_framing`, either
/// `write_all`/`write_body` call failing), not just the success path;
/// without it, a failure partway through a request already counted as
/// in-flight would leak it as permanently in-flight, and a graceful
/// shutdown's `ConnectionCounter::drain` would wait out its full
/// `grace_period` for a connection that already died.
struct InFlightGuard<'a> {
    connections: &'a ConnectionCounter,
}

impl<'a> InFlightGuard<'a> {
    fn new(connections: &'a ConnectionCounter) -> Self {
        connections.record_request_start();
        Self { connections }
    }
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.connections.record_request_finish();
    }
}

/// Serves one accepted TCP connection: reads and parses HTTP/1.1 requests
/// off the wire with `httparse`, calls `handler.call` synchronously for
/// each one, and writes the serialized response, for as long as the client
/// keeps the connection open (HTTP/1.1 keep-alive).
///
/// `max_keepalive` bounds how many requests this connection will be
/// answered: on the `max_keepalive`-th request's response, [`handle`] adds
/// `Connection: close` (after stripping any `Connection` header `handler`
/// itself set -- see this module's top doc comment and `PLAN.md`'s Phase 4
/// "Architecture note") and returns right after writing it, without looping
/// back to read another request. Every other response gets no `Connection`
/// header at all -- HTTP/1.1 connections are persistent by default (RFC
/// 7230), so there is nothing to say on the normal path.
///
/// `keep_alive_timeout` bounds how long this connection may sit idle -- no
/// bytes of a new request buffered yet -- before [`handle`] closes it with
/// no response (there is nothing to answer; the client isn't mid-request).
/// It applies *only* to that specific bottom-of-loop read: a read that's
/// continuing an already-in-progress request (partial headers already
/// buffered, the `MAX_BUF_CAPACITY` growth path, or a mid-body read) is
/// unaffected. Once a client has sent at least one byte of a new request,
/// there is currently no time bound on how long it may then go silent --
/// `MAX_BUF_CAPACITY`/`MAX_BODY_CAPACITY` only bound how much such a client
/// can make this connection *buffer* before being rejected, not how long it
/// can take to send it (a client that sends one byte and then nothing would
/// never trip either cap). A real, acknowledged gap flagged by this phase's
/// safety review, deliberately not fixed here: PRD.md's `--keep-alive-
/// timeout` is specified as bounding *idle* connections, not slow ones, so
/// closing that gap is scoped as its own future hardening item, not smuggled
/// into this flag's meaning.
///
/// Returns once the client closes the connection (EOF), the connection is
/// closed by this function (`max_keepalive` reached, or `keep_alive_timeout`
/// elapsed on an idle read), or a read/write/parse error occurs. The caller
/// (the `serve` accept loop) runs this per connection independently, so one
/// connection's error doesn't affect others.
///
/// `connections` (Phase 8, `PLAN.md`, Phase 8/RNF05): each request's
/// handling is bracketed with
/// [`ConnectionCounter::record_request_start`]/[`record_request_finish`]
/// (just before `handler.call`, just after the response is fully written)
/// so a graceful shutdown's drain step can wait for in-flight *requests*,
/// not open connections -- see that type's own doc comment for why that
/// distinction matters (an idle keep-alive connection between requests is
/// *not* something `drain` needs to wait for).
pub(crate) async fn handle(
    mut socket: TcpStream,
    handler: Rc<dyn Handler>,
    max_keepalive: usize,
    keep_alive_timeout: Duration,
    connections: Arc<ConnectionCounter>,
) -> io::Result<()> {
    let mut buf = vec![0u8; INITIAL_BUF_CAPACITY];
    let mut filled = 0usize;
    let mut requests_served = 0usize;

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

            // RAII, constructed as soon as headers are complete -- *before*
            // body buffering, not just before `Handler::call` -- so a
            // graceful shutdown's `drain` also waits out a request whose
            // body is still trickling in over the wire, and the 413/400
            // rejection responses just below (both real responses that must
            // finish writing before this connection is safe to drop), not
            // only the `Handler::call` path. A CodeRabbit finding on this
            // PR caught the original placement (just before `Handler::call`,
            // after body buffering) as a real, if narrow, gap: `drain`
            // could observe zero in-flight requests and let a shutdown tear
            // this connection down while still waiting on a slow client's
            // body bytes. See this loop's other `InFlightGuard` comment,
            // further down, for why RAII (not a manual call at the bottom)
            // still matters here regardless of where construction starts.
            let in_flight = InFlightGuard::new(&connections);

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

            // Captured as an owned `bool` now, before `handler.call` below
            // (which only needs a borrow of `parsed_request` for its own
            // duration) -- read again at the body-write step further down,
            // where a borrow of `parsed_request.method` would otherwise
            // still be valid too (`buf` isn't mutated in between), but an
            // owned `bool` makes that independent of that fact staying true
            // if this function is ever restructured.
            let is_head = parsed_request.method.eq_ignore_ascii_case("HEAD");

            // `in_flight` (constructed above, right after headers were
            // parsed) is RAII, not a manual `record_request_finish()` call
            // at the bottom of this block: several steps between here and
            // the response being fully written can return early via `?`
            // (`ensure_framing`, both `write_all`/`write_body` calls) --
            // without a guard, any of those would leak this request as
            // permanently "in flight", starving `ConnectionCounter::drain`
            // of ever seeing it finish. Explicitly `drop`ped right after
            // the response write completes (success or error), not left to
            // the end of this loop iteration's scope, so the "in flight"
            // window is exactly "headers parsed through response written",
            // matching what `drain` actually needs to wait for.
            let mut response = handler.call(&parsed_request);

            // The engine is the sole source of truth for the `Connection`
            // header on every response, not `Handler` -- see this module's
            // top doc comment and `PLAN.md`'s Phase 4 "Architecture note".
            // Strip whatever `handler` set (case- *and* incidental-
            // whitespace-insensitively -- a Rack app's header Hash reaches
            // here with no trimming/normalization applied anywhere upstream,
            // so a stray-whitespace key like `" Connection"` would otherwise
            // survive this filter and land on the wire alongside the
            // engine's own line below) before deciding, just below, whether
            // *this* response is the one that gets `Connection: close`.
            response
                .headers
                .retain(|(name, _)| !name.trim().eq_ignore_ascii_case("connection"));

            // A response with a body but neither Content-Length nor
            // Transfer-Encoding leaves the client with no way to know where
            // the body ends -- harmless on the final (Connection: close)
            // response, since EOF marks the end, but on every other one the
            // client keeps waiting for more body bytes while this loop
            // waits for the next request on the same socket: a hang, only
            // ever broken by `keep_alive_timeout`. Found by this phase's
            // code review, not by any existing gate (every fixture app used
            // so far always set Content-Length itself).
            ensure_framing(&mut response).await?;

            requests_served += 1;
            let is_last_allowed_request = requests_served >= max_keepalive;
            if is_last_allowed_request {
                response
                    .headers
                    .push(("Connection".to_string(), "close".to_string()));
            }

            let head = serialize_head(&response);
            // HTTP/1.1 (RFC 9110 section 9.3.2): a response to `HEAD` must
            // carry the same header fields a `GET` would (so `ensure_framing`
            // above still ran, and `Content-Length` above still reflects the
            // body's real length) but must never send the body itself.
            // Found by this phase's own safety review, corroborated
            // independently by CodeRabbit on the same PR: before this,
            // `write_body` ran unconditionally, so a `HEAD` response with any
            // non-empty body (previously only reachable via a handler that
            // deliberately returned one for `HEAD`; now also reachable via
            // `ext/helix_rack`'s Phase 7 fault-containment `500` body) wrote
            // bytes the client doesn't expect there, which the client then
            // misreads as the start of the *next* response on the same
            // persistent connection -- silent desync, not a clean error.
            if is_head {
                socket.write_all(&head).await?;
            } else {
                write_response(&mut socket, head, &response.body).await?;
            }
            drop(in_flight);

            if is_last_allowed_request {
                // This was the max_keepalive-th request: the response just
                // sent already told the client this connection is closing
                // (`Connection: close` above) -- close it now rather than
                // looping back to read another request (or even processing
                // any already-pipelined bytes after this one; the client was
                // told not to expect more answers on this socket).
                return Ok(());
            }

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

        // `keep_alive_timeout` applies only here, and only when nothing of a
        // new request has been buffered yet (`filled == 0`) -- a genuinely
        // idle connection, not one mid-request. See `handle`'s doc comment.
        let read = if filled == 0 {
            match tokio::time::timeout(keep_alive_timeout, socket.read(&mut buf[filled..])).await
            {
                Ok(result) => result?,
                Err(_elapsed) => {
                    // Idle timeout: nothing to answer (the client isn't
                    // mid-request), so just close.
                    return Ok(());
                }
            }
        } else {
            socket.read(&mut buf[filled..]).await?
        };
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

/// Ensures `response` has valid HTTP/1.1 message framing before
/// [`serialize_head`]/[`write_body`] write it -- see the call site in
/// [`handle`] for why a response with a body but neither `Content-Length`
/// nor `Transfer-Encoding` is a real hang hazard on a persistent connection,
/// not just a cosmetic gap.
///
/// Only ever injects `Content-Length`, never `Transfer-Encoding`/chunked --
/// this engine doesn't speak chunked encoding (PRD.md's scope). Both
/// `ResponseBody` variants always have a knowable exact length before any
/// bytes are written, so this is always possible when framing is missing.
/// Does *not* second-guess a `Content-Length` the handler already set (even
/// if it were wrong) -- only fills the gap when neither header is present
/// at all, consistent with "the handler is responsible for every header it
/// sets" everywhere else in this module.
async fn ensure_framing(response: &mut HandlerResponse) -> io::Result<()> {
    let has_framing = response.headers.iter().any(|(name, _)| {
        let name = name.trim();
        name.eq_ignore_ascii_case("content-length") || name.eq_ignore_ascii_case("transfer-encoding")
    });
    if has_framing {
        return Ok(());
    }

    let body_len: u64 = match &response.body {
        ResponseBody::InMemory(bytes) => bytes.len() as u64,
        ResponseBody::Spooled(file) => {
            // A duplicated fd (same underlying file, own seek position --
            // `write_body` relies on the same `try_clone` pattern), so
            // reading its metadata doesn't disturb whatever position the
            // original `File` is left at.
            let duplicated = file.try_clone()?;
            tokio::fs::File::from_std(duplicated).metadata().await?.len()
        }
    };
    response
        .headers
        .push(("Content-Length".to_string(), body_len.to_string()));
    Ok(())
}

/// Serializes a [`HandlerResponse`]'s front matter into real HTTP/1.1 bytes:
/// a status line with a reason phrase, the given headers verbatim (by the
/// time this runs, [`ensure_framing`] has already guaranteed valid framing
/// -- this function itself injects nothing), then a blank line. The body is
/// a separate step ([`write_body`]) since, as of Phase 3, it isn't always
/// already a byte slice sitting in memory to append here.
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
/// Writes a response's head and (non-`HEAD`-suppressed) body to `socket`.
///
/// Gated behind the opt-in `combine-write` Cargo feature (off by default),
/// added as a follow-up to the `TCP_NODELAY` fix (see `PLAN.md`'s Phase 13
/// "Post-Phase-13 follow-up" notes): with Nagle disabled, two back-to-back
/// `write_all` calls (head, then body) typically go out as two separate TCP
/// segments instead of coalescing, so combining them into one `write_all`
/// is a real, if smaller, lever on top of that fix -- one syscall and one
/// segment instead of two, for the common case. Built as a feature, not an
/// unconditional change, specifically so its effect can be measured against
/// the `combine-write`-off baseline independently of the `TCP_NODELAY` fix,
/// rather than assumed.
///
/// Only [`ResponseBody::InMemory`] can be combined this way: concatenating
/// head and body into one buffer before writing is exactly the small-body
/// case this is for. [`ResponseBody::Spooled`] keeps its own
/// `write_head` + streamed-chunk path unconditionally -- reading a
/// potentially multi-hundred-MB spooled body into memory just to concatenate
/// it with the head would defeat the entire reason [`ResponseBody::Spooled`]
/// exists.
#[cfg(feature = "combine-write")]
async fn write_response(
    socket: &mut TcpStream,
    head: Vec<u8>,
    body: &ResponseBody,
) -> io::Result<()> {
    match body {
        ResponseBody::InMemory(bytes) => {
            let mut combined = head;
            combined.extend_from_slice(bytes);
            socket.write_all(&combined).await
        }
        ResponseBody::Spooled(_) => {
            socket.write_all(&head).await?;
            write_body(socket, body).await
        }
    }
}

/// The default (`combine-write` feature off) path: head and body written as
/// two separate `write_all` calls, unchanged since Phase 1/2. See the
/// `combine-write`-gated [`write_response`] above for the alternative this
/// is being measured against.
#[cfg(not(feature = "combine-write"))]
async fn write_response(
    socket: &mut TcpStream,
    head: Vec<u8>,
    body: &ResponseBody,
) -> io::Result<()> {
    socket.write_all(&head).await?;
    write_body(socket, body).await
}

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
