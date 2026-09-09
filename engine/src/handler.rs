//! Phase 2 plumbing (see `PLAN.md`, Phase 2): the pluggable [`Handler`]
//! trait that lets a caller supply the response for each parsed request,
//! replacing Phase 1's single hardcoded response.
//!
//! [`ParsedRequest`] borrows its strings and body out of the connection's
//! read buffer (zero-copy, per PRD.md RF01) -- it does not outlive the call
//! to [`Handler::call`] that receives it. [`HandlerResponse`] is owned:
//! by the time a handler returns, the connection may reuse or shift its
//! read buffer for the next pipelined request, so the response can't keep
//! borrowing it.

/// One HTTP/1.1 request, parsed zero-copy out of a connection's read
/// buffer.
///
/// Every string here is a borrowed slice of the bytes that arrived on the
/// wire -- no per-request `String` allocation for method, path, query
/// string, or header name/value.
#[derive(Debug)]
pub struct ParsedRequest<'req> {
    /// The HTTP method, e.g. `"GET"`.
    pub method: &'req str,
    /// The request path, without the query string, e.g. `"/foo"`.
    pub path: &'req str,
    /// The query string, without the leading `?`. Empty (not absent) when
    /// the request has no query string -- matching Rack's `QUERY_STRING`
    /// convention (PRD.md RF02: the key is always present, empty if
    /// unused).
    pub query: &'req str,
    /// Header name/value pairs, in the order they appeared on the wire.
    /// Repeated headers are kept as separate entries, not merged.
    pub headers: Vec<(&'req str, &'req str)>,
    /// The request body. Empty when the request had no body (no
    /// `Content-Length`, or `Content-Length: 0`).
    pub body: &'req [u8],
}

/// A handler-produced HTTP response.
///
/// Owned, unlike [`ParsedRequest`]: it may still be alive after the
/// connection has moved its read buffer around for the next pipelined
/// request, so it can't borrow from that buffer.
#[derive(Debug)]
pub struct HandlerResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: ResponseBody,
}

/// A [`HandlerResponse`]'s body: either fully in memory, or spooled to a
/// file and streamed back out (see `PLAN.md`'s Phase 3 "Architecture
/// decision" for why a tempfile rather than raw-fd writes or pull-based
/// `Enumerator`/`Fiber` iteration -- both considered and rejected there).
///
/// No `Clone`/`Copy`: `std::fs::File` isn't `Clone` (it wraps a raw OS file
/// descriptor, not cheaply duplicable data), and nothing in this crate has
/// needed to clone a `HandlerResponse` since Phase 1 -- dropped along with
/// this enum's addition rather than worked around.
#[derive(Debug)]
pub enum ResponseBody {
    /// The whole body already in RAM, written to the socket in one
    /// `write_all` call -- Phase 1/2's only case, and unchanged behavior
    /// for it now that it's a named variant instead of the field's whole
    /// type.
    InMemory(Vec<u8>),
    /// The body's bytes live in this file. `connection::handle`'s
    /// body-writing step does not assume any particular file position on
    /// entry -- it seeks to the start explicitly before reading -- so a
    /// caller may hand over a file it just finished writing (cursor at EOF)
    /// without rewinding it first. Read back out and written to the socket
    /// in bounded chunks via async tokio file I/O, never loaded into memory
    /// all at once (that would defeat the entire point of this variant --
    /// see `PLAN.md`'s Phase 3 gate: bounded server RSS regardless of body
    /// size).
    Spooled(std::fs::File),
}

/// Produces the response for one parsed request.
///
/// Called **synchronously** from the connection's Tokio task -- see
/// `PLAN.md`'s Phase 2 "Architecture note" for why: the real Phase 2
/// implementation runs the whole `current_thread` runtime on the same OS
/// thread that already holds Ruby's GVL for the call it's servicing, so
/// there is nothing to `.await` here and no GVL acquire/release logic
/// belongs in this trait.
///
/// No longer `Send + Sync` (see `PLAN.md`'s Phase 2 "Architecture note" on
/// `Rc`, not `Arc`): a magnus-backed implementation holds a Ruby `Value`
/// (the loaded app), and `magnus::Value` is not `Send`/`Sync` -- Ruby values
/// can't cross threads without the VM's involvement. Since this whole engine
/// only ever runs on one OS thread (PRD.md RNF01), a single handler instance
/// is shared across every connection's task via `Rc`, not `Arc`.
pub trait Handler {
    fn call(&self, req: &ParsedRequest<'_>) -> HandlerResponse;
}
