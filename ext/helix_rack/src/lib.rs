//! magnus glue for Phase 2 (see `PLAN.md` at the repo root, Phase 2): binds
//! `HelixRack._serve_native(app, port)`, the native entry point
//! `lib/helix_rack.rb`'s `HelixRack.serve` calls into.
//!
//! Per `PLAN.md`'s Phase 2 "Architecture note", this runs `engine`'s Tokio
//! `current_thread` runtime via `block_on` on the *same* OS thread Ruby
//! called it from, so `RackAppHandler::call` (below) can call into Ruby
//! directly.
//!
//! One deviation from that note, flagged prominently because the note is
//! explicit that Phase 2 needs no GVL acquire/release logic: `_serve_native`
//! releases the GVL (via [`gvl::without_gvl`]) for the whole time it's idle
//! (blocked in `accept()`/epoll wait with no request in flight), reacquiring
//! it (via [`gvl::with_gvl`]) only for each synchronous `Handler::call`. This
//! is *not* Phase 5/RF06's deliverable -- there's no fine-grained release
//! around individual socket reads/writes, no drain/grace-period shutdown
//! (that's Phase 8's job), and no ordering proof of anything; the unblock
//! function it does register only flips a flag `_serve_native` polls every
//! 20ms, not an instant wakeup. It exists only because the Phase 2 gate's
//! test harness (`spec/support/phase2_server_helper.rb`, out of bounds to
//! edit) runs the server on a background `Thread`, drives requests from the
//! main thread concurrently, and kills the server thread between examples --
//! all three needing the GVL to actually move between threads, which a
//! magnus call that never releases it cannot do (it holds the GVL for its
//! *entire* call, starving every other Ruby thread in the process for as
//! long as it runs, including at `Thread#kill`/process-exit time). Verified
//! by direct reproduction: a two-thread script (one thread in
//! `HelixRack.serve`, the main thread polling `TCPSocket.new` for the port
//! to accept) hung indefinitely, unresponsive even to `SIGTERM`, without the
//! GVL release; and, with the release added but no unblock function, the
//! script's own process failed to exit afterwards (Ruby's shutdown couldn't
//! reap the still-blocked server thread).

use std::os::raw::c_void;
use std::panic::{self, AssertUnwindSafe};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use magnus::gc;
use magnus::prelude::*;
use magnus::value::Opaque;
use magnus::{Error, RClass, RHash, RString, Ruby, Value};

use helixrack_engine::{serve, ConnectionCounter, Handler, HandlerResponse, ParsedRequest};

/// Minimal `rb_thread_call_without_gvl`/`rb_thread_call_with_gvl` wrappers
/// (see this module's top doc comment for why they're here). Neither is
/// wrapped by magnus 0.8.2 itself -- both are listed, unimplemented, among
/// the C-API functions enumerated in that crate's `src/lib.rs` -- so this
/// calls the raw `rb-sys` FFI bindings directly.
mod gvl {
    use super::*;

    /// Runs `f`, converts its `extern "C"` callback (`arg`, a raw
    /// `Box<F>` pointer boxed by the caller) back into `F`, calls it, and
    /// returns a raw `Box<R>` pointer for the caller to unbox.
    ///
    /// A panic escaping `f` and unwinding across this `extern "C"`
    /// boundary is undefined behavior (Rust does not guarantee unwinding
    /// through a foreign frame) -- `catch_unwind` stops it here and aborts
    /// the process instead, the same fallback magnus's own callback
    /// trampolines use for exactly this class of problem (see e.g.
    /// `magnus::method`'s `call_handle_error`).
    unsafe extern "C" fn trampoline<F, R>(arg: *mut c_void) -> *mut c_void
    where
        F: FnOnce() -> R,
    {
        let f = Box::from_raw(arg as *mut F);
        match panic::catch_unwind(AssertUnwindSafe(f)) {
            Ok(result) => Box::into_raw(Box::new(result)) as *mut c_void,
            Err(_) => std::process::abort(),
        }
    }

    /// The unblock function registered with `rb_thread_call_without_gvl`:
    /// called (potentially from another Ruby thread, e.g. one running
    /// `Thread#kill` against the thread `without_gvl` is running on, or
    /// during process shutdown) to ask the blocked call to return. An
    /// atomic store is safe to do from wherever this ends up being invoked
    /// from. `arg` is the same `*const AtomicBool` passed as `without_gvl`'s
    /// `f` argument.
    unsafe extern "C" fn unblock(arg: *mut c_void) {
        (*(arg as *const AtomicBool)).store(true, Ordering::SeqCst);
    }

    /// Releases the GVL for the duration of `f`, letting other Ruby
    /// threads run. `f` must not call into any Ruby/magnus API directly --
    /// use [`with_gvl`] from inside `f` to do that.
    ///
    /// `f` receives a `&AtomicBool` that flips to `true` if something asks
    /// this call to unblock (see [`unblock`]) while it's running; `f` is
    /// responsible for noticing that and returning promptly. This is a
    /// best-effort, polled cancellation, not an instant wakeup -- see this
    /// module's top doc comment.
    pub(super) fn without_gvl<F, R>(f: F) -> R
    where
        F: FnOnce(&AtomicBool) -> R,
    {
        // Heap-allocated (not a stack local) so its address stays valid and
        // stable across the closure boxed into `data` below -- a stack
        // local would move (invalidating any pointer to it) the moment it's
        // captured into that closure.
        let cancel_ptr = Box::into_raw(Box::new(AtomicBool::new(false)));

        let result = call_without_gvl(move || f(unsafe { &*cancel_ptr }), cancel_ptr);

        // SAFETY: `rb_thread_call_without_gvl` has returned, so nothing
        // (including a racing `unblock` call) can still be dereferencing
        // `cancel_ptr` -- safe to reclaim and drop.
        drop(unsafe { Box::from_raw(cancel_ptr) });
        result
    }

    /// The actual `rb_thread_call_without_gvl` FFI call, split out from
    /// [`without_gvl`] so `G` (the already-`AtomicBool`-capturing closure
    /// built there) is a concrete, directly-inferred generic parameter here
    /// -- instantiating `trampoline::<G, R>` inline at the call site (with
    /// `G` written as `_`) leaves the compiler unable to pick a type among
    /// several unrelated `FnOnce` impls.
    fn call_without_gvl<G, R>(sub_f: G, cancel_ptr: *mut AtomicBool) -> R
    where
        G: FnOnce() -> R,
    {
        let data = Box::into_raw(Box::new(sub_f)) as *mut c_void;
        let result = unsafe {
            rb_sys::rb_thread_call_without_gvl(
                Some(trampoline::<G, R>),
                data,
                Some(unblock),
                cancel_ptr as *mut c_void,
            )
        };
        *unsafe { Box::from_raw(result as *mut R) }
    }

    /// Reacquires the GVL for the duration of `f`, so it can safely call
    /// Ruby/magnus APIs. Only meaningful when called from inside a
    /// [`without_gvl`] callback (running on the same OS thread) -- see
    /// `rb_thread_call_with_gvl`'s own documented restriction.
    pub(super) fn with_gvl<F, R>(f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let data = Box::into_raw(Box::new(f)) as *mut c_void;
        let result =
            unsafe { rb_sys::rb_thread_call_with_gvl(Some(trampoline::<F, R>), data) };
        *unsafe { Box::from_raw(result as *mut R) }
    }
}

/// A [`Handler`] backed by a loaded Ruby Rack app (anything responding to
/// `#call(env)`), built once per `_serve_native` call and shared (via `Rc`,
/// see `PLAN.md`'s Phase 2 "Architecture note") across every connection's
/// task.
struct RackAppHandler {
    /// The loaded Rack app. Wrapped in [`Opaque`] because a `Value` held in
    /// a heap-allocated struct field (as opposed to a local on the C stack)
    /// is invisible to Ruby's conservative-on-the-stack GC scan -- `Opaque`
    /// is magnus's documented type for exactly that case, unwrapped again
    /// with `Ruby::get_inner` (which only works from a Ruby thread) each
    /// time `call` runs.
    ///
    /// That alone isn't enough to stop `app` being collected, though: it
    /// only makes the *type* system happy about holding a `Value` outside
    /// the stack; the object itself still needs a GC root. `app` is
    /// registered as a permanent GC root in [`RackAppHandler::new`] via
    /// `gc::register_mark_object` -- correct here because the loaded app is
    /// meant to live for the entire server run (this whole call blocks for
    /// the process's lifetime in this phase; there's no unload path yet).
    app: Opaque<Value>,
    /// `SERVER_PORT`'s value, precomputed once as a string rather than on
    /// every request.
    port: String,
}

impl RackAppHandler {
    fn new(ruby: &Ruby, app: Value, port: u16) -> Result<Self, Error> {
        gc::register_mark_object(app);
        // `StringIO` (used to build `rack.input`, see `call` below) is a
        // stdlib class, not always already loaded -- `require` is a no-op
        // (and cheap) if something else already pulled it in.
        ruby.require("stringio")?;
        Ok(Self {
            app: Opaque::from(app),
            port: port.to_string(),
        })
    }

    /// Builds the Rack `env` Hash for one parsed request, per PRD.md RF02
    /// and `PLAN.md`'s Phase 2 gate description.
    fn build_env(&self, ruby: &Ruby, req: &ParsedRequest<'_>) -> Result<RHash, Error> {
        let env = ruby.hash_new();
        env.aset("REQUEST_METHOD", req.method)?;
        env.aset("PATH_INFO", req.path)?;
        env.aset("QUERY_STRING", req.query)?;
        env.aset("SERVER_NAME", "127.0.0.1")?;
        env.aset("SERVER_PORT", self.port.as_str())?;
        // Not one of PLAN.md's mandated keys (the gate's fixture app doesn't
        // echo it back), but Rack::Lint (verified against the installed
        // rack-3.2.7) raises `LintError: env missing required key
        // SERVER_PROTOCOL` without it -- the Rack::Lint compliance example
        // needs a passing env, not just the mandated-keys one. PRD.md scopes
        // this server to HTTP/1.1 exclusively (section 3.1), so the value is
        // hardcoded rather than derived from the parsed request line.
        env.aset("SERVER_PROTOCOL", "HTTP/1.1")?;
        // Rack 3 dropped `rack.version` as a required key (verified against
        // the installed rack-3.2.7's `Rack::Lint`, which no longer asserts
        // it) -- PLAN.md's Phase 2 gate still mandates this exact value, so
        // it's supplied unconditionally to satisfy the gate rather than as
        // a verified current Rack requirement.
        env.aset("rack.version", vec![1i64, 3i64])?;

        // Rack bodies are arbitrary bytes, not text: built as an
        // ASCII-8BIT/binary-encoded Ruby String (not UTF-8) so a
        // non-UTF-8 body doesn't get mangled or rejected, then wrapped in a
        // real `StringIO` so `env['rack.input']` responds to `#read` like
        // Rack requires.
        let body = ruby.enc_str_new(req.body, ruby.ascii8bit_encoding());
        let string_io: RClass = ruby.class_object().const_get("StringIO")?;
        let rack_input: Value = string_io.funcall("new", (body,))?;
        env.aset("rack.input", rack_input)?;

        // `$stderr` already responds to everything Rack::Lint checks for on
        // `rack.errors` (`#puts`, `#write`, `#flush`) -- the simplest
        // correct choice, per this task's brief. Fetched fresh each call
        // (via `eval`, the verified mechanism for reading a Ruby global --
        // magnus 0.8.2 doesn't wrap `rb_gv_get`) rather than cached on
        // `self`, so there's no long-lived `Value` here needing its own GC
        // root.
        let rack_errors: Value = ruby.eval("$stderr")?;
        env.aset("rack.errors", rack_errors)?;

        env.aset("rack.url_scheme", "http")?;
        Ok(env)
    }

    /// Calls `self.app.call(env)`, translating the Rack `[status, headers,
    /// body]` response back into a [`HandlerResponse`]. Returns `Err` (never
    /// panics) on any magnus/Ruby-side failure -- `call` (below) turns that
    /// into a `500` rather than letting it escape into `engine`, whose
    /// `Handler` trait has no `Result` in its signature (fault containment,
    /// PLAN.md Phase 7/RNF04, isn't implemented until later; this is just
    /// enough to keep one broken request from wedging the whole process).
    fn handle(&self, req: &ParsedRequest<'_>) -> Result<HandlerResponse, Error> {
        // SAFETY: `handle` only ever runs from inside `Handler::call`'s
        // `gvl::with_gvl` callback (below), which holds the GVL for exactly
        // this call's duration, so a `Ruby` handle is always safely
        // obtainable here.
        let ruby = unsafe { Ruby::get_unchecked() };
        let app = ruby.get_inner(self.app);

        let env = self.build_env(&ruby, req)?;
        let (status, headers, body): (u16, RHash, Value) = app.funcall("call", (env,))?;

        let headers = headers.to_vec::<String, String>()?;
        let body = read_body(body)?;

        Ok(HandlerResponse {
            status,
            headers,
            body,
        })
    }
}

impl Handler for RackAppHandler {
    fn call(&self, req: &ParsedRequest<'_>) -> HandlerResponse {
        // Reacquire the GVL (released for the idle/accept portion of
        // `_serve_native`'s run, see this module's top doc comment) for the
        // synchronous duration of this one request's Ruby call.
        gvl::with_gvl(|| {
            self.handle(req).unwrap_or_else(|_err| HandlerResponse {
                status: 500,
                headers: Vec::new(),
                body: Vec::new(),
            })
        })
    }
}

thread_local! {
    /// Scratch space for [`read_body`]'s [`collect_chunk`] callback -- see
    /// `read_body`'s doc comment for why a thread-local instead of a
    /// captured closure.
    static BODY_CHUNKS: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// The block passed to `body.each` by [`read_body`], via `Value::block_call`
/// (a plain, non-capturing `fn` pointer -- see that function's doc comment
/// for why it can't just close over a local `Vec<u8>`). Appends the bytes of
/// each yielded chunk (must be a `String`, per the Rack Body contract) to
/// [`BODY_CHUNKS`].
fn collect_chunk(ruby: &Ruby, args: &[Value], _block: Option<magnus::block::Proc>) -> Result<(), Error> {
    let Some(&chunk) = args.first() else {
        return Err(Error::new(
            ruby.exception_type_error(),
            "Rack body#each yielded with no argument",
        ));
    };
    let chunk = RString::try_convert(chunk)?;
    BODY_CHUNKS.with(|cell| {
        // SAFETY: the slice is copied out (`extend_from_slice`) before any
        // other Ruby call that could mutate or free `chunk` gets a chance
        // to run.
        unsafe {
            cell.borrow_mut().extend_from_slice(chunk.as_slice());
        }
    });
    Ok(())
}

/// Reads a Rack body (an object responding to `#each`, most commonly an
/// `Array` of `String`s in this phase -- no streaming yet, that's Phase 3)
/// by calling `#each` and concatenating every yielded chunk's bytes.
///
/// Uses `Value::block_call` (a real Ruby block passed to `#each`, run
/// synchronously on the same call stack), not `Value::enumeratorize`
/// (`Iterator`-style pull via `Enumerator#next`, which magnus implements
/// with a real Ruby `Fiber` under the hood): the latter was tried first and
/// reliably hung the whole server -- reproduced in isolation -- specifically
/// when the body being read was a `Rack::Lint::Wrapper` (the response body
/// Rack::Lint substitutes in), while a plain `Array` body never hung. Never
/// fully root-caused (suspected: `Enumerator#next`'s Fiber switch interacting
/// badly with this crate's own `rb_thread_call_with_gvl`/`_without_gvl`
/// nesting, which Ruby's own docs call "difficult" and admit having "few
/// experiences" with), but `block_call` sidesteps it entirely by never
/// creating a `Fiber`. `block_call`'s block is a plain, non-capturing `fn`
/// pointer (can't close over a local accumulator), so chunks are collected
/// into the [`BODY_CHUNKS`] thread-local instead -- sound here because
/// exactly one native OS thread ever runs Ruby code in this whole engine
/// (PRD.md RNF01) and `read_body` never recurses into itself.
fn read_body(body: Value) -> Result<Vec<u8>, Error> {
    BODY_CHUNKS.with(|cell| cell.borrow_mut().clear());

    let _: Value = body.block_call("each", (), collect_chunk)?;

    Ok(BODY_CHUNKS.with(|cell| std::mem::take(&mut *cell.borrow_mut())))
}

/// `HelixRack._serve_native(app, port, bind)` (see `lib/helix_rack.rb`):
/// binds a TCP listener on `bind`:`port` and runs `engine::serve` to
/// completion, blocking the calling (Ruby-owning) thread for as long as it
/// runs -- see this module's top doc comment for why that's correct for
/// this phase.
fn _serve_native(ruby: &Ruby, app: Value, port: i64, bind: String) -> Result<(), Error> {
    let port = u16::try_from(port).map_err(|_| {
        Error::new(
            ruby.exception_arg_error(),
            format!("port {port} is not a valid TCP port (0-65535)"),
        )
    })?;

    let handler: Rc<dyn Handler> = Rc::new(RackAppHandler::new(ruby, app, port)?);
    let connections = Arc::new(ConnectionCounter::new());

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(|e| Error::new(ruby.exception_runtime_error(), e.to_string()))?;

    // `serve` spawns a `spawn_local` task per connection (see `PLAN.md`'s
    // Phase 2 "Architecture note"), which needs a `LocalSet` to schedule
    // onto -- wrapping `block_on`'s future in `LocalSet::run_until` is what
    // provides that, matching `engine/tests/support/mod.rs`'s test harness.
    //
    // Wrapped in `gvl::without_gvl` -- see this module's top doc comment for
    // why that's necessary here. `Handler::call` (via `RackAppHandler`)
    // reacquires the GVL with `gvl::with_gvl` for each request. `serve`
    // itself never returns on its own (Phase 8 -- graceful shutdown -- is
    // what teaches it to), so it's raced against `cancelled`, which resolves
    // once `without_gvl`'s unblock function has fired (see `gvl::without_gvl`).
    let local_set = tokio::task::LocalSet::new();
    let result: std::io::Result<()> = gvl::without_gvl(|cancel| {
        runtime.block_on(local_set.run_until(async move {
            let listener = tokio::net::TcpListener::bind((bind.as_str(), port)).await?;
            tokio::select! {
                res = serve(listener, connections, handler) => res,
                () = cancelled(cancel) => Ok(()),
            }
        }))
    });

    result.map_err(|e| Error::new(ruby.exception_runtime_error(), e.to_string()))
}

/// Polls `cancel` (set by `gvl::without_gvl`'s unblock function, e.g. when
/// something `Thread#kill`s the calling Ruby thread) every 20ms, resolving
/// once it's `true`. Not a low-latency wakeup -- see this module's top doc
/// comment -- just enough that `_serve_native` returns within a bounded,
/// short time instead of never.
async fn cancelled(cancel: &AtomicBool) {
    while !cancel.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let module = ruby.define_module("HelixRack")?;
    module.define_module_function("_serve_native", magnus::function!(_serve_native, 3))?;
    Ok(())
}
