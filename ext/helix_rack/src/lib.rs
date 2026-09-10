//! magnus glue for Phase 2 (see `PLAN.md` at the repo root, Phase 2): binds
//! `HelixRack._serve_native(app, port)`, the native entry point
//! `lib/helix_rack.rb`'s `HelixRack.serve` calls into.
//!
//! Per `PLAN.md`'s Phase 2 "Architecture note", this runs `engine`'s Tokio
//! `current_thread` runtime via `block_on` on the *same* OS thread Ruby
//! called it from, so `RackAppHandler::call` (below) can call into Ruby
//! directly.
//!
//! Since Phase 3 (see `PLAN.md`, Phase 3 "Architecture decision"),
//! [`read_body`] no longer unconditionally accumulates a whole Rack
//! response body into one `Vec<u8>`: past [`SPOOL_THRESHOLD_BYTES`], it
//! spills to a tempfile and returns `ResponseBody::Spooled` instead of
//! `ResponseBody::InMemory`, so `engine`'s `connection::handle` can stream
//! it back out to the socket in bounded chunks rather than needing the
//! whole thing resident in RAM. See [`BodyAccumulator`] and
//! [`SPOOL_THRESHOLD_BYTES`]'s own doc comments for the mechanism and the
//! threshold choice.
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

use std::io::Write as _;
use std::os::raw::c_void;
use std::panic::{self, AssertUnwindSafe};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use magnus::gc;
use magnus::prelude::*;
use magnus::value::Opaque;
use magnus::{Error, RClass, RHash, RString, Ruby, Value};

use helixrack_engine::{
    probe_io_backend, serve, ConnectionCounter, Handler, HandlerResponse, ParsedRequest, ResponseBody,
};

/// Phase 10 (`PLAN.md`, Phase 10/RNF03): every Rust-side heap allocation in
/// this crate (and in `helixrack_engine`, since Rust's global allocator is
/// artifact-wide, not per-crate) goes through mimalloc instead of the
/// platform default (glibc `malloc` on Linux) from here on. "Artifact-wide"
/// on purpose, not "process-wide": this only reaches allocations made
/// *inside this `cdylib`*. A second, unrelated Rust Ruby extension `dlopen`'d
/// into the same host process keeps its own `#[global_allocator]` (or the
/// platform default) entirely -- `#[global_allocator]` binds once per linked
/// artifact, not once per OS process.
///
/// `#[global_allocator]` can only be set once, in the crate that produces
/// the final linked artifact -- here, that's this crate's own `cdylib`
/// (`helix_rack.so`), not `engine` (a plain `rlib` dependency, which has no
/// artifact of its own for this attribute to apply to).
///
/// **Scope, stated plainly rather than left to sound broader than it is:**
/// this affects this extension's *own* Rust-side allocations -- the
/// `Vec<u8>`/`String`/`Box` traffic `engine::connection`'s HTTP parsing,
/// buffering, and response construction does, which is exactly where this
/// project's own allocation churn happens (PRD.md RNF03's stated goal:
/// avoiding heap fragmentation for the server's own memory footprint). It
/// does **not** replace Ruby's own object allocator -- CRuby's VALUE heap
/// is its own GC-managed arena/slab system, not a thin wrapper over libc
/// `malloc` calls this attribute could intercept, and this diff does not
/// attempt the considerably more invasive step of overriding the
/// process-wide C `malloc` symbol itself (which a dlopen'd extension *can*
/// do via symbol interposition, but doing it soundly -- for a shared
/// library loaded into an already-running process with its own allocator
/// already active, not a standalone binary -- was judged out of scope for
/// this phase's real, achievable deliverable).
///
/// **Two known, accepted gaps**, surfaced by this phase's safety review and
/// recorded here rather than fixed, since neither is this project's own
/// regression -- both are pre-existing properties of adding *any*
/// `#[global_allocator]` (mimalloc or otherwise) to a Ruby C extension, not
/// something this diff introduces:
/// - **Fork safety.** This crate registers no `pthread_atfork` handler.
///   mimalloc's own allocator state (its heap/segment/page structures) is
///   not guaranteed fork-safe by default without one -- a process that
///   `fork()`s (CRuby's own `Process.fork`, or a pre-fork server like
///   Puma/Unicorn) while another thread holds an internal mimalloc lock can
///   leave the child with a permanently wedged allocator. Out of scope for
///   this phase: this project's own gate (Phase 2 onward) never forks, and a
///   correct fix means auditing every deployment topology this extension
///   could be loaded into, not a one-line change here.
/// - **Debug-build corruption detection.** mimalloc's own double-free/heap-
///   corruption detection is compiled out at `MI_DEBUG=0` (the default for
///   the `mimalloc` crate's non-`debug`-featured release build used here),
///   trading a security-relevant diagnostic for the release-mode speed
///   RNF03 is asking for. The `debug`/`debug_in_debug` Cargo features exist
///   precisely for turning this back on when investigating a suspected
///   corruption bug, at a real performance cost -- not enabled by default.
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

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
    /// Ruby/magnus APIs.
    ///
    /// # Safety (not enforced by the type system -- caller's responsibility)
    ///
    /// Must only be called from the same OS thread that is currently inside
    /// a [`without_gvl`] callback on that thread (i.e. nested inside the
    /// `f` passed to a `without_gvl` call still running on this thread).
    /// This is `rb_thread_call_with_gvl`'s own documented restriction, not
    /// one this module adds. Calling it from a thread that never released
    /// the GVL via `without_gvl` is undefined behavior at the CRuby level
    /// (not a panic, not a `Result::Err`) -- there is currently exactly one
    /// call site (`RackAppHandler::call`), correctly nested; if a future
    /// caller is added, re-verify this invariant by inspection, since
    /// nothing here will catch a violation for you.
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

/// Phase 6 (`PLAN.md`, Phase 6): the watchdog OS thread and the
/// counter-based preemption *signal* it drives via `rb_postponed_job_trigger`.
///
/// # Step 1 findings -- the API, verified, not assumed
///
/// PLAN.md's Phase 6 architecture note flags one load-bearing claim that
/// needs verifying before any of this is trustworthy: that
/// `rb_postponed_job_trigger` is genuinely safe to call from a different OS
/// thread, without holding the GVL. Verified two ways, both pointing at the
/// same answer:
///
/// 1. This project's own generated bindgen output (the same technique
///    `ext/helix_rack/src/lib.rs`'s `gvl` module used in Phase 2 to find
///    `rb_thread_call_without_gvl`'s real signature):
///    `target/debug/build/rb-sys-*/out/bindings-0.9.130-mri-x86_64-linux-4.0.6.rs`
///    (there are several `rb-sys-*` build dirs from different cargo
///    profiles/feature sets; all carry the same bindings for this Ruby
///    version) declares exactly four postponed-job functions:
///    `rb_postponed_job_preregister`, `rb_postponed_job_trigger`, and the
///    two deprecated ones below. `rb_postponed_job_trigger`'s doc comment
///    there reads: "This method is async-signal-safe and can be called from
///    any thread, at any time, including in signal handlers."
/// 2. Cross-checked against this machine's actually-installed Ruby 4.0.6
///    headers (`ruby -v` confirms 4.0.6, matching the bindgen filename
///    exactly -- not a different Ruby than the one this extension builds
///    against): `~/.local/share/mise/installs/ruby/4.0.6/include/ruby-4.0.0/
///    ruby/debug.h`. Same declarations, same doc comment, word for word --
///    bindgen's output is a direct transcription of this header, not a
///    separate claim to independently doubt.
///
/// That header also explains *why* the modern `..._preregister`/
/// `..._trigger` pair exists instead of the older, single-call
/// `rb_postponed_job_register`/`rb_postponed_job_register_one` (still
/// present, but `#[deprecated]` in the bindgen output): those older
/// functions "claimed to be fully async-signal-safe... [but] were subject
/// to race conditions which could cause crashes when racing with Ruby's
/// internal use of them." This module uses only the current, non-deprecated
/// pair.
///
/// # Does magnus wrap any of this?
///
/// No. Checked the same way Phase 2 checked for `rb_thread_call_without_gvl`
/// -- magnus 0.8.2's own `src/lib.rs` "C Function Index" (`grep -rn
/// "postponed_job" .../magnus-0.8.2/src/lib.rs`). It lists exactly two
/// entries, both commented out (magnus's convention there for "known,
/// deliberately unimplemented"): `rb_postponed_job_register` and
/// `rb_postponed_job_register_one` -- the two *deprecated* functions. The
/// current `rb_postponed_job_preregister`/`rb_postponed_job_trigger` pair
/// isn't mentioned anywhere in that crate at all (`grep -rn
/// "postponed_job_preregister\|postponed_job_trigger"` across the whole
/// `magnus-0.8.2` source tree returns nothing) -- not implemented, not even
/// on the "known unimplemented" list, presumably because that list predates
/// the newer API. Either way: raw `rb-sys` FFI, same as `gvl`, is the only
/// option.
///
/// # Step 3 -- the open question, investigated and answered "no, ship
/// counter-only"
///
/// PLAN.md's Phase 6 section asks, as a genuinely open question: can the
/// postponed job's callback -- invoked synchronously by Ruby's own bytecode
/// dispatch, nested inside the still-in-progress `Handler::call`'s
/// `gvl::with_gvl` scope, itself nested inside the outer
/// `runtime.block_on(local_set.run_until(...))` call in `_serve_native` --
/// safely drive Tokio's reactor forward to service *other* connections
/// during that pause? Investigated empirically (a throwaway standalone
/// crate, not theorized from memory): built a minimal `current_thread` +
/// `LocalSet` runtime, spawned a second `spawn_local` task standing in for
/// "another connection", and, from *inside* the first task's own poll (the
/// direct analogue of being nested inside `Handler::call`), called
/// `tokio::runtime::Handle::block_on` on a trivial future -- the most direct
/// available way to "drive the runtime forward" from that nested position.
/// It panicked immediately: `"Cannot start a runtime from within a runtime.
/// This happens because a function (like `block_on`) attempted to block the
/// current thread while the thread is being used to drive asynchronous
/// tasks."` Tokio's `current_thread` runtime is not reentrant, and this is
/// enforced, not merely discouraged.
///
/// There is also no lower-level, safe, *public* Tokio API to do a partial
/// "just drive the reactor / wake ready tasks" step without going through
/// `block_on` (no `Runtime::turn()`-style primitive exists in Tokio 1.x);
/// reaching for private/internal mechanics to fake one would be exactly the
/// kind of guess-implementation this phase's brief says not to ship. And
/// even setting the reentrancy panic aside, the goal is arguably incoherent
/// on its own terms: any *other* connection whose task is ready to run is,
/// for an HTTP server whose only real work is calling `Handler::call`,
/// indistinguishable from "ready to have its Rack app's `.call(env)`
/// invoked" -- so "drain the reactor without risking a second nested call
/// into Ruby" (PLAN.md's own stated constraint) would require the nested
/// poll to selectively run only non-Ruby-touching tasks, which the
/// scheduler's public API gives no way to do.
///
/// Conclusion: this phase ships the verified, correctly-firing counter-based
/// trigger mechanism only. **The "actually unstarves the event loop"
/// capability PRD.md's RF07 and PLAN.md's Phase 6 deliverable describe is
/// NOT implemented** -- a long-running CPU-bound handler still fully
/// occupies this server's one OS thread until it returns or yields the GVL
/// on its own; all this phase adds is a correctly-firing signal that it
/// happened, counted, and readable from Ruby via
/// `HelixRack._postponed_job_count`. No later phase in `PLAN.md` revisits
/// this gap -- see that document's Phase 6 section, updated alongside this
/// module, for the same conclusion recorded where the next reader of the
/// plan will see it.
mod watchdog {
    use super::*;
    use std::sync::atomic::AtomicU64;
    use std::sync::{Condvar, Mutex, OnceLock};
    use std::thread::{self, JoinHandle};
    use std::time::Instant;

    /// How many times [`postponed_job_callback`] has actually run. Global,
    /// not scoped to one `Watchdog`/`_serve_native` call:
    /// `rb_postponed_job_preregister` is itself a one-time, process-lifetime
    /// registration (see [`register`]), so there is exactly one callback for
    /// the whole process, and `HelixRack._postponed_job_count` is a simple,
    /// always-valid global getter regardless of whether a server is
    /// currently running. This does **not** reset between `HelixRack.serve`
    /// calls in the same process (e.g. across RSpec examples that each boot
    /// and kill their own server) -- a caller that needs a fresh reading
    /// must capture a baseline before its request and assert on the delta,
    /// which is exactly what `spec/integration/phase6_preemption_spec.rb`
    /// does, rather than assuming this starts at zero.
    static POSTPONED_JOB_COUNT: AtomicU64 = AtomicU64::new(0);

    /// The handle returned by `rb_postponed_job_preregister`, set exactly
    /// once by [`register`].
    static POSTPONED_JOB_HANDLE: OnceLock<rb_sys::rb_postponed_job_handle_t> = OnceLock::new();

    /// The callback Ruby invokes -- on whatever Ruby thread next checks for
    /// interrupts, always holding the GVL at that point (per
    /// `rb_postponed_job_trigger`'s doc comment, verified above) -- once
    /// [`fire`] has called `rb_postponed_job_trigger`. Deliberately the
    /// simplest possible thing: increments [`POSTPONED_JOB_COUNT`] and
    /// returns. Touches no Ruby object and calls no Ruby/magnus API, even
    /// though the GVL is held here and the doc comment says allocation would
    /// be safe -- this phase's gate needs nothing more than a count, and the
    /// smaller the surface of an `extern "C"` callback Ruby can invoke at an
    /// arbitrary bytecode dispatch point, the easier it is to be sure it's
    /// correct.
    ///
    /// **Maintained invariant, not just today's implementation detail:**
    /// this function must never panic. Ruby dispatches it at an interrupt
    /// checkpoint that can land *inside* `app.funcall("call", env)`
    /// (`RackAppHandler::handle`, this file's top level) -- i.e. with a live
    /// Ruby/C frame above it -- which is exactly the case
    /// `Handler::call`'s own `catch_unwind` doc comment names as this
    /// function's half of why that `catch_unwind` is sound: not because
    /// anything wraps *this* callback the way magnus wraps a `block_call`
    /// closure, but because it cannot panic in the first place (one atomic
    /// `fetch_add`, no allocation, no `unwrap`/`expect`, no Ruby call). If
    /// this function ever grows a call that can panic -- even an `eprintln!`
    /// for debugging, which reintroduces this crate's own `log_fault`-vs-
    /// `eprintln!` lesson (see `log_fault`'s doc comment) in a spot no
    /// `catch_unwind` here protects -- that panic unwinds across a live
    /// foreign frame into Ruby's own interrupt-checkpoint dispatch code,
    /// which is undefined behavior, not a clean Ruby exception or an HTTP
    /// `500`.
    ///
    /// # Safety
    /// Called by Ruby itself (via the postponed job mechanism), always
    /// holding the GVL when it does -- never called directly by this crate.
    unsafe extern "C" fn postponed_job_callback(_data: *mut c_void) {
        POSTPONED_JOB_COUNT.fetch_add(1, Ordering::SeqCst);
    }

    /// Registers [`postponed_job_callback`] in Ruby's postponed job table,
    /// storing the returned handle in [`POSTPONED_JOB_HANDLE`]. Called
    /// exactly once, from `init` (this extension's `#[magnus::init]` entry
    /// point -- i.e. while Ruby is loading the native extension, holding the
    /// GVL): `rb_postponed_job_preregister`'s own doc comment (see this
    /// module's top doc comment) says "Generally, this function will be
    /// called during the initialization routine of an extension", and the
    /// table it registers into is small (32 entries) and process-lifetime --
    /// registering more than once would either grow the table pointlessly or
    /// (same doc comment) silently overwrite the stored `data` for an
    /// existing registration of the same `func` and hand back the same
    /// handle anyway, since `postponed_job_callback` is the same function
    /// pointer every time. `_serve_native` (unlike this) *does* run once per
    /// server start, potentially many times per process across tests --
    /// which is exactly why registration lives here, in `init`, and not
    /// there.
    /// `rb_postponed_job_preregister`'s documented failure sentinel: the
    /// 32-entry preregistration table is full (e.g. many other extensions
    /// already registered their own jobs). A `#define` macro in Ruby's own
    /// `ruby/debug.h`
    /// (`#define POSTPONED_JOB_HANDLE_INVALID ((rb_postponed_job_handle_t)UINT_MAX)`,
    /// verified by reading the installed Ruby 4.0.6 header directly), not a
    /// constant `bindgen` translated into these bindings -- hardcoded here
    /// with that source cited, not guessed.
    const POSTPONED_JOB_HANDLE_INVALID: rb_sys::rb_postponed_job_handle_t = u32::MAX;

    fn register() {
        // SAFETY: `rb_postponed_job_preregister` requires no GVL-related
        // precondition beyond "generally called from an extension's init
        // routine" (see the doc comment quoted above) -- `init` satisfies
        // that directly. `postponed_job_callback` matches
        // `rb_postponed_job_func_t` exactly (`extern "C" fn(*mut c_void)`),
        // and `data` is unused (passed as `null_mut`) since the callback
        // needs none.
        let handle = unsafe {
            rb_sys::rb_postponed_job_preregister(0, Some(postponed_job_callback), std::ptr::null_mut())
        };
        // A safety-review finding: the old version of this function stored
        // whatever came back unconditionally. If the table were ever full
        // (unlikely with only 32 slots and few extensions loaded, but
        // documented and real), this would silently store an invalid handle
        // that `fire` would trigger on every over-slice request thereafter.
        // Ruby's own docs are explicit this failure is permanent for the
        // process's lifetime ("no further registration will do so"), so
        // there is nothing to retry -- failing loudly at extension-load time
        // is the right response, not a silent no-op preemption feature.
        assert_ne!(
            handle, POSTPONED_JOB_HANDLE_INVALID,
            "rb_postponed_job_preregister's table is full -- HelixRack's Phase 6 preemption \
             signal cannot function for the rest of this process"
        );
        // `.set` only fails if already set -- `register` has exactly one
        // caller (`init`, invoked once per process by Ruby's extension
        // loader), so this can't race in practice; `expect` documents that
        // invariant rather than silently ignoring a violation of it.
        POSTPONED_JOB_HANDLE
            .set(handle)
            .expect("watchdog::register must only be called once, from init");
    }

    /// Calls `rb_postponed_job_trigger` for the handle [`register`] stored.
    /// Safe to call from any thread without holding the GVL -- see this
    /// module's top doc comment for the verification. This is the only
    /// place in this module that actually touches the Ruby C-API from
    /// [`Watchdog`]'s own OS thread.
    fn fire() {
        let handle = *POSTPONED_JOB_HANDLE
            .get()
            .expect("watchdog::register must run (from init) before any Watchdog can fire");
        // SAFETY: `rb_postponed_job_trigger` is documented async-signal-safe
        // and callable from any thread at any time without the GVL (see
        // this module's top doc comment) -- the one precondition is that
        // `handle` came from a still-valid `rb_postponed_job_preregister`
        // call, which it did (`register`, at extension-init time, and the
        // registration table lives for the process's entire lifetime).
        unsafe { rb_sys::rb_postponed_job_trigger(handle) };
    }

    /// Current value of [`POSTPONED_JOB_COUNT`] -- backs
    /// `HelixRack._postponed_job_count`.
    pub(super) fn postponed_job_count() -> u64 {
        POSTPONED_JOB_COUNT.load(Ordering::SeqCst)
    }

    /// Registers the postponed job callback -- see [`register`]'s doc
    /// comment for why this must run exactly once, from `init`.
    pub(super) fn init() {
        register();
    }

    /// One request's deadline state, guarded by [`Watchdog`]'s `Mutex` --
    /// see that struct's doc comment for the full synchronization design.
    /// `generation` is bumped on every [`Watchdog::arm`]/[`Watchdog::disarm`]
    /// transition so the watchdog thread can tell, after firing and
    /// reacquiring the lock, whether the armed period it just fired for is
    /// still the current one (see [`Watchdog::run`]) -- comparing `Instant`
    /// deadlines directly would work in practice (two real deadlines
    /// colliding to the nanosecond is not realistic) but a plain counter
    /// removes any doubt rather than relying on that.
    enum State {
        /// No request is currently in flight; the watchdog thread blocks
        /// indefinitely (see [`Watchdog::run`]) until [`Watchdog::arm`] or
        /// [`Watchdog::shutdown`] changes this.
        Idle,
        /// A request is in flight; the watchdog thread wakes at `Instant`
        /// (or sooner, if the state changes first) to check whether it's
        /// still due.
        Armed(Instant),
        /// `_serve_native` is returning; the watchdog thread exits its loop.
        Shutdown,
    }

    /// The Phase 6 watchdog: a dedicated OS thread (see `PLAN.md`'s Phase 6
    /// "Architecture note" for why a second OS thread is unavoidable here --
    /// the main thread has handed control to Ruby's VM synchronously for the
    /// duration of `Handler::call` and cannot notice a long-running one on
    /// its own) that tracks one request's deadline at a time and calls
    /// [`fire`] if that deadline passes while the request is still in
    /// flight.
    ///
    /// # Synchronization design
    ///
    /// A `Mutex<(State, u64)>` + `Condvar`, not raw atomics: this state
    /// changes at most a few times per request (arm, disarm, occasionally a
    /// fire) plus once at shutdown, so lock/unlock overhead (tens of
    /// nanoseconds) is immaterial next to the cost of a Ruby request, and a
    /// mutex makes the one genuinely subtle race here -- "a request finishes
    /// just as the watchdog is about to fire" -- trivial to reason about:
    /// [`Watchdog::disarm`] and the watchdog thread's own deadline check
    /// both take the same lock, so they can never interleave; either
    /// `disarm` runs first (sets `Idle`, the watchdog thread's `now >=
    /// deadline` check never even sees `Armed` again) or the watchdog
    /// thread's check runs first (while still holding the lock, sees
    /// `Armed(deadline)` with `deadline` already passed, and proceeds to
    /// fire) and `disarm` simply blocks on the mutex until the watchdog
    /// thread releases it after firing. Either outcome is correct: a fire
    /// that loses this race by a hair is a harmless extra
    /// `rb_postponed_job_trigger` call whose callback just increments a
    /// counter (see [`postponed_job_callback`]) -- there is no "un-fire"
    /// needed, and the gate's fast-handler fixture leaves enough margin
    /// under the slice that this race is never actually live for it (see
    /// `spec/integration/phase6_preemption_spec.rb`).
    ///
    /// No busy-spinning: idle waits block indefinitely on the `Condvar`
    /// (zero wakeups between requests); an armed wait uses
    /// `Condvar::wait_timeout` bounded to exactly the remaining slice, so
    /// the OS -- not this thread -- accounts for the wait, and the thread
    /// wakes at most twice per over-slice request (once at the deadline to
    /// fire, once more at `disarm`) and exactly once per under-slice request
    /// (at `disarm`, well before its `wait_timeout` would have elapsed).
    pub(super) struct Watchdog {
        /// `(state, generation)` -- see [`State`]'s doc comment for why
        /// `generation` is tracked alongside it.
        state: Mutex<(State, u64)>,
        cv: Condvar,
    }

    impl Watchdog {
        /// Spawns the watchdog thread and returns the shared handle plus a
        /// [`WatchdogGuard`] that shuts it down and joins it when dropped --
        /// see that type's doc comment for why RAII, not a manual
        /// shutdown-then-join call `_serve_native` is trusted to remember on
        /// every exit path.
        pub(super) fn spawn() -> (Arc<Self>, WatchdogGuard) {
            let watchdog = Arc::new(Self {
                state: Mutex::new((State::Idle, 0)),
                cv: Condvar::new(),
            });
            let thread_watchdog = Arc::clone(&watchdog);
            let join_handle = thread::Builder::new()
                .name("helix_rack-watchdog".to_string())
                .spawn(move || thread_watchdog.run())
                .expect("failed to spawn the HelixRack Phase 6 watchdog thread");
            let guard = WatchdogGuard {
                watchdog: Arc::clone(&watchdog),
                thread: Some(join_handle),
            };
            (watchdog, guard)
        }

        /// Arms the watchdog for one request: due `slice` from now. Every
        /// call bumps `generation` (see [`State`]'s doc comment). Must be
        /// paired with [`Watchdog::disarm`] once the request finishes --
        /// [`ArmedGuard`] (returned by [`Watchdog::arm_guard`]) does this via
        /// `Drop` so a caller can't forget, even on an early return.
        fn arm(&self, slice: Duration) {
            let deadline = Instant::now() + slice;
            let mut state = self.state.lock().unwrap();
            state.0 = State::Armed(deadline);
            state.1 += 1;
            drop(state);
            self.cv.notify_one();
        }

        /// Ends the current request's armed period. A no-op (state simply
        /// goes to `Idle` either way) past the point the watchdog has
        /// already fired for this request.
        fn disarm(&self) {
            let mut state = self.state.lock().unwrap();
            state.0 = State::Idle;
            state.1 += 1;
            drop(state);
            self.cv.notify_one();
        }

        /// Arms for `slice` and returns an RAII guard that disarms on drop
        /// -- the only way `RackAppHandler::call` touches the watchdog, so
        /// arm/disarm can never desync even if `self.handle` returns early.
        pub(super) fn arm_guard(&self, slice: Duration) -> ArmedGuard<'_> {
            self.arm(slice);
            ArmedGuard { watchdog: self }
        }

        /// Tells the watchdog thread to exit its loop, and wakes it
        /// immediately (rather than waiting for whatever it's currently
        /// blocked on) so `_serve_native` can join it without delay.
        pub(super) fn shutdown(&self) {
            let mut state = self.state.lock().unwrap();
            state.0 = State::Shutdown;
            drop(state);
            self.cv.notify_one();
        }

        /// The watchdog thread's body -- see this struct's doc comment for
        /// the synchronization design this implements.
        fn run(&self) {
            // SAFETY (not memory-safety, just an invariant worth stating):
            // `.unwrap()` on this lock's `LockResult` would only fail if a
            // prior holder panicked while holding it -- nothing in `arm`,
            // `disarm`, `shutdown`, or this loop body panics while the lock
            // is held, so poisoning here would itself indicate a bug
            // elsewhere in this module worth crashing loudly on, not a case
            // to recover from silently.
            let mut guard = self.state.lock().unwrap();
            loop {
                match guard.0 {
                    State::Shutdown => return,
                    State::Idle => {
                        guard = self.cv.wait(guard).unwrap();
                    }
                    State::Armed(deadline) => {
                        let now = Instant::now();
                        if now < deadline {
                            let (new_guard, _timeout_result) =
                                self.cv.wait_timeout(guard, deadline - now).unwrap();
                            guard = new_guard;
                            continue;
                        }
                        // Deadline passed while still armed -- see this
                        // struct's doc comment for why it's correct to fire
                        // here rather than re-check against `disarm`'s
                        // concurrent write: the lock already serializes
                        // that.
                        let fired_generation = guard.1;
                        drop(guard);
                        fire();
                        guard = self.state.lock().unwrap();
                        // Only block again if nothing has changed since the
                        // fire (same generation still `Armed`) -- otherwise
                        // loop back to the top and let the normal match
                        // handle whatever the new state actually is
                        // (`Idle` from a `disarm`, or a fresh `Armed` from
                        // the next request already having started). Without
                        // this check, an armed period that outlives one
                        // slice would busy-loop calling `fire` every
                        // iteration until `disarm` finally runs.
                        //
                        // Looped, not a single `wait`: a safety-review
                        // finding caught that `Condvar::wait` can return on
                        // a spurious wakeup with nothing having actually
                        // changed (documented standard library behavior,
                        // not specific to this code) -- a single `wait` call
                        // would then fall through with `guard.0` still
                        // `Armed(deadline)` at the same already-passed
                        // deadline, and the outer `match` would take the
                        // "deadline passed" branch again, firing a second
                        // time for a request that hasn't finished. Re-
                        // checking the generation after every wakeup, spurious
                        // or not, and only stopping once it has genuinely
                        // changed, fixes that without reintroducing the
                        // busy-loop this check exists to prevent.
                        while guard.1 == fired_generation {
                            guard = self.cv.wait(guard).unwrap();
                        }
                    }
                }
            }
        }
    }

    /// RAII guard returned by [`Watchdog::arm_guard`]: disarms the watchdog
    /// when dropped.
    pub(super) struct ArmedGuard<'a> {
        watchdog: &'a Watchdog,
    }

    impl Drop for ArmedGuard<'_> {
        fn drop(&mut self) {
            self.watchdog.disarm();
        }
    }

    /// RAII guard returned by [`Watchdog::spawn`]: on drop, shuts down the
    /// watchdog and joins its thread.
    ///
    /// A safety-review finding on an earlier version of this module: with a
    /// manual `watchdog.shutdown(); watchdog_thread.join();` call instead of
    /// this guard, a *genuine* Rust-level early return in `_serve_native`
    /// between spawning the watchdog and reaching that manual call (e.g. the
    /// Tokio runtime's `.build()?` failing for an ordinary reason, unrelated
    /// to Ruby) would leave the watchdog thread parked forever with nothing
    /// left holding a reference able to shut it down. `Drop` runs on every
    /// such early return automatically.
    ///
    /// This does **not** cover every early-return path, though, and it's
    /// worth being precise about which: a `Thread#kill` pending against the
    /// calling Ruby thread can be delivered via a non-local, longjmp-like
    /// exit (see `_serve_native`'s own doc comment on `RackAppHandler::
    /// prepare_app`'s placement for the full story, including a residual,
    /// unresolved gap this ordering narrows but does not close) that does
    /// *not* run Rust's normal unwind/`Drop` machinery -- this guard's
    /// `Drop` is exactly as blind to that exit as a manual call would have
    /// been. `_serve_native` still `drop`s this explicitly at one specific
    /// point on the normal path (inside the `gvl::without_gvl` closure,
    /// before it returns), because that placement, not RAII, is what
    /// actually closes that particular window.
    pub(super) struct WatchdogGuard {
        watchdog: Arc<Watchdog>,
        thread: Option<JoinHandle<()>>,
    }

    impl Drop for WatchdogGuard {
        fn drop(&mut self) {
            self.watchdog.shutdown();
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
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
    /// Phase 6 (`PLAN.md`, Phase 6): the watchdog that fires a postponed job
    /// if a single `call` (below) runs past `cpu_time_slice`. Shared (not
    /// owned) because `_serve_native` spawns exactly one `watchdog::Watchdog`
    /// per server run and must retain its own handle to shut it down and
    /// join its thread once `serve` returns -- see `_serve_native`'s doc
    /// comment.
    watchdog: Arc<watchdog::Watchdog>,
    /// PRD.md section 6.2's `--cpu-time-slice` (`exe/helix_rack` ->
    /// `lib/helix_rack.rb` -> `_serve_native`), converted to a `Duration`
    /// once here rather than on every request.
    cpu_time_slice: Duration,
}

impl RackAppHandler {
    /// Does every Ruby/magnus-API call `RackAppHandler` needs before it can
    /// be constructed (`app`'s GC root, `StringIO` loaded) -- split out from
    /// `new` (which does no Ruby calls at all now, just struct
    /// construction) so `_serve_native` can call this *before*
    /// `watchdog::Watchdog::spawn`, not after. That ordering matters: see
    /// `_serve_native`'s doc comment on the watchdog-thread-leak safety-
    /// review finding this fixes.
    fn prepare_app(ruby: &Ruby, app: Value) -> Result<(), Error> {
        gc::register_mark_object(app);
        // `StringIO` (used to build `rack.input`, see `call` below) is a
        // stdlib class, not always already loaded -- `require` is a no-op
        // (and cheap) if something else already pulled it in.
        ruby.require("stringio")?;
        Ok(())
    }

    /// Constructs the handler. Does **not** call into Ruby/magnus at all --
    /// [`RackAppHandler::prepare_app`] must have already run for `app`
    /// (`_serve_native` is this type's only caller, and does exactly that,
    /// in that order).
    fn new(app: Value, port: u16, watchdog: Arc<watchdog::Watchdog>, cpu_time_slice: Duration) -> Self {
        Self {
            app: Opaque::from(app),
            port: port.to_string(),
            watchdog,
            cpu_time_slice,
        }
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
    /// body]` response back into a [`HandlerResponse`]. Returns `Err` on any
    /// magnus/Ruby-side failure (a raised exception, a failed type
    /// conversion) rather than letting it escape into `engine`, whose
    /// `Handler` trait has no `Result` in its signature; a genuine Rust
    /// panic can still escape this function itself (a bug in this crate, or
    /// the deliberate one below, gated on a debug header) --
    /// `Handler::call`'s `catch_unwind` around its whole call to this method
    /// is what turns *either* kind of fault into a `500` (PLAN.md Phase
    /// 7/RNF04), not this function alone.
    fn handle(&self, req: &ParsedRequest<'_>) -> Result<HandlerResponse, Error> {
        // Phase 7 (`PLAN.md`, Phase 7 "Gate"): a deliberate Rust panic when
        // a request carries this header *and* `debug_panic_header_enabled()`
        // is true -- exists purely so
        // `spec/integration/phase7_fault_containment_spec.rb`'s fixture app
        // (`spec/fixtures/apps/phase7_fault_app.rb`) has a real HTTP-
        // request-reachable way to trigger a genuine Rust panic *inside this
        // function's own logic*, which is what `Handler::call`'s
        // `catch_unwind` (below) actually guards. An earlier version of this
        // fixture instead exposed a separate Ruby-callable
        // `HelixRack._debug_panic` module function for the Rack app to call
        // -- verified by direct smoke test (not assumed) that this does
        // *not* exercise that `catch_unwind` at all: magnus wraps every
        // Ruby-callable function (`define_module_function`, in `init` below)
        // in its own `catch_unwind` already (see the vendored
        // `magnus-0.8.2/src/method.rs`'s `call_handle_error`, `Error::
        // from_panic`), which converts a panic caught *there* straight into
        // a raised Ruby exception before it ever reaches this crate's own
        // code -- so it would land in `Handler::call`'s `Ok(Err(err))`
        // branch, not its `Err(payload)` one. A header check inside this
        // function's own body, with no intervening Ruby-callable-function
        // hop, is what actually reaches the code path Phase 7 is testing.
        //
        // Gated on `debug_panic_header_enabled()` (see that function's doc
        // comment), not left unconditional, per a safety-review finding: an
        // always-on remote trigger for a deliberate panic is a real problem
        // even though this crate's own `catch_unwind` catches it today --
        // it's a free remote amplification knob (each trigger costs real
        // CPU on this server's single event-loop thread, worse still with
        // `RUST_BACKTRACE=1`), and a *silent* one if this crate's build
        // profile ever gains `panic = "abort"` (nothing here enforces
        // `panic = "unwind"`), at which point it would stop being "a caught
        // panic" and become a one-request remote process kill.
        if debug_panic_header_enabled()
            && req
                .headers
                .iter()
                .any(|(name, value)| name.eq_ignore_ascii_case("x-helixrack-debug-panic") && *value == "1")
        {
            panic!("x-helixrack-debug-panic: deliberate panic for the Phase 7 fault-containment gate");
        }

        // SAFETY: `handle` only ever runs from inside `Handler::call`'s
        // `gvl::with_gvl` callback (below), which holds the GVL for exactly
        // this call's duration, so a `Ruby` handle is always safely
        // obtainable here.
        let ruby = unsafe { Ruby::get_unchecked() };
        let app = ruby.get_inner(self.app);

        let env = self.build_env(&ruby, req)?;
        let (status, headers, body): (u16, RHash, Value) = app.funcall("call", (env,))?;

        let headers = headers.to_vec::<String, String>()?;
        // `read_body` (see its own doc comment) returns `InMemory` for a
        // body that stayed under `SPOOL_THRESHOLD_BYTES`, or `Spooled` for
        // one that crossed it -- either way, already the right
        // `ResponseBody` variant for `engine`'s `Handler` trait, no further
        // wrapping needed here.
        let body = read_body(&ruby, body)?;

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
            // Phase 6 (`PLAN.md`, Phase 6; see the `watchdog` module's top
            // doc comment): armed for exactly the span of `self.handle`
            // below, disarmed automatically (even on early return) when
            // `_armed` drops at the end of this closure. If `self.handle`
            // runs past `cpu_time_slice` while still armed, the watchdog
            // thread fires a postponed job that increments a counter
            // readable via `HelixRack._postponed_job_count` -- see that
            // module's doc comment for why this phase ships only that
            // counted *signal*, not an "actually unstarves the event loop"
            // capability.
            let _armed = self.watchdog.arm_guard(self.cpu_time_slice);

            // Phase 7 (`PLAN.md`, Phase 7/RNF04): the *inner* `catch_unwind`
            // that phase's "Narrowed scope" note calls for. `self.handle`
            // already turns a raised Ruby exception (or any other
            // magnus/Ruby-side failure -- a failed type conversion, a
            // funcall on a method that doesn't exist) into `Err`, never a
            // panic: a Ruby exception propagating out of `app.funcall`
            // arrives here as `Err(magnus::Error)`, not as a Rust panic --
            // magnus converts it at the FFI boundary, it does not `longjmp`
            // through Rust frames the way `Thread#kill`'s non-local exit
            // does (see the `watchdog` module and `_serve_native`'s doc
            // comments for that unrelated mechanism). This `catch_unwind`
            // exists for the other kind of fault: a genuine Rust-level
            // panic somewhere in `self.handle`'s own logic (a bug in this
            // crate, or -- for the Phase 7 gate's third fixture row -- the
            // deliberate debug-header panic at the top of `handle`, see its
            // doc comment for why that's the code path that actually needs
            // covering here, not a separately magnus-registered function).
            // Sound to catch here -- verified against the actual call graph
            // inside `self.handle`'s dynamic extent, not assumed from
            // "magnus calls return synchronously" as a blanket rule (false
            // for `read_body`'s `body.block_call("each", ...)`, which does
            // leave a live Ruby/C frame above the passed Rust closure while
            // it runs). There are exactly **two** re-entrant Ruby-to-Rust
            // callbacks that can be live somewhere above a panic reaching
            // this `catch_unwind` while a request is in flight -- both
            // checked, not just the more obvious one:
            //
            // 1. `block_call`'s per-chunk closure (`read_body`, this file).
            //    Magnus wraps it in its *own* `catch_unwind`
            //    (`magnus-0.8.2/src/method.rs`'s `call_handle_error`, via
            //    `Error::from_panic`), converting a panic caught *there*
            //    into a raised Ruby exception before it ever reaches this
            //    crate's frames -- it surfaces to `self.handle` as
            //    `Err(magnus::Error)` from `block_call`'s own return, not as
            //    something unwinding across a live foreign frame here.
            // 2. `watchdog::postponed_job_callback` -- *not* one of magnus's
            //    wrapped call shapes (a raw `unsafe extern "C" fn`
            //    registered by hand via `rb_postponed_job_preregister`, see
            //    the `watchdog` module), and genuinely dispatched by Ruby at
            //    an interrupt checkpoint *inside* `app.funcall("call", env)`
            //    below while `self.watchdog.arm_guard` (above) is armed --
            //    so it does run with a live Ruby/C frame above it, squarely
            //    inside this function's dynamic extent. Sound anyway, but
            //    for a different, narrower reason than (1): that callback's
            //    entire body is one `fetch_add` on an `AtomicU64` -- no
            //    allocation, no Ruby call, no `unwrap`/`expect` -- so it
            //    cannot panic, by construction. This is a real invariant on
            //    `postponed_job_callback` itself, not incidental; see that
            //    function's own doc comment, and keep it panic-free if it's
            //    ever touched (even an `eprintln!` there would reintroduce
            //    finding 1's ENOSPC-panic class of bug, in a spot this
            //    `catch_unwind` cannot help with).
            //
            // Every *other* panic this `catch_unwind` might catch (a bug
            // elsewhere in `self.handle`'s own code, or the deliberate
            // debug-header one above) never had a Ruby/C frame above it in
            // the first place. This is a narrower, call-graph-specific
            // invariant, not "no Rust code here ever sits under a Ruby
            // frame" -- if `self.handle`'s dynamic extent ever gains a
            // *third* re-entrant callback, re-verify it against this same
            // two-case structure (does magnus already wrap it, or is it
            // panic-free by construction); nothing here catches that for
            // you. Unlike `gvl::trampoline`'s outer `catch_unwind`, which
            // guards the actual `extern "C"` boundary handed to
            // `rb_thread_call_with_gvl`/`_without_gvl` and must abort
            // rather than unwind further (there, the caller above it
            // genuinely is opaque C code, every time).
            match panic::catch_unwind(AssertUnwindSafe(|| self.handle(req))) {
                Ok(Ok(response)) => response,
                Ok(Err(err)) => {
                    log_fault("request failed", &err.to_string());
                    error_response()
                }
                Err(payload) => {
                    log_fault("request handler panicked", &panic_payload_message(payload));
                    error_response()
                }
            }
        })
    }
}

/// Whether [`RackAppHandler::handle`]'s deliberate debug-panic header check
/// is armed. Read once (via a `OnceLock`, not on every request -- an
/// `env::var` syscall per request is needless overhead for a flag that
/// cannot change during a server's run) from the `HELIX_RACK_DEBUG_PANIC`
/// environment variable, `true` only for the exact value `"1"`.
///
/// Defaults to disabled (unset, or any value other than `"1"`, including
/// empty) -- a safety-review finding on this phase's first version found
/// that shipping the header check unconditionally in a release build (the
/// artifact `exe/helix_rack`, and this gem, actually load) is a real
/// problem, not just an unlikely one: it's a free remote panic-amplification
/// knob for anyone who can send an HTTP request, and if this crate's build
/// profile ever gains `panic = "abort"` (nothing pins `panic = "unwind"`
/// today), the same header stops being "a caught panic" and becomes a
/// one-request remote process kill. `spec/support/phase7_server_helper.rb`
/// sets this env var when it spawns the Phase 7 gate's subprocess -- no
/// other caller has a reason to.
fn debug_panic_header_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("HELIX_RACK_DEBUG_PANIC").as_deref() == Ok("1"))
}

/// The [`HandlerResponse`] `Handler::call` returns for any fault caught
/// above (a Ruby-side error or a caught Rust panic) -- deliberately the
/// same shape for both over HTTP: a caller has no reliable way to
/// distinguish "the Rack app raised" from "the ext panicked" from outside
/// the process anyway (nor should it need to -- both are the
/// app-observable definition of `RNF04`'s "the server never crashes").
/// `log_fault` (below) is what actually distinguishes the two cases, in the
/// server's own logs, not in what goes out on the wire; PLAN.md's Phase 7
/// gate reads that log (not just this response's status) to tell them
/// apart -- see `spec/support/phase7_server_helper.rb`'s doc comment for
/// why the response alone isn't proof enough.
///
/// Real headers/body (`Content-Type` + `Content-Length`, not the empty
/// values Phase 2's minimal version returned) so `connection::ensure_framing`
/// has something concrete to work with rather than a bodyless 500 that
/// still needs its own framing decision downstream.
fn error_response() -> HandlerResponse {
    const BODY: &[u8] = b"Internal Server Error\n";

    HandlerResponse {
        status: 500,
        headers: vec![
            ("content-type".to_string(), "text/plain; charset=utf-8".to_string()),
            ("content-length".to_string(), BODY.len().to_string()),
        ],
        body: ResponseBody::InMemory(BODY.to_vec()),
    }
}

/// Logs one caught fault to the process's real stderr, not Ruby's
/// `$stderr`/`rack.errors`: deliberately not routed back through any
/// Ruby/magnus call. For the `Err(magnus::Error)` case that would very
/// likely be safe (the Ruby VM's own state is not corrupted by a Rust
/// panic caught entirely within this crate's frames, and any Ruby call
/// that led to `err` had already returned before `handle` produced it) --
/// but for the caught-panic case that safety argument is far less
/// certain (a panic can, in general, fire from a point where some
/// magnus/Ruby-side invariant this crate itself was mid-establishing --
/// e.g. a partially-populated `RHash` -- has not been restored), and this
/// one fault-logging call site has no way to tell which case it's in once
/// it only holds a formatted `String`. Simplest, safe-for-both-cases
/// choice: never call back into Ruby from here at all.
///
/// Writes via `writeln!` on `io::stderr()` (which locks internally),
/// discarding the `io::Result` (`let _ =`), rather than `eprintln!` -- a
/// safety-review finding: `eprintln!` *panics* if the
/// underlying write fails (verified: a disk-full/`ENOSPC` stderr target
/// reproduces this), and this call runs in `Handler::call` *after* its own
/// `catch_unwind` has already returned, still inside the `gvl::with_gvl`
/// closure -- a panic escaping from here would propagate to
/// `gvl::trampoline`'s outer `catch_unwind`, whose only response to a
/// caught panic is `process::abort()`. A log line failing to reach a full
/// disk must never itself be the thing that takes the whole process down;
/// silently dropping that one log line is the correct tradeoff here.
fn log_fault(kind: &str, detail: &str) {
    let _ = writeln!(std::io::stderr(), "[helix_rack] {kind}: {detail}");
}

/// Extracts a human-readable message from a caught panic's payload
/// (`std::panic::catch_unwind`'s `Err` value): the two payload shapes
/// `panic!`/`.unwrap()`/`.expect()` actually produce, `&'static str` (a
/// string-literal panic message) and `String` (a formatted one, e.g. from
/// `panic!("{x}")` or `.expect("...")`), per `std::panic`'s own
/// documentation of what `panic::catch_unwind` returns for the standard
/// panic hook. Anything else (a custom payload type, rare in practice)
/// falls back to a fixed placeholder rather than guessing further.
///
/// Takes the `Box` by value rather than `&(dyn Any + Send)` -- verified by
/// a standalone reproduction that the reference-parameter form is an actual
/// footgun here, not just a style choice: `Box<dyn Any + Send>` is itself
/// `Any` (the blanket `impl<T: 'static> Any for T` applies to the box, not
/// just its contents), so passing `&boxed_payload` to a `&(dyn Any + Send)`
/// parameter type-checks but can unsize-coerce the *outer* `Box` into the
/// trait object instead of deref-coercing to the *inner* value -- every
/// `downcast_ref` then silently misses, with no compiler warning. Taking
/// the payload by value and calling `downcast_ref` on it directly (where
/// method-call auto-deref resolves unambiguously) sidesteps the whole
/// question.
fn panic_payload_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

/// How much of a Rack response body [`read_body`] will accumulate in memory
/// before spilling the rest to a tempfile (see [`BodyAccumulator`] and
/// `PLAN.md`'s Phase 3 "Architecture decision"). 1 MiB: PLAN.md leaves the
/// exact number an implementation choice ("a few hundred KiB to low
/// single-digit MiB"), and 1 MiB is a plain round number in the middle of
/// that range. It's comfortably above typical small API/HTML response
/// bodies (order of KB to low tens of KB) so the common case never touches
/// disk at all -- the whole point of a threshold rather than always
/// spooling -- while still being small enough that even a request that
/// does cross it (this phase's ~200 MB fixture included) spends only a
/// trivial, bounded amount of RAM before switching to the tempfile path for
/// the rest of the body.
const SPOOL_THRESHOLD_BYTES: usize = 1024 * 1024;

/// [`read_body`]'s accumulator: a Rack response body starts `InMemory` and
/// stays there for as long as its accumulated size is under
/// [`SPOOL_THRESHOLD_BYTES`]; the first chunk that would push it over
/// switches to `Spooled`, writing what had accumulated so far plus that
/// chunk to a fresh tempfile, and every chunk after that is written
/// straight to the same file instead of growing memory further. See
/// `PLAN.md`'s Phase 3 "Architecture decision" for why this shape (spool
/// only once a body is *proven* large) was chosen over the two rejected
/// alternatives.
enum BodyAccumulator {
    /// Bytes accumulated so far. Every chunk of a body that never crosses
    /// [`SPOOL_THRESHOLD_BYTES`] ends up here and only here -- no tempfile
    /// is ever created for such a body.
    InMemory(Vec<u8>),
    /// The body has crossed [`SPOOL_THRESHOLD_BYTES`]; every chunk from
    /// here on (including the one that triggered the switch) is written
    /// straight to this file rather than into a growing `Vec`.
    ///
    /// Created via `tempfile::tempfile()`, not `tempfile::NamedTempFile`:
    /// confirmed by reading the `tempfile` 3.27.0 crate's own source
    /// (`src/file/mod.rs`'s `tempfile`/`tempfile_in`, `src/file/imp/unix.rs`'s
    /// `create`) that on Linux it opens the file with `O_TMPFILE` -- an
    /// anonymous inode with no directory entry ever created in the first
    /// place -- falling back, only on filesystems that reject `O_TMPFILE`
    /// (`EOPNOTSUPP`/`EISDIR`/`ENOENT`), to create-then-immediately-`unlink`
    /// (`create_unlinked`), which leaves no directory entry either by the
    /// time this function returns. Either path means there is no
    /// cleanup-on-every-exit-path lifecycle to manage beyond the returned
    /// `File` being dropped normally (which closes its fd; the OS reclaims
    /// the space once the last fd to the anonymous/unlinked inode closes)
    /// -- directly eliminating the disk-cleanup tradeoff PLAN.md's
    /// Phase 3 "Architecture decision" flagged as a real cost of this
    /// design.
    Spooled(std::fs::File),
}

thread_local! {
    /// Scratch space for [`read_body`]'s [`collect_chunk`] callback -- see
    /// `read_body`'s doc comment for why a thread-local instead of a
    /// captured closure. Reset to `InMemory(Vec::new())` at the start of
    /// every `read_body` call.
    static BODY_ACCUMULATOR: std::cell::RefCell<BodyAccumulator> =
        const { std::cell::RefCell::new(BodyAccumulator::InMemory(Vec::new())) };

    /// Reentrancy guard for [`read_body`] -- see its doc comment. `true`
    /// while a `read_body` call is between resetting and reading
    /// [`BODY_ACCUMULATOR`]; a nested call while `true` would otherwise
    /// silently corrupt (not error on) the outer call's collected bytes.
    static READING_BODY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// The block passed to `body.each` by [`read_body`], via `Value::block_call`
/// (a plain, non-capturing `fn` pointer -- see that function's doc comment
/// for why it can't just close over a local accumulator). Appends the bytes
/// of each yielded chunk (must be a `String`, per the Rack Body contract) to
/// [`BODY_ACCUMULATOR`], spilling to a tempfile the moment doing so would
/// push it past [`SPOOL_THRESHOLD_BYTES`] (see [`spool_chunk`]).
fn collect_chunk(ruby: &Ruby, args: &[Value], _block: Option<magnus::block::Proc>) -> Result<(), Error> {
    let Some(&chunk) = args.first() else {
        return Err(Error::new(
            ruby.exception_type_error(),
            "Rack body#each yielded with no argument",
        ));
    };
    let chunk = RString::try_convert(chunk)?;
    BODY_ACCUMULATOR.with(|cell| {
        // SAFETY: the slice is only read (copied into `buf`, or written out
        // to a file, never retained past this call) before any other Ruby
        // call that could mutate or free `chunk` gets a chance to run --
        // same invariant this callback has always relied on, now inside
        // `spool_chunk`. Neither `tempfile::tempfile()` nor `File::write_all`
        // call back into Ruby.
        unsafe { spool_chunk(ruby, &mut cell.borrow_mut(), chunk.as_slice()) }
    })
}

/// Appends one yielded chunk's `bytes` to `acc` (see [`BodyAccumulator`]):
/// while still `InMemory`, grows the buffer in place unless doing so would
/// push the accumulated total past [`SPOOL_THRESHOLD_BYTES`], in which case
/// it spills everything accumulated so far -- plus `bytes` -- to a fresh
/// tempfile and switches `acc` to `Spooled` for the rest of this body; once
/// `Spooled`, every further chunk (this one included, on the transition
/// call) is written straight to that file instead.
///
/// File I/O (`tempfile::tempfile()`, `write_all`) can fail (disk full,
/// permissions) -- surfaced here as a real `magnus::Error`, matching the
/// pattern this file already uses elsewhere for I/O failures, rather than
/// panicking or silently dropping bytes.
fn spool_chunk(ruby: &Ruby, acc: &mut BodyAccumulator, bytes: &[u8]) -> Result<(), Error> {
    let io_error = |e: std::io::Error| Error::new(ruby.exception_runtime_error(), e.to_string());

    match acc {
        BodyAccumulator::InMemory(buf) => {
            if buf.len() + bytes.len() > SPOOL_THRESHOLD_BYTES {
                let mut file = tempfile::tempfile().map_err(io_error)?;
                file.write_all(buf).map_err(io_error)?;
                file.write_all(bytes).map_err(io_error)?;
                *acc = BodyAccumulator::Spooled(file);
            } else {
                buf.extend_from_slice(bytes);
            }
        }
        BodyAccumulator::Spooled(file) => {
            file.write_all(bytes).map_err(io_error)?;
        }
    }
    Ok(())
}

/// Reads a Rack body (an object responding to `#each`, most commonly an
/// `Array` of `String`s for a small response, or an `Enumerator`-like
/// object yielding many chunks for a large/streaming one) by calling
/// `#each` and accumulating every yielded chunk's bytes -- either fully in
/// memory, or spooled to a tempfile past [`SPOOL_THRESHOLD_BYTES`] (see
/// [`BodyAccumulator`] and `PLAN.md`'s Phase 3 "Architecture decision").
/// Returns the resulting [`ResponseBody`] directly: `InMemory` for a body
/// that never crossed the threshold, `Spooled` for one that did.
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
/// into the [`BODY_ACCUMULATOR`] thread-local instead -- sound here because
/// exactly one native OS thread ever runs Ruby code in this whole engine
/// (PRD.md RNF01) and, per [`READING_BODY`]'s guard below, `read_body`
/// refuses to recurse into itself rather than silently corrupting a
/// concurrently-in-progress call's collected bytes. Reentrancy is not
/// hypothetical: a Rack app whose response body's `#each` itself triggers
/// another request through this same server (e.g. by calling
/// `HelixRack.serve` again, or anything else that pumps the event loop)
/// would hit this without the guard.
fn read_body(ruby: &Ruby, body: Value) -> Result<ResponseBody, Error> {
    if READING_BODY.with(|cell| cell.replace(true)) {
        return Err(Error::new(
            ruby.exception_runtime_error(),
            "HelixRack: a Rack response body's #each triggered another request body read on \
             the same thread before the first one finished -- refusing rather than silently \
             corrupting either body",
        ));
    }
    // Guard, not a bare reset at the end: `?` below can return early, and
    // both the reentrancy flag and any partial accumulator state must come
    // back down on that path too -- otherwise a chunk write failing mid-
    // spool (disk full, fd exhaustion) would leave BODY_ACCUMULATOR holding
    // an open fd on a partially-written tempfile until the *next*
    // read_body call happens to overwrite it (line below), rather than
    // freeing it as soon as this request is done with it. On the success
    // path this just re-overwrites an already-fresh-empty accumulator
    // (harmless) after the real result has already been taken out below.
    struct ResetOnDrop;
    impl Drop for ResetOnDrop {
        fn drop(&mut self) {
            READING_BODY.with(|cell| cell.set(false));
            BODY_ACCUMULATOR.with(|cell| *cell.borrow_mut() = BodyAccumulator::InMemory(Vec::new()));
        }
    }
    let _reset = ResetOnDrop;

    BODY_ACCUMULATOR.with(|cell| *cell.borrow_mut() = BodyAccumulator::InMemory(Vec::new()));

    let _: Value = body.block_call("each", (), collect_chunk)?;

    Ok(BODY_ACCUMULATOR.with(|cell| {
        let taken = std::mem::replace(&mut *cell.borrow_mut(), BodyAccumulator::InMemory(Vec::new()));
        match taken {
            BodyAccumulator::InMemory(buf) => ResponseBody::InMemory(buf),
            BodyAccumulator::Spooled(file) => ResponseBody::Spooled(file),
        }
    }))
}

/// `HelixRack._serve_native(app, port, bind, keep_alive_timeout,
/// max_keepalive, cpu_time_slice_ms)` (see `lib/helix_rack.rb`): binds a TCP
/// listener on `bind`:`port` and runs `engine::serve` to completion,
/// blocking the calling (Ruby-owning) thread for as long as it runs -- see
/// this module's top doc comment for why that's correct for this phase.
///
/// `keep_alive_timeout_seconds` and `max_keepalive` implement `PLAN.md`'s
/// Phase 4 (PRD.md section 6.2's `--keep-alive-timeout`/`--max-keepalive`
/// CLI flags, threaded here from `exe/helix_rack` via `lib/helix_rack.rb`) --
/// see `engine::serve`/`connection::handle`'s doc comments for their exact
/// semantics; this function only converts and forwards them.
///
/// `cpu_time_slice_ms` implements `PLAN.md`'s Phase 6 (PRD.md section 6.2's
/// `--cpu-time-slice`, threaded the same way): a `watchdog::Watchdog` is
/// spawned here, once per call, for `RackAppHandler` to arm/disarm around
/// each request (see `watchdog`'s and `RackAppHandler::call`'s doc
/// comments), and is always shut down and joined before this function
/// returns -- on every path, including an error from `listener.bind`/`serve`
/// -- so no watchdog thread outlives the `_serve_native` call that spawned
/// it (this matters more than usual for a background thread in this
/// codebase: `spec/support/phase6_server_helper.rb`'s test harness boots and
/// kills many short-lived servers in the same RSpec process, and a leaked
/// watchdog thread per example would accumulate for the rest of the run).
///
/// `grace_period_seconds` implements `PLAN.md`'s Phase 8 (PRD.md section
/// 6.2's `--grace-period`, threaded the same way): bounds how long the
/// drain step (this function's own doc comment on the `tokio::select!`
/// below) waits for already-accepted connections to finish once shutdown
/// starts.
// `ruby` isn't a caller-facing argument (magnus supplies it, per the
// `#[magnus::init]`-adjacent `function!` convention already used for every
// other `Error`-returning native entry point in this file) -- seven real
// config knobs is what PRD.md section 6.2's table asks this function to
// accept and validate in one place, not a natural grouping to split up.
#[allow(clippy::too_many_arguments)]
fn _serve_native(
    ruby: &Ruby,
    app: Value,
    port: i64,
    bind: String,
    keep_alive_timeout_seconds: i64,
    max_keepalive: i64,
    cpu_time_slice_ms: i64,
    grace_period_seconds: i64,
) -> Result<(), Error> {
    let port = u16::try_from(port).map_err(|_| {
        Error::new(
            ruby.exception_arg_error(),
            format!("port {port} is not a valid TCP port (0-65535)"),
        )
    })?;
    let keep_alive_timeout_seconds = u64::try_from(keep_alive_timeout_seconds).map_err(|_| {
        Error::new(
            ruby.exception_arg_error(),
            format!("keep_alive_timeout {keep_alive_timeout_seconds} must not be negative"),
        )
    })?;
    let max_keepalive = usize::try_from(max_keepalive).map_err(|_| {
        Error::new(
            ruby.exception_arg_error(),
            format!("max_keepalive {max_keepalive} must not be negative"),
        )
    })?;
    // Rejected here, at the configuration boundary, rather than given some
    // in-loop meaning: `connection::handle` always answers the request it
    // already parsed off the wire before it has any chance to check this
    // value (there's no sensible way to reject a request that's already
    // been read), so 0 can't mean "answer none" -- better to refuse a
    // config value with no coherent meaning than silently treat it as 1.
    if max_keepalive == 0 {
        return Err(Error::new(
            ruby.exception_arg_error(),
            "max_keepalive must be at least 1 (0 has no coherent meaning: a connection always \
             answers the request it already read off the wire before this limit is checked)"
                .to_string(),
        ));
    }
    let keep_alive_timeout = Duration::from_secs(keep_alive_timeout_seconds);
    let cpu_time_slice_ms = u64::try_from(cpu_time_slice_ms).map_err(|_| {
        Error::new(
            ruby.exception_arg_error(),
            format!("cpu_time_slice {cpu_time_slice_ms} must not be negative"),
        )
    })?;
    let cpu_time_slice = Duration::from_millis(cpu_time_slice_ms);
    let grace_period_seconds = u64::try_from(grace_period_seconds).map_err(|_| {
        Error::new(
            ruby.exception_arg_error(),
            format!("grace_period {grace_period_seconds} must not be negative"),
        )
    })?;
    let grace_period = Duration::from_secs(grace_period_seconds);

    // Every Ruby/magnus-API call this function needs happens *before*
    // `watchdog::Watchdog::spawn` below, not after -- see that call's own
    // comment for why. `RackAppHandler::new` itself (right after) makes
    // none at all.
    RackAppHandler::prepare_app(ruby, app)?;

    // A safety-review finding: a `Thread#kill` pending against this calling
    // Ruby thread can be delivered at essentially any call back into Ruby's
    // VM (any magnus/`ruby.*` call), via the same non-local-exit mechanism
    // documented in detail on the `gvl::without_gvl` call far below (a
    // longjmp-like exit that does not run Rust's normal unwind/`Drop`
    // machinery) -- not only at the one specific checkpoint that call's
    // comment documents. Reproduced independently: a boot/kill loop with
    // `RackAppHandler::new`'s old `ruby.require("stringio")?` still placed
    // *after* `Watchdog::spawn` leaked a watchdog thread as early as the
    // second iteration -- well before ever reaching `without_gvl`, so
    // `WatchdogGuard`'s `Drop` (see its doc comment) doesn't help there
    // either, since the same non-Rust-unwinding exit skips it too. Moving
    // the only Ruby/magnus call in this stretch before `Watchdog::spawn`
    // (so there's no Ruby call, and so no checkpoint, between spawning the
    // watchdog and reaching `without_gvl`) measurably shrank the window --
    // re-tested the same way, 1 leak in 20 boot/kill iterations, down from
    // roughly 1 in 2 before this reordering -- but did **not** eliminate
    // it: something can still deliver a kill signal into this stretch of
    // plain Rust/OS code with no Ruby call in it at all, which this
    // investigation did not fully root-cause (a plausible but unconfirmed
    // candidate: MRI's own timer-thread-driven async interrupt checks,
    // which may not be gated on an active C-API call the way VM bytecode-
    // dispatch checkpoints are). Left as a known, rare, low-severity residual
    // gap rather than silently claimed as fixed: one idle watchdog thread
    // leaking on an unlucky `Thread#kill` timing during server *startup*
    // specifically (not steady-state operation) is a one-time, bounded cost
    // per occurrence, not an unbounded leak -- but it is real, and worth a
    // second look before this mechanism is trusted for, e.g., a supervisor
    // that boots/kills HelixRack servers in a tight loop.
    let (watchdog, watchdog_guard) = watchdog::Watchdog::spawn();
    let handler: Rc<dyn Handler> = Rc::new(RackAppHandler::new(
        app,
        port,
        Arc::clone(&watchdog),
        cpu_time_slice,
    ));
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
    // itself never returns on its own, so it's raced against `cancelled`,
    // which resolves once either `without_gvl`'s unblock function has fired
    // (`Thread#kill`, in tests) or a `SIGTERM`/`SIGINT` trap has called
    // `request_shutdown` directly (Phase 8, `PLAN.md`, Phase 8/RNF05,
    // `lib/helix_rack.rb`'s `install_shutdown_traps`) -- read `cancelled`'s
    // own doc comment before touching either path: this two-source design
    // exists because the unblock-function path alone was measured to be
    // permanently unreliable for a signal arriving during nested Ruby-level
    // blocking I/O, not merely slow, and that finding (plus the fix) is
    // documented there in full rather than duplicated here.
    //
    // Once `cancelled` does resolve, `select!` dropping the losing
    // `serve(...)` future closes `listener` (bound inside that same
    // future), which stops the accept loop from taking any *new*
    // connection -- what `select!` alone does *not* do is let each
    // already-`spawn_local`'d, still-in-flight connection task keep running
    // afterward; that's `drain` below. And per `cancelled`'s doc comment,
    // `select!` itself cannot even be polled again to notice `cancelled`
    // resolving while a connection's `Handler::call` is synchronously
    // executing -- "stop accepting new connections" in practice means "once
    // whatever was already in flight when the signal arrived finishes, not
    // a moment before"; read `PLAN.md`'s Phase 8 Resolution note before
    // assuming otherwise.
    //
    // `watchdog_guard` is dropped *explicitly, inside* this closure, right
    // after `block_on` returns and before the closure itself returns --
    // not left to run "whenever its scope naturally ends" (which, for a
    // value moved into this closure, would be right here anyway on the
    // normal path, but the explicit `drop` documents that the timing is
    // load-bearing, not incidental). Verified empirically that placement
    // matters, the hard way: an earlier version of this function (before
    // `WatchdogGuard` existed, with a manual `shutdown`/`join` call) put
    // that call after `gvl::without_gvl` returned instead of inside its
    // closure, and a throwaway same-process script that booted and
    // `Thread#kill`ed a server in a loop (the same pattern `spec/support/
    // phase6_server_helper.rb`'s `ensure` block uses) showed one extra
    // `helix_rack-watchdog` OS thread left behind per iteration
    // (`/proc/self/task`, thread names via `/proc/self/task/<tid>/comm`) --
    // every one of them still alive, never cleaned up. Root cause:
    // `rb_thread_call_without_gvl` (inside `gvl::without_gvl`) reacquires
    // the GVL before it returns to its Rust caller, and GVL reacquisition is
    // itself one of Ruby's interrupt-checkpoints -- if the calling thread
    // has a pending `Thread#kill` at that point, Ruby delivers it there via
    // a non-local exit (not normal Rust unwinding, and not something
    // `WatchdogGuard`'s `Drop` would run for either -- a non-local exit does
    // not unwind the Rust stack the way a panic does), which skips every
    // remaining Rust statement in this function, including the watchdog
    // cleanup that used to sit right after this call. This was already true
    // of Phase 2/5's implementation (the only code after `without_gvl` was a
    // trivial `result.map_err`, nobody could observe it being skipped), and
    // would have been just as true of this closure's own `runtime.block_on`
    // line had it not been reached before that GVL-reacquire step --
    // confirmed with `eprintln!` tracing that `block_on` reliably returns
    // (the boundary this closure runs inside is not itself skippable), only
    // code placed *after* the whole `without_gvl` call is at risk. Nothing
    // about shutting down/joining an OS thread needs the GVL, so running it
    // here, still inside the GVL-released region, is both correct and the
    // fix -- `WatchdogGuard` additionally covers every *other* early-return
    // path out of this function (a fallible step between `Watchdog::spawn`
    // and reaching this closure) automatically, which a manual call here
    // never could; see that type's doc comment.
    let local_set = tokio::task::LocalSet::new();
    let result: std::io::Result<()> = gvl::without_gvl(|cancel| {
        let result = runtime.block_on(local_set.run_until(async move {
            let listener = tokio::net::TcpListener::bind((bind.as_str(), port)).await?;
            // A second `Arc` clone, distinct from the one `serve` (below)
            // takes ownership of: `drain` (below) needs `connections` to
            // still be alive and readable *after* `serve`'s future is
            // dropped as `select!`'s losing branch, not just while it's
            // running.
            let connections_for_drain = Arc::clone(&connections);
            let served = tokio::select! {
                res = serve(listener, connections, handler, max_keepalive, keep_alive_timeout) => res,
                () = cancelled(cancel) => Ok(()),
            };

            // Phase 8 (`PLAN.md`, Phase 8/RNF05)'s actual deliverable: once
            // `listener` has already stopped accepting (whether `serve`
            // itself returned an `Err`, which is not expected in practice,
            // or -- the real case this exists for -- `cancelled` won that
            // race and dropped `serve`'s future, closing `listener` with
            // it), give every connection `spawn_local`'d before that point
            // a chance to actually finish its in-flight response, bounded
            // by `grace_period`, before this whole future -- and the
            // `LocalSet` driving those tasks along with it -- returns and
            // drops them. Runs even on `serve`'s `Err` path too: a
            // connection accepted just before a bind/accept-level error
            // still deserves its grace period, not an unconditional cut.
            connections_for_drain.drain(grace_period).await;

            served
        }));

        drop(watchdog_guard);

        result
    });

    result.map_err(|e| Error::new(ruby.exception_runtime_error(), e.to_string()))
}

/// Polls `cancel` (set by `gvl::without_gvl`'s unblock function, e.g. when
/// something `Thread#kill`s the calling Ruby thread) **and**
/// [`SHUTDOWN_REQUESTED`] (set directly by [`request_shutdown`], called
/// from the `SIGTERM`/`SIGINT` trap `lib/helix_rack.rb`'s
/// `HelixRack.serve` installs -- Phase 8, `PLAN.md`, Phase 8/RNF05) every
/// 20ms, resolving once either is `true`.
///
/// Two independent sources feeding one check, not one mechanism reused,
/// because Phase 8's own gate development found `cancel` alone is not
/// reliable for the signal case -- the central finding of this phase,
/// worth the long account below since it directly shapes why this
/// function looks the way it does.
///
/// **What's solid, verified two ways:** while *any* connection's
/// `Handler::call` (`RackAppHandler`, this file) is synchronously
/// executing, this whole runtime's single OS thread (PRD.md RNF01,
/// `PLAN.md`'s Phase 2 architecture note) cannot make progress on anything
/// else -- not the accept loop, not this very function's own 20ms timer --
/// since `Handler::call` is a plain synchronous Rust function with no
/// `.await` point in it. Ruby's own GVL release during a handler's
/// blocking I/O (verified in Phase 5) helps a *separate* Ruby `Thread` on
/// a *separate* OS thread -- it does nothing for *this* OS thread's own
/// stalled reactor.
///
/// **What's the actual Phase 8 finding, also verified, not merely
/// theorized:** that alone would only explain a *bounded* delay (the
/// signal gets noticed once the in-flight call returns). Direct
/// measurement showed something stronger and worse: if a real `SIGTERM`
/// arrives while a handler is blocked inside a *nested* Ruby-level
/// blocking I/O call (this project's Phase 8 fixture: a `TCPSocket#read`,
/// itself internally GVL-releasing, invoked from inside this file's own
/// `gvl::with_gvl` reacquisition, itself inside the outer
/// `gvl::without_gvl` span `cancel`'s UBF is registered against), `cancel`
/// can fail to flip at all -- confirmed with a throwaway script that sent
/// `SIGTERM` in that exact window, then let the process sit completely
/// idle (no further requests, no polling) for 90 full seconds: `cancel`
/// never flipped once, for the rest of that process's life. Two control
/// experiments with the same script shape -- a `SIGTERM` sent after
/// handling one *fast* request first, and one sent after a request that
/// merely `sleep`s (also GVL-releasing, but not nested Ruby I/O) --
/// resolved promptly every time, ruling out "any prior GVL release" or
/// general system load as the cause; the specific nested-I/O-at-
/// signal-time window is what reproduces it. The precise MRI-internal
/// mechanism (plausible candidate: the inner blocking read's own,
/// separately-registered UBF somehow never yields the "active for signal
/// delivery" slot back to the outer `without_gvl`'s UBF once it returns)
/// was not fully root-caused -- this doc comment states what was
/// *measured*, not a confirmed CRuby internals explanation.
///
/// **The fix, not a workaround:** don't depend on `cancel`/the UBF path
/// for the signal case at all. `install_shutdown_traps`'s trap body calls
/// [`request_shutdown`] directly -- a plain magnus-registered function
/// call from Ruby code that's already running (confirmed, every test run,
/// that the trap block itself always does run promptly even when `cancel`
/// never flips), setting a flag this function polls independently of
/// whatever state the UBF path is in. `cancel` still matters for
/// `Thread#kill`-driven cancellation (`spec/support/phase*_server_helper.rb`'s
/// test harnesses), which was never observed to have this failure mode --
/// but is no longer this function's only way to learn about a signal.
async fn cancelled(cancel: &AtomicBool) {
    while !cancel.load(Ordering::SeqCst) && !SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Process-global, not per-`_serve_native`-call -- unlike `cancel`, which
/// `gvl::without_gvl` allocates fresh per call, this must be settable from
/// a Ruby trap context that has no access to any one call's own local
/// state. Reset by [`reset_shutdown_request`], set by [`request_shutdown`],
/// read only by [`cancelled`].
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

/// `HelixRack._request_shutdown` -- called directly from the Ruby-level
/// `SIGTERM`/`SIGINT` trap `lib/helix_rack.rb`'s `install_shutdown_traps`
/// installs, **not** from anywhere else. See [`cancelled`]'s doc comment
/// for the full account of why a second, UBF-independent cancellation
/// signal exists at all: a real `SIGTERM` arriving while a handler is
/// blocked in nested Ruby-level I/O was measured to permanently prevent
/// the existing `cancel`/UBF path from ever flipping, and this function is
/// the fix, not a fallback for a merely-slow case. Deliberately trivial
/// (one atomic store, no allocation, nothing that can raise) -- it runs
/// from inside a Ruby trap context, and this project's own
/// `watchdog::postponed_job_callback` doc comment already establishes why
/// a callback MRI can invoke at an arbitrary, hard-to-reason-about point
/// should stay that simple.
fn request_shutdown() {
    SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
}

/// `HelixRack._reset_shutdown_request` -- called from `lib/helix_rack.rb`'s
/// `HelixRack.serve`, *before* `install_shutdown_traps` installs the traps
/// that can call [`request_shutdown`], and before `_serve_native` runs.
///
/// A safety-review finding corrected this call site's placement from an
/// earlier version, worth recording since the fix direction is
/// counter-intuitive at first: the earlier version reset from *inside*
/// `_serve_native`, *after* `install_shutdown_traps` had already run in
/// `HelixRack.serve` -- reasoning (wrongly) that the risk was a signal
/// landing in the gap between the reset and the accept loop starting. Direct
/// tracing of the actual code path showed the real, silent gap runs the
/// *other* direction: `install_shutdown_traps` armed the traps first, so
/// any signal landing between that and the (later) reset call had its
/// `SHUTDOWN_REQUESTED.store(true, ...)` immediately erased by the reset
/// that ran after it -- not delayed, erased, requiring a second signal to
/// notice anything at all. Worse under Kubernetes than it sounds: `serve`
/// is typically called from a background thread in this project's own
/// same-process test harnesses (`spec/support/phase{2,5,6}_server_helper.rb`),
/// and MRI runs trap bodies on the *main* thread -- so the old ordering was
/// a genuine cross-thread race, not a same-thread instruction gap. Resetting
/// before the traps are even installed closes it: nothing can call
/// `request_shutdown` for *this* server run until after this has already
/// run.
fn reset_shutdown_request() {
    SHUTDOWN_REQUESTED.store(false, Ordering::SeqCst);
}

/// `HelixRack._io_backend_probe` -- Phase 9 (`PLAN.md`, Phase 9): relays
/// `helixrack_engine::probe_io_backend`'s real, syscall-based io_uring
/// capability check out to Ruby. Diagnostic only, per that function's own
/// doc comment -- this does not change what `_serve_native`'s accept loop
/// actually does; it always runs on Tokio's own (epoll, on Linux) reactor
/// regardless of this value.
fn io_backend_probe() -> String {
    probe_io_backend().as_str().to_string()
}

#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let module = ruby.define_module("HelixRack")?;
    module.define_module_function("_serve_native", magnus::function!(_serve_native, 7))?;
    module.define_module_function(
        "_postponed_job_count",
        magnus::function!(watchdog::postponed_job_count, 0),
    )?;
    module.define_module_function("_request_shutdown", magnus::function!(request_shutdown, 0))?;
    module.define_module_function(
        "_reset_shutdown_request",
        magnus::function!(reset_shutdown_request, 0),
    )?;
    module.define_module_function("_io_backend_probe", magnus::function!(io_backend_probe, 0))?;
    // Phase 6 (`PLAN.md`, Phase 6): pre-registers the postponed job callback
    // exactly once, at extension load time -- see `watchdog::register`'s doc
    // comment for why it belongs here rather than in `_serve_native`.
    watchdog::init();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rb_sys_test_helpers::ruby_test;

    /// Exercises [`READING_BODY`]'s guard directly, rather than through
    /// genuine Ruby-level reentrancy (a Rack body's `#each` recursively
    /// calling back into `HelixRack.serve`) -- that would need a second
    /// magnus-exposed entry point and a running server just to set up, when
    /// the guard's own state machine is the actual unit this regression
    /// protects. Simulates "already inside a `read_body` call" by setting
    /// the flag directly before calling, matching what a real reentrant
    /// call would see.
    #[ruby_test]
    fn read_body_rejects_reentrant_calls() {
        let ruby = unsafe { Ruby::get_unchecked() };
        let body: Value = ruby.eval(r#"["chunk"]"#).expect("build a one-element Array body");

        READING_BODY.with(|cell| cell.set(true));
        let result = read_body(&ruby, body);
        READING_BODY.with(|cell| cell.set(false));

        assert!(
            result.is_err(),
            "expected read_body to refuse a reentrant call, got: {result:?}"
        );
    }

    #[ruby_test]
    fn read_body_succeeds_normally_and_resets_the_guard_afterward() {
        let ruby = unsafe { Ruby::get_unchecked() };
        let body: Value = ruby.eval(r#"["a", "b"]"#).expect("build a two-element Array body");

        let result = read_body(&ruby, body).expect("a non-reentrant call should succeed");

        match result {
            ResponseBody::InMemory(bytes) => assert_eq!(bytes, b"ab"),
            ResponseBody::Spooled(_) => {
                panic!("a 2-byte body is far under SPOOL_THRESHOLD_BYTES, expected InMemory")
            }
        }
        assert!(
            !READING_BODY.with(|cell| cell.get()),
            "the guard must reset back to false after a normal call completes"
        );
    }

    /// Exercises the actual spill-to-tempfile path (see [`BodyAccumulator`],
    /// [`spool_chunk`]): a body whose accumulated bytes cross
    /// [`SPOOL_THRESHOLD_BYTES`] must come back as `ResponseBody::Spooled`,
    /// with the file's contents matching every chunk yielded, in order --
    /// not just "spooled to *some* file", since a wrong offset/ordering bug
    /// in `spool_chunk` wouldn't otherwise be caught by
    /// `read_body_succeeds_normally_and_resets_the_guard_afterward` above
    /// (that test's body never crosses the threshold).
    #[ruby_test]
    fn read_body_spills_to_a_tempfile_past_the_threshold() {
        use std::io::{Read, Seek, SeekFrom};

        let ruby = unsafe { Ruby::get_unchecked() };
        // Two chunks whose combined length exceeds SPOOL_THRESHOLD_BYTES --
        // a single Ruby String literal that size would be unwieldy to write
        // out here, so this builds it via `"x" * n` instead.
        let chunk_len = SPOOL_THRESHOLD_BYTES;
        let body: Value = ruby
            .eval(&format!(r#"["x" * {chunk_len}, "y" * {chunk_len}]"#))
            .expect("build a two-chunk Array body that crosses the spool threshold");

        let result = read_body(&ruby, body).expect("a non-reentrant call should succeed");

        let mut file = match result {
            ResponseBody::Spooled(file) => file,
            ResponseBody::InMemory(bytes) => panic!(
                "expected Spooled once the body's {} bytes crossed SPOOL_THRESHOLD_BYTES \
                 ({SPOOL_THRESHOLD_BYTES}), got InMemory({} bytes)",
                chunk_len * 2,
                bytes.len()
            ),
        };

        file.seek(SeekFrom::Start(0)).expect("seek spooled file to start");
        let mut contents = Vec::new();
        file.read_to_end(&mut contents).expect("read spooled file contents");

        let mut expected = vec![b'x'; chunk_len];
        expected.extend(vec![b'y'; chunk_len]);
        assert_eq!(contents, expected);
    }
}
