# Ruby (MRI/CRuby) concurrency and GVL findings

Verified discoveries from building HelixRack's Rust/magnus extension (`ext/helix_rack/`), not
textbook claims. Each item was checked against this project's own generated rb-sys bindings, the
installed Ruby 4.0.6 headers, magnus 0.8.2's own source, or direct empirical reproduction (a
throwaway script, not assumption) before being relied on. See `PLAN.md`'s per-phase "Resolution"
notes for the full narrative each of these came from.

## GVL release/reacquire

- `rb_thread_call_without_gvl` / `rb_thread_call_with_gvl` are not wrapped by magnus 0.8.2 (listed,
  unimplemented, in its own source) -- calling them needs raw `rb-sys` FFI.
- `rb_thread_call_without_gvl` reacquires the GVL *before* returning to its Rust caller. GVL
  reacquisition is itself one of Ruby's interrupt checkpoints.
- Two variants exist: `rb_thread_call_without_gvl` reacts to signals/`Thread#kill`/etc.;
  `rb_thread_call_without_gvl2` explicitly does not. There is no faster variant that still reacts to
  signals -- if you need signal reactivity, you pay whatever latency that reactivity costs (see
  "Signal-to-unblock latency is not bounded" below).
- A blocking read on a real `IO` (an anonymous `IO.pipe`, a `TCPSocket`) genuinely releases the GVL
  for other Ruby threads to run -- verified by direct reproduction (a two-thread script, one blocked
  on the read, the other polling), not assumed from documentation.

## `Thread#kill` / signal delivery is a non-local exit, not normal unwinding

- A pending `Thread#kill` (or any pending interrupt) delivered while a thread reacquires the GVL
  after `rb_thread_call_without_gvl` arrives via a longjmp-style non-local exit -- it does **not**
  run Rust's normal unwind/`Drop` machinery. Any cleanup code placed textually after such a call,
  or relying on `Drop` to run during that specific transition, can be silently skipped.
- This is a genuinely different mechanism from a Ruby exception raised through `funcall`/`block_call`
  (see "magnus's own panic containment" below) -- that path returns a normal `Err`, doesn't skip
  Rust code, and doesn't need this caveat.
- Reproduced concretely: a boot/kill loop leaked one watchdog OS thread per iteration until cleanup
  was moved to run *inside* the `without_gvl` closure, before it returns, rather than after.

## `rb_postponed_job_preregister` / `rb_postponed_job_trigger`

- `rb_postponed_job_trigger` is documented (and cross-checked against the installed header, not
  taken on faith) as async-signal-safe: callable from any thread, at any time, without holding the
  GVL, including from a signal handler.
- The callback it schedules runs at Ruby's next interrupt checkpoint -- which can be **inside** an
  in-progress `funcall`/method call, not only between top-level statements. A postponed-job callback
  must therefore be panic-free by construction (no allocation, no Ruby call, nothing that can
  `unwrap`/`expect`) -- there is no `catch_unwind` boundary protecting it specifically, and a panic
  there would unwind across a live foreign (Ruby VM) frame: undefined behavior.
- The older, deprecated `rb_postponed_job_register`/`_register_one` pair is documented as *not*
  fully async-signal-safe (race conditions with Ruby's own GC), which is why the newer
  preregister/trigger split exists.

## A real signal can permanently fail to flip `rb_thread_call_without_gvl`'s unblock function

Superseded finding, kept for the debugging trail: an early pass at this (Phase 6/7 era) assumed
`Signal.trap` reliably wakes a thread blocked in `rb_thread_call_without_gvl`, just with unbounded,
variable latency (~20ms to several seconds, blamed on generic OS scheduling jitter). Phase 8's own
gate development found something worse and more specific, confirmed with a 90-second
total-silence measurement, not assumed:

- Registering `Signal.trap("TERM"/"INT")` (even with a trivial body) is what gives MRI a reason to
  *attempt* delivering a pending interrupt to a thread blocked in `rb_thread_call_without_gvl` via
  its unblock function -- the trap's own Ruby block running is real evidence of that, confirmed
  every time across many runs.
- But if the signal arrives while that thread is inside **nested** Ruby-level blocking I/O --
  concretely: a Rack handler doing a blocking `TCPSocket#read` (itself internally GVL-releasing),
  invoked from inside a `rb_thread_call_with_gvl` reacquisition, itself inside the outer
  `rb_thread_call_without_gvl` span the unblock function is registered against -- the unblock
  function can fail to fire **at all**, not just late. Verified concretely: sent `SIGTERM` in
  exactly that window, then left the process completely idle (no further requests, no client
  polling of any kind) for 90 full seconds. The unblock function never fired once.
- Two control experiments, same script shape: `SIGTERM` sent after handling one *fast* request
  first, and one sent after a request that only calls `Kernel#sleep` (also GVL-releasing, but not
  nested Ruby I/O). Both resolved promptly and reliably every time. This rules out "any prior GVL
  release" or general system load as the cause -- the specific nested-I/O-at-signal-time window is
  what reproduces it, not scheduling jitter in general (the earlier, superseded finding's
  explanation).
- The exact MRI-internal mechanism was not fully root-caused. A plausible, unconfirmed candidate:
  the inner blocking read's own, separately-registered unblock function never yields the "active
  for signal delivery" slot back to the outer one once it returns.
- The fix that actually worked: stop depending on the unblock-function path for the signal case at
  all. Have the trap call a plain, registered Ruby-callable native function directly (a trivial
  atomic-flag `store`, panic-free by construction) instead of relying on the interrupt/UBF mechanism
  to eventually deliver anything. Confirmed the trap's own Ruby block reliably runs promptly even in
  the exact window where the unblock function was shown to never fire -- so a direct call from
  inside it sidesteps the unreliable path entirely, rather than trying to make that path reliable.
- Practical implication for any Ruby native extension doing signal-driven cancellation of a
  `rb_thread_call_without_gvl` region: **do not rely solely on the unblock-function path if the
  blocked region can contain nested Ruby-level blocking I/O.** Have the signal handler itself set a
  plain flag your blocked code polls, and don't assume "the trap ran" implies "the unblock function
  will run too" -- they were observed to be more loosely coupled than that assumption requires, and
  in the specific case above, only one of the two ever happened.

## magnus's own panic containment

- magnus wraps *every* Ruby-callable function it registers (`define_module_function`, `method!`,
  `function!`, and a `block_call` closure) in its own `catch_unwind`
  (`magnus-0.8.2/src/method.rs`'s `call_handle_error`), converting a caught panic into a raised Ruby
  exception (`Error::from_panic`) before it ever reaches the calling Rust code as a `Err(magnus::Error)`from that call.
- Consequence: a panic inside a function reached *only* via one of those magnus-wrapped call shapes
  never reaches an *outer* `catch_unwind` as a raw panic -- it already arrived as a normal `Err`.
  Testing "does my outer `catch_unwind` catch a real panic" via a separately-registered
  Ruby-callable function does not exercise that outer `catch_unwind` at all; the panic needs to
  originate in code that isn't itself behind one of magnus's own wrapped boundaries.
- `SystemStackError` (Ruby's own stack-overflow guard) is raised as an ordinary exception once the
  VM's C stack limit is hit -- it is not a process signal and does not bypass `funcall`'s normal
  `Err` return path.

## `Box<dyn Any + Send>` downcast footgun

- `Box<T>` implements `Any` itself (the blanket `impl<U: 'static> Any for U` applies to the box, not
  just its contents). Passing `&boxed_value` where a `&(dyn Any + Send)` parameter is expected
  type-checks (via unsize coercion of the *outer* `Box`) but silently produces a value whose
  `downcast_ref::<T>()` always misses, even for the exact type actually inside the box -- no
  compiler warning. Fix: take the `Box` by value and call `downcast_ref` directly on it (method-call
  auto-deref resolves correctly), or explicitly deref (`&*boxed_value`) before passing a reference.
- Reproduced standalone, both the failure and the fix, before touching production code.

## Practical takeaways for this codebase

- Anything that must survive a `Thread#kill`/signal-driven non-local exit needs its cleanup placed
  *before* the last Ruby-reachable checkpoint in the relevant code path, not relegated to `Drop` or
  "runs after this call returns" -- RAII guards still help for genuine Rust-level early returns, but
  do not cover this specific non-local-exit class of control flow.
- A deliberately-panicking test fixture reachable "through" a Ruby app needs to panic in code that
  is *not* itself a magnus-wrapped call boundary, or it will exercise magnus's own panic-to-exception
  conversion instead of whatever custom fault-containment logic is under test.
- A postponed-job or other Ruby-VM-invoked native callback should be reviewed for panic-freedom
  specifically, every time it changes -- there is no generic safety net for that class of callback.
- Don't rely on `rb_thread_call_without_gvl`'s unblock function as the *only* way a signal reaches a
  blocked native call -- it can permanently fail to fire if the blocked region contains nested
  Ruby-level blocking I/O. Have the signal trap call a native function directly to set a flag your
  code polls, independent of whether the unblock function ever runs.
- A single-OS-thread Tokio runtime (this whole engine) cannot make progress on *anything* --
  accept loop, timers, other connections' I/O -- while a synchronous, non-`.await` Rust function
  (like this codebase's `Handler::call`) is executing. This is a deliberate architecture tradeoff
  (PRD.md RNF01), not a bug, but it means "stop accepting new connections" or "notice a shutdown
  signal" cannot be faster than "whatever's already in flight finishes" -- design gates and grace
  periods around that reality, not around an assumed low-latency wakeup.
