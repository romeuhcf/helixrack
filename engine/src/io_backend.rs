//! Phase 9 (`PLAN.md`, Phase 9): a real io_uring capability probe.
//!
//! Diagnostic only -- see `PLAN.md`'s Phase 9 "Architecture note" for the
//! researched (not assumed) reason this doesn't actually switch what
//! [`crate::serve`] does: Tokio's own reactor has no io_uring backend at
//! all, and the one real alternative (`tokio-uring`) is a separate runtime
//! with an I/O model incompatible with this engine's zero-copy,
//! borrowed-buffer design. Network I/O always runs on Tokio's own (epoll,
//! on Linux) reactor today, regardless of what [`probe`] reports.

/// Which I/O backend [`probe`] detected as available on the current kernel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoBackend {
    IoUring,
    Epoll,
}

impl IoBackend {
    pub fn as_str(self) -> &'static str {
        match self {
            IoBackend::IoUring => "io_uring",
            IoBackend::Epoll => "epoll",
        }
    }
}

/// Probes for io_uring kernel support via a real `io_uring_setup(2)` syscall
/// attempt -- not a `/proc`/`uname` version guess, and not a hand-rolled
/// `io_uring_params` struct either (that struct's exact ABI -- field order,
/// padding, size -- is exactly the kind of detail worth getting from the
/// same crate family `tokio-uring` itself is built on, `io-uring`, rather
/// than re-deriving by hand and risking a wrong-but-plausible-looking
/// struct that misreports availability).
///
/// `entries` of `1` is the smallest valid ring size for this probe's
/// purposes -- nothing about the probe result depends on it being larger.
/// The ring (and its underlying fd) is dropped immediately after
/// construction, via [`io_uring::IoUring`]'s own `Drop` impl -- no leaked
/// fd regardless of which branch is taken (verified: 50,000 probe/drop
/// cycles left this process's open fd count unchanged).
///
/// Linux-only (see `engine/Cargo.toml`'s matching `cfg` split on the
/// `io-uring` dependency itself) -- io_uring is a Linux-specific kernel
/// interface; every other platform is unconditionally [`IoBackend::Epoll`]
/// below.
#[cfg(target_os = "linux")]
pub fn probe() -> IoBackend {
    match io_uring::IoUring::new(1) {
        Ok(_ring) => IoBackend::IoUring,
        Err(err) => {
            // A safety-review finding, reproduced live (not assumed):
            // `Err` does *not* uniformly mean "this kernel lacks io_uring".
            // Under `ulimit -l 0` (a low `RLIMIT_MEMLOCK`, which container
            // runtimes commonly set and Kubernetes doesn't raise for you)
            // and under real fd exhaustion (`EMFILE`, after opening enough
            // fds to hit the process limit), this same probe -- on a
            // kernel that genuinely does support io_uring, confirmed by
            // the identical binary reporting `io_uring` under normal
            // conditions -- reported `epoll` instead. `io_uring_setup(2)`'s
            // own ERRORS section lists `EMFILE`/`ENFILE`/`ENOMEM` alongside
            // `ENOSYS`/`EPERM`/`EINVAL`; only the latter group means "not
            // available here". The returned value still has to be `Epoll`
            // either way (this probe can't retry with different resource
            // conditions), but silently discarding *which* kind of failure
            // this was would mask the more urgent case: `EMFILE` on this
            // process is also about to break `serve`'s own accept loop,
            // which is a very different problem from "no io_uring".
            if is_resource_exhaustion(&err) {
                eprintln!(
                    "[helixrack-engine] io_uring probe inconclusive ({err}) -- this looks like \
                     resource pressure (fd or memlock limits), not a kernel that genuinely lacks \
                     io_uring support. Reporting epoll, but this may not reflect the real kernel \
                     capability, and the same resource pressure likely affects this process more \
                     broadly."
                );
            }
            IoBackend::Epoll
        }
    }
}

#[cfg(target_os = "linux")]
fn is_resource_exhaustion(err: &std::io::Error) -> bool {
    matches!(
        err.raw_os_error(),
        Some(libc::EMFILE) | Some(libc::ENFILE) | Some(libc::ENOMEM)
    )
}

/// Non-Linux platforms have no io_uring kernel interface at all -- there is
/// nothing to probe, and no `io-uring` dependency even compiled in on this
/// platform (see `engine/Cargo.toml`'s `cfg` split). Always
/// [`IoBackend::Epoll`] -- a slight misnomer off-Linux (Tokio's own `mio`
/// backend there is kqueue on macOS/BSD, IOCP on Windows), but this value
/// only ever feeds a diagnostic string, not a real backend switch, so it
/// stays as the one "not io_uring" answer everywhere.
#[cfg(not(target_os = "linux"))]
pub fn probe() -> IoBackend {
    IoBackend::Epoll
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Not a claim about what *this* machine's kernel supports (that would
    /// make the test environment-dependent) -- just that `probe` always
    /// returns one of the two defined variants and never panics, which is
    /// the one thing safe to assert unconditionally regardless of the CI
    /// kernel's own io_uring support (or, off Linux, regardless of there
    /// being any io_uring at all).
    #[test]
    fn probe_returns_a_defined_backend_without_panicking() {
        let backend = probe();
        assert!(matches!(backend, IoBackend::IoUring | IoBackend::Epoll));
    }

    #[test]
    fn as_str_matches_the_variant() {
        assert_eq!(IoBackend::IoUring.as_str(), "io_uring");
        assert_eq!(IoBackend::Epoll.as_str(), "epoll");
    }
}
