# GKE / lower-level performance tuning wishlist

Not implemented, not benchmarked, not scoped to any phase in `PLAN.md`. A list of tuning levers
below HelixRack's own code that could plausibly move the numbers Phase 13's benchmark measures,
kept here so they aren't lost. Each one is tied to a specific fact already established elsewhere
in this project (PRD.md, PLAN.md's own architecture notes) rather than a generic checklist entry
-- anything added later should keep that discipline: state *why* it would matter for this specific
server, not just that it's a knob that exists.

## Node level

- **Kubernetes CPU Manager, `static` policy.** PRD.md 7.2's own benchmark environment is `cpus:
  "1.0"` -- an integer CPU request/limit is exactly what the `static` CPU Manager policy needs to
  grant a pod an *exclusive*, non-shared physical core rather than a CFS-quota-throttled slice of a
  shared one. HelixRack's whole design is one OS thread doing the accept loop and every request
  handler in sequence (`PLAN.md`'s Phase 2 architecture note) -- CFS throttling jitter on that one
  thread shows up directly as tail-latency noise, which is exactly what a static, pinned core would
  remove. Needs a GKE node pool with `--cpu-manager-policy=static` and pods with CPU
  requests == limits (`Guaranteed` QoS, see below) to be eligible at all.
- **Topology Manager, `single-numa-node` policy**, on multi-socket/multi-NUMA-node machine types --
  keeps the pinned CPU core and its local memory on the same NUMA node, avoiding cross-node memory
  latency for a workload that's already trying to minimize per-request latency variance.
- **Disable SMT (hyper-threading) on the node pool**, or otherwise ensure the pinned core is a full
  physical core, not one hyperthread sibling contending with another pod's. Same jitter argument as
  the CPU Manager point above.
- **Compute-optimized machine types (`c2`/`c3` family), not general-purpose (`n2`/`e2`)** for the
  node pool running this benchmark -- higher sustained per-core clock matters more than core count
  for a workload that's fundamentally single-threaded per pod.
- **Avoid GKE Sandbox (gVisor) node pools** for this workload -- gVisor intercepts syscalls in
  userspace, adding per-syscall overhead directly on the hot path of a server whose entire job is
  syscall-bound network I/O (`accept`/`read`/`write`, per `engine/src/connection.rs`). Worth
  confirming empirically (not done yet) rather than assumed, but the mechanism is a real cost, not
  speculative.

## Pod level

- **`Guaranteed` QoS class**: CPU and memory `requests` == `limits`, matching PRD.md 7.2's `cpus:
  "1.0"` / `memory: "512Mi"` exactly on both. Needed for CPU Manager `static` policy eligibility
  (above), and generally the QoS class least likely to be evicted or CPU-shared under node pressure.
- **`terminationGracePeriodSeconds` matched to `--grace-period`** (Phase 8/RNF05's CLI flag,
  `exe/helix_rack`'s own `--grace-period SECONDS`, default 30). If Kubernetes' own grace period is
  shorter than the value passed to HelixRack, a rolling deployment can SIGKILL the process before
  its own drain (`ConnectionCounter::drain`, `engine/src/lib.rs`) finishes waiting for in-flight
  requests -- silently defeating Phase 8's whole mechanism.
- **A `preStop` hook with a short sleep** before Kubernetes delivers SIGTERM, so the endpoint has
  time to be removed from the Service/load balancer's routing before the pod actually stops
  accepting connections -- avoids a window of dropped connections during rollouts that graceful
  shutdown alone doesn't cover (that gap is between "traffic stops being routed here" and "this pod
  is gone," not between "SIGTERM received" and "drain complete").
- **Readiness/liveness probe timing sized around Phase 6/8's own findings**: the accept loop cannot
  be polled while a handler executes (`PLAN.md`'s Phase 8 "Architecture note" -- a single-OS-thread
  reactor is frozen for the duration of `Handler::call`), so a probe with too tight a timeout could
  spuriously fail during a legitimately slow request rather than a genuinely unhealthy process. Any
  chosen timeout should be justified against the CPU-bound scenario's own measured P99, once that
  exists, not picked arbitrarily.
- **Topology spread constraints / pod anti-affinity** across nodes, to avoid two benchmark pods (or
  a benchmark pod and something else CPU-heavy) landing on the same physical node and contending for
  the same NUMA-local resources the node-level tuning above is trying to isolate.

## Container / language runtime level

- **YJIT enabled (`RUBY_YJIT_ENABLE=1` or `ruby --yjit`)** for the Ruby side of the request path --
  orthogonal to HelixRack's own Rust code, but the Rack/Grape app code every request actually runs
  (param validation, JSON serialization -- exactly Phase 13's CPU-bound scenario) is plain Ruby
  bytecode YJIT can speed up. Plausibly the single highest-leverage item on this whole list for the
  CPU-bound scenario specifically, and untested here.
- **`mimalloc` env var tuning** (Phase 10's allocator, `ext/helix_rack/src/lib.rs`'s
  `GLOBAL_ALLOCATOR`): `MIMALLOC_LARGE_OS_PAGES=1` paired with node-level hugepages support, to
  reduce TLB pressure on allocation-heavy request paths (the CPU-bound scenario builds and discards
  a 2,000-record array per request, per `bench/apps/cpu_bound_app.rb`). Not tried; mimalloc's own
  docs describe the flag, this project hasn't benchmarked it.
- **Ruby GC heap tuning** (`RUBY_GC_HEAP_INIT_SLOTS`, `RUBY_GC_HEAP_GROWTH_FACTOR`, and similar) sized
  to the CPU-bound scenario's own allocation pattern, to trade memory (still bounded by the 512Mi
  pod limit) for fewer GC pauses interrupting request handling.
- **A future multi-threaded Tokio runtime is *not* a drop-in lever here** -- worth recording as a
  limit on this whole list, not just an opportunity: `engine`'s Tokio runtime is deliberately
  `current_thread` (PLAN.md's Phase 2 architecture note), and `Handler::call` assumes it alone holds
  the GVL/runs on the one OS thread Ruby called it from. Raising the pod's CPU limit above `1.0`
  would not by itself let HelixRack use the extra cores -- that would need a real concurrency-safety
  redesign (multiple Ruby-calling OS threads, GVL contention among them), not a config flag.

## System / kernel level

Everything in this section needs node-level (not pod-level) privileges -- a privileged DaemonSet,
custom node image, or GKE node pool config -- so it's the furthest from something this project's own
Kubernetes manifests alone can set.

- **`net.core.somaxconn` / `net.ipv4.tcp_max_syn_backlog`** raised for high-concurrency keep-alive
  workloads -- PRD.md 7.2's own load profile is "100 a 5.000 conexões concorrentes HTTP Keep-Alive,"
  and the default backlog on a stock node image may cap accept-queue depth below what that implies
  under a connection-storm scenario.
- **`net.ipv4.tcp_fin_timeout` reduction**, for faster socket reclaim under the same connection-churn
  profile, particularly relevant given Phase 4's `--max-keepalive`/`--keep-alive-timeout` flags force
  periodic connection turnover by design.
- **IRQ affinity, keeping network interrupt handling off the pinned core** the CPU Manager `static`
  policy (above) gave this pod exclusively -- otherwise the same core doing request handling is also
  fielding NIC interrupts, reintroducing the jitter the CPU pinning was meant to remove. Needs
  `irqbalance` configuration or manual `/proc/irq/*/smp_affinity` tuning at the node level.
- **`vm.swappiness=0`** node-wide, to avoid swap-induced latency spikes under node memory pressure --
  lower-value here than the others since a `Guaranteed`-QoS pod's own cgroup memory limit (512Mi)
  already bounds what it can consume, but still relevant if the node itself is oversubscribed by
  other pods.
