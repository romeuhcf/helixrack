#!/bin/sh
# Phase 13 (PLAN.md, Phase 13): selects which server and which scenario app
# to boot, via BENCH_SERVER/BENCH_APP env vars -- bench/run.rb sets both per
# container it starts. Always binds 0.0.0.0:9292 inside the container;
# bench/run.rb maps that to a host port per run.
set -eu

app_path="bench/apps/${BENCH_APP}.ru"

case "$BENCH_SERVER" in
  helix_rack)
    exec bundle exec exe/helix_rack -a "$app_path" -p 9292 -b 0.0.0.0
    ;;
  puma)
    # Puma's own defaults otherwise (single process, 0:16 threads) -- the
    # realistic "out of the box" config an operator would actually run, not
    # artificially constrained to match HelixRack's own single-request-at-a-
    # time design (that would bias the comparison, not make it fairer).
    exec bundle exec puma -b "tcp://0.0.0.0:9292" -e production "$app_path"
    ;;
  falcon)
    # `--threaded -n 1`, not Falcon's own default (`--forked -n 8`):
    # forking 8 worker processes onto a container limited to one vCPU
    # (PRD.md 7.2's own stated constraint) isn't a configuration any
    # operator would actually choose -- one fiber-scheduled process sized to
    # the resource limit is the realistic comparison here.
    exec bundle exec falcon serve -b "http://0.0.0.0:9292" -c "$app_path" --threaded -n 1
    ;;
  *)
    echo "Unknown BENCH_SERVER: $BENCH_SERVER (expected helix_rack, puma, or falcon)" >&2
    exit 1
    ;;
esac
