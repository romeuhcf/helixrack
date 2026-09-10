// Phase 13 (PLAN.md, Phase 13): one parameterized k6 script driving every
// (server, scenario) pair `bench/run.rb` benchmarks -- which path it hits
// and how long it runs for come from env vars `bench/run.rb` sets per run,
// not three near-duplicate scripts.
import http from "k6/http";
import { check } from "k6";

const TARGET_URL = __ENV.TARGET_URL;
const VUS = parseInt(__ENV.VUS || "50", 10);
const DURATION = __ENV.DURATION || "15s";

export const options = {
  vus: VUS,
  duration: DURATION,
  // No thresholds here that would make k6 itself exit nonzero on a "bad"
  // result -- bench/run.rb reads the exported summary and applies PLAN.md's
  // actual gate itself, across the full N-run methodology, not a single run.
};

export default function scenario() {
  const res = http.get(TARGET_URL);
  check(res, { "status is 200": (r) => r.status === 200 });
}
