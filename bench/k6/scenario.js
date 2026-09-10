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
  // CodeRabbit finding: no request-*correctness* threshold here previously
  // meant a run full of non-200 responses could still produce a "successful"
  // p99 latency number -- fast error responses don't mean what a fast 200
  // means. This only enforces "every response actually succeeded," not
  // PLAN.md's own P99 threshold: `bench/run.rb` still reads the exported
  // summary and applies that gate itself, across the full N-run methodology,
  // not a single run's numbers.
  thresholds: {
    checks: ["rate==1.0"],
  },
};

export default function scenario() {
  const res = http.get(TARGET_URL);
  check(res, { "status is 200": (r) => r.status === 200 });
}
