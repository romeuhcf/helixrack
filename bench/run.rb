#!/usr/bin/env ruby
# frozen_string_literal: true

# Phase 13 (PLAN.md, Phase 13): the actual benchmark harness PRD.md 7.2 and
# PLAN.md's Phase 13 section describe -- N repeated runs per (server,
# scenario) pair, in a `--cpus=1.0 --memory=512m` container per PRD.md 7.2's
# stated Kubernetes limits, comparing HelixRack against Puma (PLAN.md's
# actual numeric Gate) and Falcon (PRD.md's third comparison point,
# informational -- the Gate doesn't reference it).
#
# Run with `bundle exec ruby bench/run.rb`. Not part of `bundle exec rspec`/
# `main.yml`'s routine per-PR job: a full run is N=10 runs x 3 scenarios x 3
# servers, each run booting a fresh container and running a real k6 load
# test against it -- real minutes, not a fast unit-test-style check, the
# same reasoning Phase 10/12 already established for scoping a heavy,
# real verification to an explicit, on-demand invocation rather than the
# fast loop.
#
# Known, accepted limitation of running this from a devbox rather than a
# real GKE node (stated honestly, not silently assumed clean): CPU frequency
# scaling and turbo boost are not disabled here -- PLAN.md's Phase 13 text
# asks for that "if the host allows it," and a devbox sandbox does not
# expose that control. The numbers this produces are only as reproducible
# as this specific machine, at this specific time -- exactly the caveat
# PLAN.md's own Phase 13 text already anticipates ("numbers will drift
# machine to machine").

require "json"
require "fileutils"
require "net/http"
require "timeout"

module Bench
  # Comma-separated Cargo features to build the `helix_rack` extension with
  # inside the bench image (e.g. `combine-write`, see `engine/src/
  # connection.rs`'s `write_response`) -- empty (this project's real
  # default) unless set, so a feature-gated optimization can be measured
  # against the default-off baseline independently, with the same
  # methodology, rather than assumed. A distinct image tag per feature set
  # (not always `:latest`) avoids silently reusing a stale image built with
  # a different feature set than the current run asked for.
  CARGO_FEATURES = ENV.fetch("BENCH_CARGO_FEATURES", "")
  IMAGE = CARGO_FEATURES.empty? ? "helixrack-bench:latest" : "helixrack-bench:features-#{CARGO_FEATURES.tr(",", "-")}"
  NETWORK = "helixrack-bench-net"
  POSTGRES_NAME = "helixrack-bench-postgres"
  SERVER_NAME = "helixrack-bench-server"
  RESULTS_DIR = File.expand_path("results", __dir__)
  REPO_ROOT = File.expand_path("..", __dir__)

  # PRD.md 7.2's own stated environment: "Pod Kubernetes limitado a cpus:
  # '1.0' e memory: '512Mi'".
  CPUS = "1.0"
  MEMORY = "512m"

  # PLAN.md's Phase 13 text: "N repeated runs (e.g. N=10) per scenario ...
  # report median and a confidence interval, not a single sample."
  N_RUNS = ENV.fetch("BENCH_N_RUNS", "10").to_i
  VUS = ENV.fetch("BENCH_VUS", "50").to_i
  DURATION = ENV.fetch("BENCH_DURATION", "15s")

  # `BENCH_SERVERS`/`BENCH_SCENARIOS` (comma-separated) narrow a run -- for
  # iterating on this script itself against one (server, scenario) pair
  # rather than the full matrix every time. Unset (the default) runs
  # everything, which is what a real Phase 13 gate run means.
  ALL_SERVERS = %w[helix_rack puma falcon].freeze
  ALL_SCENARIOS = {
    "hello_world" => "/",
    "io_mixed" => "/widgets",
    "cpu_bound" => "/report"
  }.freeze
  SERVERS = ENV.key?("BENCH_SERVERS") ? ENV.fetch("BENCH_SERVERS").split(",") : ALL_SERVERS
  SCENARIOS = if ENV.key?("BENCH_SCENARIOS")
                ALL_SCENARIOS.slice(*ENV.fetch("BENCH_SCENARIOS").split(","))
              else
                ALL_SCENARIOS
              end

  # PLAN.md's Phase 13 "Gate": only Puma is a numeric threshold. Falcon is
  # PRD.md 7.2's third comparison point, reported alongside but not gating.
  GATE_BASELINE_SERVER = "puma"
  GATE_P99_RATIO_MAX = 0.60
  GATE_RSS_RATIO_MAX = 0.50

  module_function

  def run!(*cmd, **opts)
    puts "+ #{cmd.join(" ")}"
    system(*cmd, **opts, exception: true)
  end

  def capture(*cmd)
    IO.popen(cmd, &:read)
  end

  def setup_network
    system("docker", "network", "create", NETWORK, out: File::NULL, err: File::NULL)
  end

  def teardown_container(name)
    system("docker", "stop", name, out: File::NULL, err: File::NULL)
    system("docker", "rm", "-f", name, out: File::NULL, err: File::NULL)
  end

  def setup_postgres
    teardown_container(POSTGRES_NAME)
    run!(
      "docker", "run", "-d", "--name", POSTGRES_NAME, "--network", NETWORK,
      "-e", "POSTGRES_DB=bench", "-e", "POSTGRES_USER=bench", "-e", "POSTGRES_PASSWORD=bench",
      "postgres:16-alpine"
    )
    wait_until_postgres_ready!
    seed_postgres!
  end

  def wait_until_postgres_ready!
    # 180s, not a shorter guess: Postgres's own real first-boot cycle on
    # this machine (a temporary server for `CREATE DATABASE`, a full
    # shutdown/checkpoint-sync, then the real server starting) was directly
    # measured taking as long as 56s under load -- confirmed by timing it by
    # hand before picking this number, not assumed short.
    attempt = 0
    Timeout.timeout(180) do
      loop do
        attempt += 1
        break if system("docker", "exec", POSTGRES_NAME, "psql", "-U", "bench", "-d", "bench", "-c", "SELECT 1",
                        out: File::NULL, err: File::NULL)

        warn "  waiting for postgres (attempt #{attempt})..." if (attempt % 10).zero?
        sleep 1
      end
    end
  end

  def seed_postgres!
    run!(
      "docker", "exec", POSTGRES_NAME, "psql", "-U", "bench", "-d", "bench", "-c",
      "CREATE TABLE IF NOT EXISTS widgets (id serial primary key, name text); " \
      "TRUNCATE widgets; " \
      "INSERT INTO widgets (name) SELECT 'widget-' || g FROM generate_series(1, 50) g;"
    )
  end

  def build_image
    cmd = ["docker", "build", "-f", "bench/Dockerfile", "-t", IMAGE]
    cmd += ["--build-arg", "RB_SYS_CARGO_FEATURES=#{CARGO_FEATURES}"] unless CARGO_FEATURES.empty?
    cmd << "."
    run!(*cmd, chdir: REPO_ROOT)
  end

  def boot_server(server, scenario)
    teardown_container(SERVER_NAME)
    run!(
      "docker", "run", "-d", "--name", SERVER_NAME, "--network", NETWORK,
      "--cpus", CPUS, "--memory", MEMORY,
      "-p", "19292:9292",
      "-e", "BENCH_SERVER=#{server}", "-e", "BENCH_APP=#{scenario}",
      "-e", "BENCH_PG_HOST=#{POSTGRES_NAME}", "-e", "BENCH_PG_PORT=5432",
      "-e", "BENCH_PG_DBNAME=bench", "-e", "BENCH_PG_USER=bench", "-e", "BENCH_PG_PASSWORD=bench",
      IMAGE
    )
    wait_until_server_ready!
  end

  def wait_until_server_ready!
    Timeout.timeout(60) do
      loop do
        Net::HTTP.start("127.0.0.1", 19_292, read_timeout: 2) { |http| http.head("/") }
        return
      rescue StandardError
        sleep 0.5
      end
    end
  end

  def run_k6(target_path)
    out_path = "/tmp/helixrack-bench-k6-#{Process.pid}.json"
    FileUtils.rm_f(out_path)
    # CodeRabbit finding: a nonzero k6 exit (e.g. the `checks` threshold in
    # bench/k6/scenario.js failing because responses weren't actually all
    # 200) used to go unchecked -- a run full of failed requests could still
    # produce a "successful"-looking p99 number from whatever did complete.
    # `exception: true` raises instead, so a bad run aborts the whole
    # benchmark rather than silently contributing a misleading sample.
    system(
      "docker", "run", "--rm", "--network", NETWORK,
      "-v", "#{REPO_ROOT}/bench/k6:/scripts:ro", "-v", "/tmp:/results",
      "-e", "TARGET_URL=http://#{SERVER_NAME}:9292#{target_path}",
      "-e", "VUS=#{VUS}", "-e", "DURATION=#{DURATION}",
      "grafana/k6:latest", "run",
      "--summary-trend-stats=avg,min,med,max,p(90),p(95),p(99)",
      "--summary-export=/results/#{File.basename(out_path)}",
      "/scripts/scenario.js",
      out: File::NULL, err: File::NULL, exception: true
    )
    data = JSON.parse(File.read(out_path))
    p99_ms = data.dig("metrics", "http_req_duration", "p(99)")
    FileUtils.rm_f(out_path)
    p99_ms
  end

  def sample_memory_peak_bytes
    capture("docker", "exec", SERVER_NAME, "cat", "/sys/fs/cgroup/memory.peak").strip.to_i
  end

  def median(values)
    sorted = values.sort
    mid = sorted.length / 2
    sorted.length.odd? ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2.0
  end

  # CodeRabbit finding, confirmed against this project's own first real run's
  # data: cgroup v2 `memory.peak` is a monotonic watermark since the cgroup
  # was created (confirmed unwritable/unresettable on this host -- `echo 0 >
  # memory.peak` inside a running container returned `Read-only file
  # system`), so measuring it repeatedly against *one* `boot_server` call
  # shared across all `N_RUNS` doesn't give independent per-run peaks -- it
  # gives a nondecreasing sequence converging on the pair's overall peak.
  # The first real run's own `rss_bytes_samples` for helix_rack/hello_world
  # showed exactly that shape (`[32837632, 33271808, 33271808, ...]`,
  # plateauing after run 2) before this fix. A fresh container per *run*,
  # not per (server, scenario) *pair*, is what actually isolates each
  # sample -- more container-boot overhead (N_RUNS restarts instead of one),
  # judged worth it for a real, independent measurement.
  def measure(server, scenario)
    p99s = []
    rss_samples = []
    N_RUNS.times do |i|
      boot_server(server, scenario)
      p99s << run_k6(SCENARIOS.fetch(scenario))
      rss_samples << sample_memory_peak_bytes
      teardown_container(SERVER_NAME)
      rss_mib = (rss_samples.last / 1_048_576.0).round(1)
      warn "  [#{server}/#{scenario}] run #{i + 1}/#{N_RUNS}: p99=#{p99s.last.round(2)}ms rss=#{rss_mib}MiB"
    end

    {
      "server" => server,
      "scenario" => scenario,
      "p99_ms_median" => median(p99s),
      "p99_ms_min" => p99s.min,
      "p99_ms_max" => p99s.max,
      "p99_ms_samples" => p99s,
      "rss_bytes_median" => median(rss_samples),
      "rss_bytes_min" => rss_samples.min,
      "rss_bytes_max" => rss_samples.max,
      "rss_bytes_samples" => rss_samples
    }
  end

  def write_report(results)
    FileUtils.mkdir_p(RESULTS_DIR)
    File.write(File.join(RESULTS_DIR, "results.json"), JSON.pretty_generate(results))

    lines = ["# Phase 13 benchmark results", ""]
    lines << "N=#{N_RUNS} runs, VUS=#{VUS}, duration=#{DURATION}, cpus=#{CPUS}, memory=#{MEMORY}."
    lines << "Cargo features: #{CARGO_FEATURES.empty? ? "(none -- default build)" : CARGO_FEATURES}"
    lines << "Container digest: #{capture("docker", "image", "inspect", "--format={{.Id}}", IMAGE).strip}"
    lines << ""

    SCENARIOS.each_key do |scenario|
      lines << "## #{scenario}"
      lines << ""
      lines << "| server | p99 median (ms) | p99 range (ms) | RSS median (MiB) | RSS range (MiB) |"
      lines << "|---|---|---|---|---|"
      SERVERS.each do |server|
        row = results.find { |r| r["server"] == server && r["scenario"] == scenario }
        next unless row

        lines << "| #{server} | #{row["p99_ms_median"].round(2)} | " \
                 "#{row["p99_ms_min"].round(2)}-#{row["p99_ms_max"].round(2)} | " \
                 "#{(row["rss_bytes_median"] / 1_048_576.0).round(1)} | " \
                 "#{(row["rss_bytes_min"] / 1_048_576.0).round(1)}-#{(row["rss_bytes_max"] / 1_048_576.0).round(1)} |"
      end
      lines << ""
    end

    File.write(File.join(RESULTS_DIR, "report.md"), lines.join("\n"))
    puts lines.join("\n")
  end

  # Ratio failures for one scenario, or `nil` if either HelixRack's or the
  # baseline's data is missing there (a run scoped via `BENCH_SERVERS`/
  # `BENCH_SCENARIOS` -- see those constants -- has nothing to compare).
  def gate_failures_for(scenario, results)
    helix = results.find { |r| r["server"] == "helix_rack" && r["scenario"] == scenario }
    baseline = results.find { |r| r["server"] == GATE_BASELINE_SERVER && r["scenario"] == scenario }
    return nil unless helix && baseline

    p99_ratio = helix["p99_ms_median"] / baseline["p99_ms_median"]
    rss_ratio = helix["rss_bytes_median"] / baseline["rss_bytes_median"].to_f

    failures = []
    failures << "#{scenario}: P99 ratio #{p99_ratio.round(3)} > #{GATE_P99_RATIO_MAX}" if p99_ratio > GATE_P99_RATIO_MAX
    failures << "#{scenario}: RSS ratio #{rss_ratio.round(3)} > #{GATE_RSS_RATIO_MAX}" if rss_ratio > GATE_RSS_RATIO_MAX
    failures
  end

  # A run scoped via `BENCH_SERVERS`/`BENCH_SCENARIOS` to skip either
  # HelixRack or the baseline server for a given scenario is reported as
  # "not evaluated", distinct from "evaluated and passed" -- a partial/
  # debugging run can never silently print a real-looking "GATE PASSED".
  def assert_gate!(results)
    # `ALL_SCENARIOS`, not `SCENARIOS` -- CodeRabbit finding: a run scoped
    # via `BENCH_SCENARIOS` (see that constant) would otherwise only iterate
    # the narrowed set, so a scenario deliberately excluded from this run
    # would never even register as "skipped," letting an *intentionally*
    # partial debugging run silently print a real-looking "GATE PASSED" once
    # every scenario it *did* check happened to pass. The full, unnarrowed
    # scenario list is what actually has to be accounted for -- either
    # evaluated or explicitly reported missing -- for that message to mean
    # anything.
    per_scenario = ALL_SCENARIOS.each_key.to_h { |s| [s, gate_failures_for(s, results)] }
    skipped, evaluated = per_scenario.partition { |_, v| v.nil? }.map(&:to_h)
    failures = evaluated.values.flatten

    unless skipped.empty?
      warn "NOT EVALUATED (missing helix_rack or #{GATE_BASELINE_SERVER} data): #{skipped.keys.join(", ")}"
    end

    if failures.empty? && skipped.empty?
      puts "GATE PASSED: HelixRack beats #{GATE_BASELINE_SERVER} on P99 (<=#{GATE_P99_RATIO_MAX}x) " \
           "and RSS (<=#{GATE_RSS_RATIO_MAX}x) across every scenario."
    elsif failures.empty?
      puts "GATE INCOMPLETE: no threshold failures among evaluated scenarios, but #{skipped.size} " \
           "scenario(s) were not evaluated (see above) -- not a pass."
      exit 1
    else
      warn "GATE FAILED:"
      failures.each { |f| warn "  - #{f}" }
      exit 1
    end
  end

  def main
    build_image
    setup_network
    setup_postgres

    results = []
    SCENARIOS.each_key do |scenario|
      SERVERS.each do |server|
        results << measure(server, scenario)
      end
    end

    write_report(results)
    assert_gate!(results)
  ensure
    teardown_container(SERVER_NAME)
    teardown_container(POSTGRES_NAME)
  end
end

Bench.main if $PROGRAM_NAME == __FILE__
