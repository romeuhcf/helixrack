# frozen_string_literal: true

require "digest"
require "net/http"
require "timeout"
require_relative "../support/phase3_server_helper"
require_relative "../fixtures/large_body_source"

# Phase 3 gate (see PLAN.md, Phase 3 "Gate"): serves a large (~200 MB)
# fixture payload through a really-booted `helix_rack` subprocess and
# asserts two things independently:
#
# * Correctness: the client's SHA-256 of the received bytes matches the
#   source's precomputed SHA-256, exactly.
# * Memory bound: the server *process's* peak RSS during the transfer (read
#   from `/proc/<pid>/status`, polled throughout) stays within
#   `baseline_rss + payload_size / 10` of the RSS measured right after boot,
#   before any request -- a delta, not an absolute ceiling. An absolute
#   ceiling was tried first and found not to hold up: `bundle exec`
#   activating this gem's full dev Gemfile (rspec, rubocop, rake-compiler,
#   irb) alone costs ~28 MB of baseline RSS before any request, already over
#   a naive `payload_size / 10` (~20 MB for this fixture) ceiling -- noise
#   entirely unrelated to whether response bodies are actually streamed.
#   The delta isolates what Phase 3 actually claims: serving a large body
#   doesn't scale memory *on top of* whatever the process already costs to
#   exist, regardless of what that fixed cost happens to be in any given
#   environment.
#
# `PLAN.md`'s Phase 3 "Architecture decision" spools response bodies to a
# tempfile past a size threshold and streams that back out in bounded
# chunks, so the server never needs to hold a whole large body in RAM at
# once (`engine/src/handler.rs`'s `ResponseBody` enum, `ext/helix_rack/src/
# lib.rs`'s `RackAppHandler`/`read_body`/`spool_chunk`).
#
# rubocop:disable Metrics/BlockLength -- one real-server integration example
# plus its own well-documented helper methods reads better kept together in
# one gate file than split across files for a line-count target.
RSpec.describe "Phase 3: streaming response body gate" do
  include Phase3::ServerHelper

  # rubocop:disable Lint/ConstantDefinitionInBlock -- deliberately real
  # constants (matching `spec/integration/phase2_rack_env_spec.rb`'s
  # `TABLE`), not `let`s: fixed configuration for the whole gate, not
  # per-example state.
  APP_PATH = File.expand_path("../fixtures/apps/large_body.ru", __dir__)

  # How far peak RSS may rise *above the baseline measured right after
  # boot* -- `payload_size / 10`, in the same unit `/proc/<pid>/status`'s
  # `VmRSS` line reports (kB), per PLAN.md's Phase 3 gate's own suggested
  # ratio. Not an absolute ceiling -- see the file-level doc comment above
  # for why.
  RSS_DELTA_BUDGET_KB = Fixtures::LargeBodySource::TOTAL_BYTES / 10 / 1024

  # How often the background RSS-polling thread samples `/proc/<pid>/status`
  # while the transfer is in flight. Small enough not to miss a peak that
  # only lasts a fraction of a second (e.g. the moment the whole body is
  # collected server-side, before any bytes reach the client), large enough
  # not to make the polling thread itself a meaningful CPU cost next to
  # actually moving ~200 MB.
  RSS_POLL_INTERVAL_SECONDS = 0.01

  # How long to wait for the RSS-polling thread's first successful sample
  # before giving up and falling through to with_rss_tracking's own
  # zero-samples check. Bounds the wait when /proc is genuinely
  # unavailable (a clean failure instead of hanging), while being far more
  # than the monitor thread should ever actually need to get scheduled and
  # take one reading.
  RSS_READY_TIMEOUT_SECONDS = 2
  # rubocop:enable Lint/ConstantDefinitionInBlock

  it "streams a large body correctly, within a bounded server RSS" do
    with_helix_rack_subprocess(APP_PATH) do |pid, port|
      baseline_rss_kb = read_vmrss_kb(pid)
      raise "could not read baseline VmRSS for pid #{pid} right after boot" unless baseline_rss_kb

      ceiling_kb = baseline_rss_kb + RSS_DELTA_BUDGET_KB

      (received_sha256, received_bytes), peak_rss_kb = with_rss_tracking(pid) { download_and_hash(port) }

      expect(received_bytes).to eq(Fixtures::LargeBodySource::TOTAL_BYTES),
                                "expected #{Fixtures::LargeBodySource::TOTAL_BYTES} bytes, got #{received_bytes}"
      expect(received_sha256).to eq(Fixtures::LargeBodySource.sha256_hexdigest),
                                 "received body's SHA-256 did not match the source's"
      expect(peak_rss_kb).to be <= ceiling_kb,
                             "server RSS peaked at #{peak_rss_kb} kB, over the #{ceiling_kb} kB ceiling " \
                             "(#{baseline_rss_kb} kB baseline + #{RSS_DELTA_BUDGET_KB} kB payload_size / 10 " \
                             "budget) -- the response body is still fully buffered in memory rather than " \
                             "spooled/streamed (PLAN.md Phase 3)"
    end
  end

  # Requests `path` from the server on `port` and streams the response body
  # through a SHA-256 digest chunk by chunk (`Net::HTTP#read_body` with a
  # block yields chunks without buffering the whole body itself) -- the
  # *client's* side of not needing ~200 MB resident at once either. Returns
  # `[hex_digest, total_bytes_received]`.
  # rubocop:disable Metrics/MethodLength -- the status check and the
  # digest/counter updates all belong inside the same `read_body` block;
  # splitting them into further private methods would scatter one linear
  # step without shortening it.
  def download_and_hash(port, path: "/")
    digest = Digest::SHA256.new
    total_bytes = 0

    Net::HTTP.start("127.0.0.1", port) do |http|
      request = Net::HTTP::Get.new(path)
      http.request(request) do |response|
        raise "unexpected response status #{response.code}" unless response.code == "200"

        response.read_body do |chunk|
          digest.update(chunk)
          total_bytes += chunk.bytesize
        end
      end
    end

    [digest.hexdigest, total_bytes]
  end
  # rubocop:enable Metrics/MethodLength

  # Runs a background thread that repeatedly reads `pid`'s `VmRSS` from
  # `/proc/<pid>/status` while `block` runs, then returns
  # `[block's return value, peak VmRSS observed, in kB]`.
  #
  # The polling thread starts before `block` runs and is joined (guaranteed
  # stopped) before this method returns, so the whole duration `block` takes
  # -- including any work that happens before the client ever receives a
  # byte, such as the current implementation's full server-side body
  # collection -- is covered, not just the time spent reading the HTTP
  # response.
  # rubocop:disable Metrics/MethodLength, Metrics/AbcSize -- the polling
  # thread's setup and its `ensure`-guaranteed teardown belong together in
  # one method so the "always stopped before returning" guarantee is
  # visible in one place.
  def with_rss_tracking(pid)
    peak_kb = 0
    samples = 0
    stop = false
    ready = Queue.new

    # All mutation of samples/peak_kb happens on this one thread, never
    # from the caller -- `ready.pop` below only *reads* whether the first
    # sample landed, so there is nothing to synchronize on those two
    # variables beyond the happens-before edge `monitor.join` already gives
    # the caller once this loop exits.
    monitor = Thread.new do
      Thread.current.report_on_exception = false
      # Sample first, check `stop` after: `Thread.new` returning doesn't
      # guarantee this thread has actually started running yet, so without
      # a readiness signal the caller's transfer could start (and finish
      # part of its work) before the very first sample lands -- an
      # unobserved early peak that the samples.zero? check below wouldn't
      # catch, since it only proves *some* sample happened, not that
      # sampling covered the whole transfer. Sampling before checking
      # `stop` (rather than the reverse) also guarantees one last sample
      # after the caller's transfer finishes, not just up to the previous
      # poll tick.
      loop do
        rss = read_vmrss_kb(pid)
        if rss
          samples += 1
          peak_kb = rss if rss > peak_kb
          ready.push(true) if samples == 1
        end
        break if stop

        sleep RSS_POLL_INTERVAL_SECONDS
      end
    end

    wait_for_first_sample(ready)

    begin
      result = yield
    ensure
      stop = true
      monitor.join
    end

    # Without this, an unreadable /proc (a different OS, a sandboxed CI
    # runner, a permissions issue) makes read_vmrss_kb return nil every
    # time, peak_kb stays 0, and the RSS assertion passes having measured
    # nothing -- once un-pended, the gate would pass without enforcing its
    # actual bound.
    raise "never read VmRSS for pid #{pid} -- /proc is unavailable, so the RSS bound was not measured" if samples.zero?

    [result, peak_kb]
  end
  # rubocop:enable Metrics/MethodLength, Metrics/AbcSize

  # Blocks until `ready` receives the monitor thread's first successful
  # sample, or `RSS_READY_TIMEOUT_SECONDS` passes -- bounding the wait when
  # /proc is genuinely unavailable (falls through to with_rss_tracking's
  # own samples.zero? check for a clear error) instead of hanging forever.
  def wait_for_first_sample(ready)
    Timeout.timeout(RSS_READY_TIMEOUT_SECONDS) { ready.pop }
  rescue Timeout::Error
    nil
  end

  # `pid`'s current resident set size, in kB, or `nil` if the process is
  # already gone (avoids a race against `with_helix_rack_subprocess`'s own
  # cleanup turning into a spurious failure here).
  def read_vmrss_kb(pid)
    status = File.read("/proc/#{pid}/status")
    match = status.match(/^VmRSS:\s+(\d+)\s+kB/)
    match && match[1].to_i
  rescue Errno::ENOENT
    nil
  end
end
# rubocop:enable Metrics/BlockLength
