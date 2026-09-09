# frozen_string_literal: true

require "digest"
require "net/http"
require_relative "../support/phase3_server_helper"
require_relative "../fixtures/large_body_source"

# Phase 3 gate (see PLAN.md, Phase 3 "Gate"): serves a large (~200 MB)
# fixture payload through a really-booted `helix_rack` subprocess and
# asserts two things independently:
#
# * Correctness: the client's SHA-256 of the received bytes matches the
#   source's precomputed SHA-256, exactly.
# * Memory bound: the server *process's* peak RSS (read from
#   `/proc/<pid>/status`, polled throughout the transfer) stays under a
#   fixed ceiling (`payload_size / 10`, per PLAN.md's suggestion).
#
# `PLAN.md`'s Phase 3 "Architecture decision" chose spooling response bodies
# to a tempfile and streaming that back out in bounded chunks, specifically
# so the server never needs to hold a whole large body in RAM at once. That
# isn't implemented yet on this branch -- `engine`'s `HandlerResponse::body`
# gained the `ResponseBody` enum this phase needs to exist and compile (see
# `engine/src/handler.rs`), but `ext/helix_rack/src/lib.rs`'s
# `RackAppHandler`/`read_body` still fully concatenates the whole Rack body
# into one `Vec<u8>` and wraps it in `ResponseBody::InMemory` -- so this gate
# is expected to fail on the memory-bound assertion, not the correctness
# one, until the real tempfile-spooling implementation lands.
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

  # `payload_size / 10`, in the same unit `/proc/<pid>/status`'s `VmRSS`
  # line reports (kB) -- PLAN.md's Phase 3 gate's own suggested ceiling.
  RSS_CEILING_KB = Fixtures::LargeBodySource::TOTAL_BYTES / 10 / 1024

  # How often the background RSS-polling thread samples `/proc/<pid>/status`
  # while the transfer is in flight. Small enough not to miss a peak that
  # only lasts a fraction of a second (e.g. the moment the whole body is
  # collected server-side, before any bytes reach the client), large enough
  # not to make the polling thread itself a meaningful CPU cost next to
  # actually moving ~200 MB.
  RSS_POLL_INTERVAL_SECONDS = 0.01

  # `rake`'s default task (rubocop + rspec) is CI's required status check
  # (this repo's CLAUDE.md, branch protection) -- a gate that's supposed to
  # be red right now must not fail the build, the same reasoning and
  # `pending:` mechanism as spec/integration/phase2_rack_env_spec.rb's
  # PENDING_REASON. RSpec fails the suite instead the moment this starts
  # passing without the marker being removed.
  PENDING_REASON = "Phase 3 not implemented yet -- see PLAN.md (response bodies aren't spooled)"
  # rubocop:enable Lint/ConstantDefinitionInBlock

  it "streams a large body correctly, within a bounded server RSS", pending: PENDING_REASON do
    with_helix_rack_subprocess(APP_PATH) do |pid, port|
      (received_sha256, received_bytes), peak_rss_kb = with_rss_tracking(pid) { download_and_hash(port) }

      expect(received_bytes).to eq(Fixtures::LargeBodySource::TOTAL_BYTES),
                                "expected #{Fixtures::LargeBodySource::TOTAL_BYTES} bytes, got #{received_bytes}"
      expect(received_sha256).to eq(Fixtures::LargeBodySource.sha256_hexdigest),
                                 "received body's SHA-256 did not match the source's"
      expect(peak_rss_kb).to be <= RSS_CEILING_KB,
                             "server RSS peaked at #{peak_rss_kb} kB, over the #{RSS_CEILING_KB} kB " \
                             "ceiling (payload_size / 10) -- the response body is still fully " \
                             "buffered in memory rather than spooled/streamed (PLAN.md Phase 3)"
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
  # rubocop:disable Metrics/MethodLength -- the polling thread's setup and
  # its `ensure`-guaranteed teardown belong together in one method so the
  # "always stopped before returning" guarantee is visible in one place.
  def with_rss_tracking(pid)
    peak_kb = 0
    stop = false

    monitor = Thread.new do
      Thread.current.report_on_exception = false
      until stop
        rss = read_vmrss_kb(pid)
        peak_kb = rss if rss && rss > peak_kb
        sleep RSS_POLL_INTERVAL_SECONDS
      end
    end

    begin
      result = yield
    ensure
      stop = true
      monitor.join
    end

    [result, peak_kb]
  end
  # rubocop:enable Metrics/MethodLength

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
