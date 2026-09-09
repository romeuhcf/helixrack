# frozen_string_literal: true

require "digest"

module Fixtures
  # Deterministic large-body content generator shared between the Phase 3
  # gate's fixture Rack app (`spec/fixtures/apps/large_body_app.rb`, which
  # streams these bytes out via `#each`) and the gate spec itself
  # (`spec/integration/phase3_streaming_body_spec.rb`, which needs the same
  # bytes' SHA-256 independently -- without holding the whole ~200 MB
  # payload in the *test's* memory at once either, see PLAN.md's Phase 3
  # "Gate": "stream the source hash computation too, don't defeat the point
  # of this gate by buffering 200MB in the test's memory").
  #
  # Each chunk is generated from its index alone, with no shared mutable
  # state and no need to keep prior chunks around to produce the next one --
  # so both sides can regenerate the exact same bytes, streaming, one chunk
  # at a time, independently of each other.
  module LargeBodySource
    # 1 MiB per chunk, 200 chunks -> ~200 MB total, matching PLAN.md's Phase
    # 3 gate wording ("e.g. 200 MB of known content").
    CHUNK_SIZE = 1024 * 1024
    CHUNK_COUNT = 200
    TOTAL_BYTES = CHUNK_SIZE * CHUNK_COUNT

    module_function

    # A single ~1 MiB pseudo-random buffer, generated once at load time and
    # reused (mutated, not reallocated) by every `chunk` call below -- see
    # `chunk`'s comment for why. Safe as shared mutable state: every caller
    # in this codebase (the fixture app's `#each`, `sha256_hexdigest`) is
    # single-threaded and consumes a yielded chunk immediately (copies or
    # hashes its bytes) without retaining a reference past that call.
    @buffer = Random.new(0xDEADBEEF).bytes(CHUNK_SIZE)

    # The bytes for chunk `index` (0-based), always exactly `CHUNK_SIZE`
    # bytes long: the shared `@buffer` above, with its first 32 bytes
    # overwritten by a SHA-256 digest of `index` -- deterministic and
    # different per chunk (catching e.g. chunks delivered out of order,
    # dropped, or duplicated -- all change the overall SHA-256 this
    # fixture's callers compare), without the original implementation's
    # per-call cost: repeating a digest to fill a fresh ~1 MiB string on
    # every single call allocated roughly 2 MiB of garbage per chunk (a
    # `String#*` repeat plus a slice), 200 times, and that churn alone was
    # large enough to swamp the Phase 3 gate's RSS measurement -- confirmed
    # by reproducing chunk generation standalone, no server involved, and
    # watching RSS climb by tens of MB. Only mutating a 32-byte region of an
    # already-allocated buffer, 200 times, costs none of that.
    def chunk(index)
      fingerprint = Digest::SHA256.digest("helixrack-phase3-fixture-chunk-#{index}")
      @buffer[0, fingerprint.bytesize] = fingerprint
      @buffer
    end

    # Yields every chunk in order. Neither caller (the fixture app's `#each`
    # below, or `sha256_hexdigest`) ever needs more than one chunk alive at
    # a time.
    def each_chunk
      CHUNK_COUNT.times { |index| yield chunk(index) }
    end

    # SHA-256 of the full ~200 MB payload, computed by streaming each chunk
    # through `Digest::SHA256#update` rather than concatenating them into
    # one big string first -- the whole point of this gate is not needing
    # ~200 MB resident at once, on either side of the connection.
    def sha256_hexdigest
      digest = Digest::SHA256.new
      each_chunk { |bytes| digest.update(bytes) }
      digest.hexdigest
    end
  end
end
