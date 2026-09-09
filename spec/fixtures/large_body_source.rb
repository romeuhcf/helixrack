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

    # The bytes for chunk `index` (0-based), always exactly `CHUNK_SIZE`
    # bytes long. Built by repeating a 32-byte SHA-256 digest of the index
    # until it fills the chunk -- deterministic, and well-distributed rather
    # than e.g. a single repeated byte (which could accidentally pass a
    # sloppy correctness check that isn't really comparing content).
    def chunk(index)
      seed = Digest::SHA256.digest("helixrack-phase3-fixture-chunk-#{index}")
      repeated = seed * ((CHUNK_SIZE / seed.bytesize) + 1)
      repeated[0, CHUNK_SIZE].force_encoding(Encoding::BINARY)
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
