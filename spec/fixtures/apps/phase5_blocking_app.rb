# frozen_string_literal: true

module Fixtures
  # Fixture Rack app for the Phase 5 gate (see `PLAN.md`, Phase 5 "What this
  # phase's gate should actually prove instead"): on `#call`, first signals
  # `started_queue` (so the test knows `Handler::call` has actually reached
  # this app's code, not just that the client's request was sent) and then
  # blocks by reading one byte from `read_pipe` -- the read end of an
  # `IO.pipe` the test controls and has not written to yet.
  #
  # `IO#read` on an empty pipe was chosen as the barrier because a
  # standalone, throwaway script (run outside this repo before this file was
  # written, see the branch's report) confirmed empirically, on this
  # project's Ruby version, that it genuinely yields the GVL to another
  # same-process Ruby `Thread` while blocked -- not assumed from memory.
  class Phase5BlockingApp
    def initialize(read_pipe, started_queue)
      @read_pipe = read_pipe
      @started_queue = started_queue
    end

    def call(_env)
      @started_queue << :blocked
      @read_pipe.read(1)
      body = "released"
      # `content-length` is required here, not cosmetic: without it (and with
      # no `Transfer-Encoding: chunked`), an HTTP/1.1 client can't tell where
      # this response's body ends short of the connection closing -- and this
      # server's connections are persistent by default (`PLAN.md`'s Phase 4
      # "Architecture note"), so a client reading a Content-Length-less
      # response here would otherwise block until this connection's
      # `keep_alive_timeout` fires, unrelated to anything Phase 5 is testing.
      [200, { "content-type" => "text/plain", "content-length" => body.bytesize.to_s }, [body]]
    end
  end
end
