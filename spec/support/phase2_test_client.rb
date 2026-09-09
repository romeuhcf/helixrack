# frozen_string_literal: true

require "net/http"

module Phase2
  # A minimal real-TCP HTTP/1.1 client for the Phase 2 gate (see PLAN.md,
  # Phase 2 "Gate"): every request it sends is a genuine socket connection
  # to a really-booted `HelixRack.serve` instance, not a mocked call into
  # the Rack app.
  class TestClient
    Response = Struct.new(:status, :headers, :body)

    attr_reader :port

    def initialize(port:)
      @port = port
    end

    def request(method:, path:, query: "", headers: {}, body: nil)
      http_request = build_request(method: method, path: path, query: query, headers: headers, body: body)
      http_response = Net::HTTP.start("127.0.0.1", port) { |http| http.request(http_request) }
      Response.new(http_response.code.to_i, http_response.each_header.to_h, http_response.body)
    end

    private

    def build_request(method:, path:, query:, headers:, body:)
      target = query.to_s.empty? ? path : "#{path}?#{query}"
      http_request = Net::HTTP.const_get(method.to_s.capitalize).new(target)
      headers.each { |name, value| http_request[name] = value }
      http_request.body = body if body
      http_request
    end
  end
end
