# frozen_string_literal: true

require "grape"
require "pg"

module Bench
  # Phase 13 (PLAN.md, Phase 13, PRD.md 7.2's "I/O Misto (Grape + Query
  # PostgreSQL)" scenario): validates GVL release during blocking I/O --
  # `pg`'s own C extension calls `rb_thread_call_without_gvl` around its
  # query execution (`ext/gvl_wrappers.h` in the `ruby-pg` source, confirmed
  # before writing this, not assumed), so a request blocked on a real
  # Postgres round trip is exactly the scenario PRD.md 7.2 asks this
  # benchmark to exercise.
  #
  # One connection per *thread*, not one shared global connection: safe
  # under Puma's default multi-threaded model (a shared `PG::Connection` is
  # not thread-safe for concurrent queries) and equally correct under
  # HelixRack's single-OS-thread model (where it simply never contends).
  # Lazily connected on first use rather than at boot, so a slow-starting
  # Postgres container doesn't fail server boot.
  class IoMixedApp < Grape::API
    format :json

    helpers do
      def pg_connection
        Thread.current[:bench_pg_connection] ||= PG.connect(
          host: ENV.fetch("BENCH_PG_HOST", "127.0.0.1"),
          port: ENV.fetch("BENCH_PG_PORT", "5432"),
          dbname: ENV.fetch("BENCH_PG_DBNAME", "bench"),
          user: ENV.fetch("BENCH_PG_USER", "bench"),
          password: ENV.fetch("BENCH_PG_PASSWORD", "bench")
        )
      end
    end

    get :widgets do
      result = pg_connection.exec_params("SELECT id, name FROM widgets ORDER BY id LIMIT 20")
      result.map { |row| { id: row["id"].to_i, name: row["name"] } }
    end
  end
end
