# frozen_string_literal: true

require "grape"

module Fixtures
  # Fixture Grape app for the Phase 11 gate (see `PLAN.md`, Phase 11
  # "Gate"): exercises the four things PRD.md section 7.1 asks for --
  # nested routes, param validation, error middleware, and JSON
  # serialization -- through one small API, so the gate's server helper
  # only has to boot one process.
  #
  # Every status code below is set explicitly (`status 201`, etc.) rather
  # than left to Grape's own per-verb default: this fixture's whole point
  # is a request-spec table with *exact* expected statuses, so it shouldn't
  # depend on an unstated, version-specific Grape default the gate would
  # otherwise be silently assuming rather than asserting.
  class Phase11GrapeApp < Grape::API
    format :json

    # Grape's own error-handling middleware layer (PRD.md 7.1's "middlewares
    # de erro"): both branches below produce a clean [status, headers, body]
    # Rack triple from *inside* Grape's own dispatch -- neither exception
    # ever reaches HelixRack's `Handler::call` (`ext/helix_rack/src/lib.rs`)
    # as a raised error, so neither row this drives logs a "request failed"
    # fault the way Phase 7's fixture app deliberately does. That matters
    # for this gate: it asserts *zero* fault-log growth across the whole
    # scenario table, precisely to prove `Rack::Lint` (wrapped around this
    # app in `phase11_grape.ru`) never caught a spec violation anywhere --
    # an app-level error that Grape itself contains and formats correctly is
    # a *pass* for that assertion, not a fault.
    rescue_from Grape::Exceptions::ValidationErrors do |e|
      error!({ error: e.message }, 400)
    end

    rescue_from :all do |e|
      error!({ error: e.message }, 500)
    end

    resource :widgets do
      params do
        requires :name, type: String
      end
      post do
        status 201
        { id: 1, name: params[:name] }
      end

      route_param :id do
        get do
          status 200
          { id: params[:id].to_i, name: "widget-#{params[:id]}" }
        end

        # The nested route PRD.md 7.1 asks for ("rotas aninhadas"): a
        # `route_param` segment (`/widgets/:id`) with its own child
        # `resource` (`/widgets/:id/parts`), two levels deep.
        resource :parts do
          get do
            status 200
            [{ id: 1, widget_id: params[:id].to_i, label: "bracket" }]
          end
        end
      end
    end

    get :boom do
      raise "deliberate error for the Phase 11 error-middleware scenario"
    end
  end
end
