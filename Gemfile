# frozen_string_literal: true

source "https://rubygems.org"

# Specify your gem's dependencies in helix_rack.gemspec
gemspec

gem "irb"
gem "rake", "~> 13.0"

gem "rake-compiler"

# Phase 12 (PLAN.md, Phase 12): drives the `gem:native` Rake task's real
# rake-compiler-dock container build. Already an indirect dependency (via
# `rb_sys`/`rake-compiler`); declared explicitly here since the Rakefile now
# `require`s it directly.
gem "rake-compiler-dock", "~> 1.9"

gem "rspec", "~> 3.0"

# Phase 11 (PLAN.md, Phase 11): drives the fixture Grape app the Rack::Lint/
# request-spec gate serves through HelixRack. Test-only -- HelixRack itself
# has no runtime dependency on Grape, only on Rack (already a gemspec
# dependency), since any Rack-compliant app (Grape included) works unmodified.
gem "grape", "~> 4.0"

gem "rubocop", "~> 1.21"
