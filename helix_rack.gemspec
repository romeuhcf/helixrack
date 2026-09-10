# frozen_string_literal: true

require_relative "lib/helix_rack/version"

Gem::Specification.new do |spec|
  spec.name = "helix_rack"
  spec.version = HelixRack::VERSION
  spec.authors = ["Romeu Fonseca"]
  spec.email = ["romeu.hcf@gmail.com"]

  spec.summary = "Native HTTP/1.1 server in Rust for Ruby Rack/Grape apps"
  spec.description = "Minimal, single-threaded Rust HTTP/1.1 server embedding the CRuby VM " \
                      "via rb-sys/magnus, built for high throughput under tight CPU limits."
  spec.homepage = "https://github.com/romeuhcf/helixrack"
  spec.license = "MIT"
  # Phase 12 (PLAN.md, Phase 12): was ">= 3.2.0" until a real multi-Ruby-
  # version cross-compile attempt (`rake gem:native`, backing this phase's
  # packaging gate) proved that wrong -- Ruby 3.2.11's own C headers don't
  # declare `rb_postponed_job_preregister`/`rb_postponed_job_trigger`
  # (`ext/helix_rack/src/lib.rs`'s Phase 6 preemption mechanism uses both
  # unconditionally, no version-gated fallback), and the build genuinely
  # failed with `cannot find function` for exactly those symbols. Verified
  # (web search, not assumed) that both were added in Ruby 3.3.0 -- Ruby 3.2
  # itself reached its own end of life on 2026-03-31, already past by the
  # time this was caught, so narrowing here costs nothing currently
  # supported. See PLAN.md's Phase 12 Resolution note.
  spec.required_ruby_version = ">= 3.3.0"
  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/romeuhcf/helixrack"
  spec.metadata["rubygems_mfa_required"] = "true"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  gemspec = File.basename(__FILE__)
  spec.files = IO.popen(%w[git ls-files -z], chdir: __dir__, err: IO::NULL) do |ls|
    ls.readlines("\x0", chomp: true).reject do |f|
      (f == gemspec) ||
        f.start_with?(*%w[bin/ Gemfile .gitignore .rspec spec/ .github/ .rubocop.yml])
    end
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]
  spec.extensions = ["ext/helix_rack/extconf.rb"]

  # Uncomment to register a new dependency of your gem
  # spec.add_dependency "example-gem", "~> 1.0"
  spec.add_dependency "rb_sys", "~> 0.9.128"

  # exe/helix_rack loads config.ru via Rack::Builder.parse_file -- a real
  # runtime dependency, not just a test one (Rack::Lint and the Phase 2
  # gate's fixture apps also use it, but that alone would only need a
  # development dependency). Constrained to what's actually been verified:
  # Rack::Builder.parse_file's return shape changed between major versions
  # (Rack 1.x/2.x returned [app, options]; Rack 3 returns the app directly,
  # confirmed by reading the installed rack-3.2.7's source) -- only the
  # Rack 3 shape has been checked against this code.
  spec.add_dependency "rack", ">= 3.0"

  # For more information and examples about making a new gem, check out our
  # guide at: https://guides.rubygems.org/make-your-own-gem/
end
