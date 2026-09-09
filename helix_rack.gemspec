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
  spec.required_ruby_version = ">= 3.2.0"
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

  # For more information and examples about making a new gem, check out our
  # guide at: https://guides.rubygems.org/make-your-own-gem/
end
