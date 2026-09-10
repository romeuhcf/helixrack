# frozen_string_literal: true

require "bundler/gem_tasks"
require "rspec/core/rake_task"

RSpec::Core::RakeTask.new(:spec)

require "rubocop/rake_task"

RuboCop::RakeTask.new

require "rb_sys/extensiontask"

task build: :compile

GEMSPEC = Gem::Specification.load("helix_rack.gemspec")

RbSys::ExtensionTask.new("helix_rack", GEMSPEC) do |ext|
  ext.lib_dir = "lib/helix_rack"
  # Phase 12 (PLAN.md, Phase 12): only *defines* the `cross`/`native:x86_64-linux`
  # tasks `RakeCompilerDock.sh` (Rakefile's `gem:native` task, below) drives
  # inside a real rake-compiler-dock container -- this line alone changes
  # nothing about the plain `rake compile`/`bundle exec rake` loop every
  # prior phase's tests depend on. Confirmed by reading rake-compiler's own
  # `extensiontask.rb`: the prerequisite-rewiring that could affect `compile`
  # only happens inside the `task 'cross' do ... end` body, which only runs
  # when the `cross` task itself is explicitly invoked, not merely defined.
  ext.cross_platform = ["x86_64-linux"]
end

require "rake_compiler_dock"

# Phase 12 (PLAN.md, Phase 12 "Deliverable"): builds a real, precompiled,
# platform-tagged native gem for `x86_64-linux` inside a real
# rake-compiler-dock container (the same underlying mechanism
# `.github/workflows/build-gems.yml`'s `oxidize-rb/actions/cross-gem@v1`
# step already uses in CI, and the same one Phase 10's `workflow_dispatch`
# run of that workflow already exercised for every real deployment
# platform -- see PLAN.md's Phase 10 Resolution note). One platform only,
# matching this project's own CI host (`main.yml` is `ubuntu-latest`,
# x86-64): this task's job is producing a real artifact for Phase 12's own
# clean-container gate to install, not re-proving every platform builds,
# which Phase 10's own run already did.
desc "Build a precompiled native gem for x86_64-linux via rake-compiler-dock"
task "gem:native" do
  # `RakeCompilerDock` never auto-detects `rb_sys` -- it's a generic tool
  # whose *default* image (`ghcr.io/rake-compiler/rake-compiler-dock-image`)
  # has no Rust toolchain at all, confirmed directly (`which cargo` found
  # nothing anywhere in it). `rb_sys`'s own toolchain image family
  # (`rbsys/<platform>`, the same one `.github/workflows/build-gems.yml`'s
  # `oxidize-rb/actions/cross-gem@v1` step already pulls in CI) has to be
  # requested explicitly via `RCD_IMAGE`, tagged to match the exact `rb_sys`
  # version this project builds against -- not hardcoded, so it can't
  # silently drift from the gemspec's own `rb_sys` dependency.
  rb_sys_version = Gem.loaded_specs["rb_sys"].version.to_s
  ENV["RCD_IMAGE"] ||= "rbsys/x86_64-linux:#{rb_sys_version}"

  # `RUBY_CC_VERSION` set *inside* the container's own shell command, not as
  # a host env var passed to `RakeCompilerDock.sh` -- `RakeCompilerDock.sh`
  # only forwards a fixed allowlist of env vars into the `docker run` it
  # shells out to (confirmed by reading the actual command it printed on a
  # first attempt: no `RUBY_CC_VERSION` among them), so setting it on the
  # host process had no effect on which cross-Ruby versions rake-compiler
  # built for. Scoped to `>= 3.3` to match `helix_rack.gemspec`'s
  # `required_ruby_version` (see that file's own comment, and PLAN.md's
  # Phase 12 Resolution note, for why): this image bundles 3.0/3.1/3.2 too,
  # and building unconditionally for all of them is exactly what surfaced
  # the gemspec's `required_ruby_version` being wrong in the first place.
  ruby_versions = "3.3.11:3.4.9:4.0.2"
  RakeCompilerDock.sh("bundle && RUBY_CC_VERSION=#{ruby_versions} rake cross native gem", platform: "x86_64-linux-gnu")
end

task default: %i[compile spec rubocop]
