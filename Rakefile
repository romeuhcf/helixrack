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
end

task default: %i[compile spec rubocop]
