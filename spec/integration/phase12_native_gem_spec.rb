# frozen_string_literal: true

require "open3"

# Phase 12 gate (see `PLAN.md`, Phase 12 "Gate"): a real precompiled native
# gem (`pkg/helix_rack-<version>-x86_64-linux.gem`, built by `rake
# gem:native` -- the same underlying rake-compiler-dock mechanism
# `.github/workflows/build-gems.yml`'s `oxidize-rb/actions/cross-gem@v1`
# step already uses in CI) installs and runs correctly inside a container
# with *no* Rust/C toolchain present -- proving it's truly precompiled, not
# silently falling back to source compilation (which would need `extconf.rb`
# and a compiler `gem install` would have no way to run).
#
# Deliberately skipped, not run, unless both a built gem and a working
# Docker are actually present: building the gem takes real minutes (a fresh
# rake-compiler-dock container pull plus a genuine cross-compile) and needs
# Docker and network access neither `bundle exec rspec` nor `main.yml`'s
# routine per-PR job provide or should pay for on every commit -- matching
# this project's own established pattern of scoping a heavy, real
# verification to where it's actually exercised (Phase 10's `build-gems.yml`
# workflow_dispatch, not `main.yml`) rather than forcing it into the fast
# loop. `.github/workflows/build-gems.yml`'s `verify-native-install` job
# (added this phase) is what actually runs this spec for real, after
# building a fresh gem.
# rubocop:disable Metrics/BlockLength -- one real clean-container integration
# gate plus its own setup reads better kept together in one file, matching
# spec/integration/phase7_fault_containment_spec.rb's precedent, than split
# up for a line-count target.
RSpec.describe "Phase 12: native gem installs and runs with no build toolchain (packaging gate)" do
  # rubocop:disable Lint/ConstantDefinitionInBlock -- deliberately real
  # constants (matching spec/integration/phase7_fault_containment_spec.rb's
  # FAULT_APP_PATH), not `let`s: fixed configuration for the whole gate.
  GEM_PATH = File.expand_path("../../pkg/helix_rack-#{HelixRack::VERSION}-x86_64-linux.gem", __dir__)
  CONTAINER_RUBY_IMAGE = "ruby:3.4-slim"
  # rubocop:enable Lint/ConstantDefinitionInBlock

  before do
    skip "docker not found on PATH -- required for this gate's clean-container check" if `which docker`.empty?
    unless File.exist?(GEM_PATH)
      skip "no prebuilt native gem at #{GEM_PATH} -- run `bundle exec rake gem:native` first " \
           "(see .github/workflows/build-gems.yml's verify-native-install job for where this runs for real)"
    end
  end

  it "gem installs and helix_rack --version/--help run correctly with zero build tools present" do
    pkg_dir = File.dirname(GEM_PATH)
    gem_filename = File.basename(GEM_PATH)

    # Two CodeRabbit findings fixed here, both real:
    # - `which gcc cc cargo rustc make` (a single command) exits nonzero the
    #   moment *any one* tool is missing, so the original `... && echo no ||
    #   echo yes` reported "yes" (absent) even with e.g. `make` present and
    #   only `cargo` missing -- checked each tool independently instead,
    #   `TOOLCHAIN_ABSENT=yes` only when *none* of them resolve.
    # - `echo "X=$(cmd)"; echo "EXIT=$?"` captures `echo`'s own exit status
    #   (always 0 once the substitution has *some* output), not `cmd`'s --
    #   captured each command's real status via an `if`/`else` instead, so a
    #   `helix_rack` that printed the right text but exited nonzero would no
    #   longer pass.
    script = <<~SCRIPT
      set -e
      if command -v gcc >/dev/null 2>&1 || command -v cc >/dev/null 2>&1 ||
         command -v cargo >/dev/null 2>&1 || command -v rustc >/dev/null 2>&1 ||
         command -v make >/dev/null 2>&1; then
        echo "TOOLCHAIN_ABSENT=no"
      else
        echo "TOOLCHAIN_ABSENT=yes"
      fi
      gem install "/pkg/#{gem_filename}" --no-document >/dev/null
      echo "INSTALL_EXIT=$?"
      if version_output="$(helix_rack --version)"; then version_exit=0; else version_exit=$?; fi
      if help_output="$(helix_rack --help)"; then help_exit=0; else help_exit=$?; fi
      echo "VERSION_OUTPUT=$version_output"
      echo "VERSION_EXIT=$version_exit"
      echo "HELP_FIRST_LINE=$(printf '%s\\n' "$help_output" | head -n 1)"
      echo "HELP_EXIT=$help_exit"
    SCRIPT

    stdout, status = Open3.capture2(
      "docker", "run", "--rm", "-v", "#{pkg_dir}:/pkg:ro", CONTAINER_RUBY_IMAGE, "bash", "-c", script
    )

    expect(status.success?).to be(true), "docker run itself failed (exit #{status.exitstatus}):\n#{stdout}"

    fields = stdout.lines.each_with_object({}) do |line, acc|
      key, value = line.chomp.split("=", 2)
      acc[key] = value if key
    end

    expect(fields["TOOLCHAIN_ABSENT"]).to eq("yes"), "expected no gcc/cc/cargo/rustc/make in #{CONTAINER_RUBY_IMAGE}"
    expect(fields["INSTALL_EXIT"]).to eq("0")
    expect(fields["VERSION_OUTPUT"]).to eq("helix_rack #{HelixRack::VERSION}")
    expect(fields["VERSION_EXIT"]).to eq("0")
    expect(fields["HELP_FIRST_LINE"]).to eq("Usage: helix_rack [options]")
    expect(fields["HELP_EXIT"]).to eq("0")
  end
end
# rubocop:enable Metrics/BlockLength
