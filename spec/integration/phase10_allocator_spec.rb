# frozen_string_literal: true

# Phase 10 gate (see `PLAN.md`, Phase 10 "Gate"): static/binary inspection
# on the compiled extension confirms mimalloc's own symbols are present
# *and* that Rust's global-allocator ABI hooks actually route to them, not
# to glibc `malloc`. Deterministic (binutils on the compiled artifact), not
# a runtime measurement, per PLAN.md's own framing for this phase.
#
# The second half (routing, not just presence) matters: mimalloc's C
# implementation being *linked in* is not, by itself, proof that
# `#[global_allocator]` (`ext/helix_rack/src/lib.rs`) actually took effect
# -- a build could plausibly link mimalloc's code (e.g. as an unused
# transitive dependency) while every real allocation still goes through the
# platform default. What's actually checked below: `__rust_alloc` (the
# well-known Rust ABI symbol every `Vec`/`Box`/`String` allocation compiles
# down to calling) is backed, via its GOT relocation, by an address that
# resolves to one of mimalloc's own defined symbols -- confirmed by hand
# first, with `nm`/`objdump -d`/`objdump -R`, against the real compiled
# `.so`, before being encoded here. Also verified this gate genuinely fails
# (both examples) when `#[global_allocator]` is temporarily removed and the
# extension rebuilt -- not a check that would pass regardless.
# rubocop:disable Metrics/BlockLength -- one real-binary integration gate
# plus its own well-documented helper methods reads better kept together in
# one file, matching this project's established precedent (e.g.
# spec/integration/phase3_streaming_body_spec.rb), than split up for a
# line-count target.
RSpec.describe "Phase 10: allocator integration gate (RNF03)" do
  # A safety-review finding: this gate's actual inspection technique --
  # disassembling a literal `jmp *offset(%rip)` instruction, GNU
  # `objdump -R`'s `*ABS*+0x...` relocation format, a `.so` filename
  # (macOS builds a `.bundle`) -- is x86-64 Linux/GNU-binutils specific, and
  # the original version of this file *raised* rather than skipped on
  # anything else, which would fail loudly for a contributor on e.g. Apple
  # Silicon or aarch64 Linux running `bundle exec rake` rather than telling
  # them why. CI (`main.yml`) is ubuntu-latest x86-64 only, so this never
  # fired there -- but a gate that can only pass or raise on the one
  # platform it was written against isn't honestly "skipped elsewhere" the
  # way `PLAN.md`'s own Phase 9 gate note already established as the right
  # shape for a platform-specific check.
  before do
    unless RUBY_PLATFORM.include?("linux") && RUBY_PLATFORM.include?("x86_64")
      skip "this gate's binary-inspection technique is x86-64 Linux/GNU-binutils specific " \
           "(RUBY_PLATFORM=#{RUBY_PLATFORM}); see PLAN.md's Phase 10 Resolution note"
    end
    %w[nm objdump].each do |tool|
      skip "#{tool} not found on PATH -- required for this gate's binary inspection" if `which #{tool}`.empty?
    end
  end

  # The exact path `require "helix_rack"` actually loaded -- not a guessed
  # build-output path, which would drift the moment the build layout does.
  let(:so_path) do
    require "helix_rack"
    $LOADED_FEATURES.find { |path| path.end_with?("helix_rack.so") } ||
      raise("could not find the loaded helix_rack.so among $LOADED_FEATURES")
  end

  it "links mimalloc's own allocation symbols into the compiled extension" do
    symbols = `nm "#{so_path}" 2>/dev/null`

    expect(symbols).to match(/\bmi_malloc/), "expected mimalloc's own symbols (e.g. mi_malloc_aligned) " \
                                              "to appear in `nm` output for #{so_path}, found none"
  end

  it "wires Rust's __rust_alloc hook to a mimalloc symbol, not the platform default allocator" do
    rust_alloc_address = symbol_address(so_path, /___rust_alloc$/)
    mimalloc_addresses = symbol_addresses(so_path, /\bmi_malloc/)

    expect(mimalloc_addresses).not_to be_empty, "found no defined mimalloc symbols to compare against"

    resolved_target = got_relocation_target(so_path, rust_alloc_address)

    expect(mimalloc_addresses).to include(resolved_target),
                                  "__rust_alloc's indirect jump resolved to " \
                                  "#{resolved_target.to_s(16)}, which is not one of mimalloc's own " \
                                  "symbol addresses (#{mimalloc_addresses.map { |a| a.to_s(16) }}) -- " \
                                  "the global allocator may not actually be mimalloc"
  end

  # `nm`'s plain, non-demangled output: Rust symbol names are mangled
  # (`_RNvCs...___rust_alloc`), so matching on a `___rust_alloc$` suffix
  # rather than the exact name is what's actually stable across rustc
  # versions/mangling schemes. Addresses are parsed as integers throughout
  # this file, not compared as hex strings -- `nm` zero-pads to 16 digits,
  # `objdump`/`readelf` don't, and comparing the raw strings would silently
  # never match.
  def symbol_address(so_path, name_pattern)
    line = `nm "#{so_path}" 2>/dev/null`.lines.find { |candidate| candidate.split(" ", 3)[2]&.match?(name_pattern) }
    raise "no symbol matching #{name_pattern.inspect} found in #{so_path}" unless line

    line.split(" ", 2).first.to_i(16)
  end

  def symbol_addresses(so_path, name_pattern)
    `nm "#{so_path}" 2>/dev/null`.lines.filter_map do |line|
      address, _type, name = line.split(" ", 3)
      address.to_i(16) if name&.match?(name_pattern)
    end
  end

  # `__rust_alloc`'s own body is a single indirect jump through the GOT
  # (`jmp *offset(%rip)`, confirmed by hand with `objdump -d` before writing
  # this) -- this resolves that jump's target the same way: find the GOT
  # relocation whose *offset* the jump instruction references (via
  # `objdump -d`'s own disassembly comment, e.g. `# de4d8 <...>`), then read
  # the address that relocation actually points at (via `objdump -R`'s
  # `R_X86_64_RELATIVE ... *ABS*+0x...` line for that same offset).
  def got_relocation_target(so_path, function_address)
    got_offset = got_offset_from_indirect_jump(so_path, function_address)

    relocations = `objdump -R "#{so_path}" 2>/dev/null`
    relocation_line = relocations.lines.find { |line| line.split(" ", 2).first.to_i(16) == got_offset }
    raise "no relocation entry found for GOT offset #{got_offset.to_s(16)}" unless relocation_line

    relocation_line[/\*ABS\*\+0x([0-9a-f]+)/, 1].to_i(16)
  end

  def got_offset_from_indirect_jump(so_path, function_address)
    disassembly = `objdump -d --start-address=0x#{function_address.to_s(16)} "#{so_path}" 2>/dev/null`
    jump_line = disassembly.lines.find { |line| line.include?("jmp") && line.include?("(%rip)") }
    raise "expected an indirect `jmp *offset(%rip)` as __rust_alloc's first instruction, found none" unless jump_line

    got_offset = jump_line[/#\s*([0-9a-f]+)\s*</, 1]
    raise "could not parse the GOT offset out of: #{jump_line.inspect}" unless got_offset

    got_offset.to_i(16)
  end
end
# rubocop:enable Metrics/BlockLength
