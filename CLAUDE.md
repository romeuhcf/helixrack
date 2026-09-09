# HelixRack

Rust-native HTTP/1.1 server embedding CRuby (rb-sys/magnus) for Rack/Grape apps. See `PRD.md`
for requirements and `PLAN.md` for the phased build plan once it lands.

## Source of truth

`PLAN.md` is the source of truth for what "done" means at each phase: it defines the deliverable
and the deterministic gate (a test, not a benchmark) for that phase. Do not invent a different
definition of done — if a phase's gate is unclear or missing from `PLAN.md`, fix `PLAN.md` first.

## Workflow

1. Before writing implementation code for a phase, the gate (the test that proves the phase's
   deliverable) must exist and must currently fail for the right reason. Use the `gate-writer`
   subagent for this.
2. Implement the minimal code to turn that gate green. Use the `phase-builder` subagent, or do it
   directly for small phases.
3. Any diff touching `ext/` (the Rust extension), GVL acquire/release points, or `unsafe` Rust
   must go through the `safety-reviewer` subagent (or `/security-review`) before it is proposed
   for merge. This is the one place a bug segfaults the whole process instead of raising.
4. `bundle exec rake` (rubocop + rspec) must pass locally before pushing. If the change touches
   `ext/`, also run `cargo test --manifest-path ext/helix_rack/Cargo.toml`.

## Gate discipline (see PLAN.md for full detail)

Tests that prove timing-sensitive behavior (GVL release, keep-alive timeout, graceful shutdown)
must not rely on `sleep`-based races. Use explicit synchronization (a barrier, a pipe, a counter)
so the same test passes or fails identically on any machine speed.

## Contribution flow

`main` requires 1 CODEOWNERS-approved PR review before merge (see `.github/CODEOWNERS`) and a
passing CI check, from anyone but the repo owner (who can push directly, but should still prefer
a PR for anything beyond a one-line fix, per the same worktree-and-review discipline used
everywhere else).
