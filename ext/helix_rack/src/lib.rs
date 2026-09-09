use magnus::{Error, Ruby};

#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    // Phase 2 (see `PLAN.md` at the repo root) has not wired this module up
    // to `engine`'s `Handler` trait yet: the Ruby-facing entry point
    // (`HelixRack.serve`) lives in `lib/helix_rack.rb` and raises
    // `NotImplementedError` until that wiring exists. Nothing to bind here
    // yet, so this just declares the module the Ruby side reopens.
    ruby.define_module("HelixRack")?;
    Ok(())
}
