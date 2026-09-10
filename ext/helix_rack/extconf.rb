# frozen_string_literal: true

require "mkmf"
require "rb_sys/mkmf"

create_rust_makefile("helix_rack/helix_rack") do |r|
  # Empty (all default-off) unless overridden -- e.g.
  # `RB_SYS_CARGO_FEATURES=combine-write bundle exec rake compile` to build
  # the opt-in path documented on `write_response` in
  # `engine/src/connection.rs`. Read here, not left unset, because rb_sys's
  # own `RB_SYS_CARGO_FEATURES` Makefile-level override (`rb_sys/mkmf.rb`)
  # only takes effect on a `--features` flag that's already present in the
  # generated cargo command -- with `r.features` never set, `CargoBuilder`
  # never emits `--features` at all (`unless features.empty?`), so that
  # override does nothing. Reading `ENV` here instead sidesteps that: the
  # flag `rake compile` embeds is decided at extconf-time, from whatever
  # `RB_SYS_CARGO_FEATURES` this process was invoked with.
  r.features = ENV.fetch("RB_SYS_CARGO_FEATURES", "").split(",")
end
