# Phase 13 (PLAN.md, Phase 13): one shared image for every server this
# benchmark compares (HelixRack, Puma, Falcon) and every scenario app --
# only the container's `CMD`/env vars differ per run (`bench/run.rb`
# decides those). Builds the Rust extension *inside* the image (rustup +
# build-essential), rather than copying the host's already-compiled `.so`
# in: this image's glibc isn't guaranteed to match the host's, and a
# mismatched glibc would fail to load the `.so` at runtime with no useful
# error until the exact moment a benchmark run needed it.
FROM ruby:4.0.6-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl libpq-dev pkg-config git libclang-dev \
    && rm -rf /var/lib/apt/lists/*

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal

WORKDIR /app
COPY Gemfile Gemfile.lock* helix_rack.gemspec ./
COPY lib/helix_rack/version.rb lib/helix_rack/version.rb
COPY ext ext
COPY engine engine
COPY Cargo.toml Cargo.lock* ./
RUN bundle install

COPY exe exe
COPY lib lib
COPY Rakefile ./
# Empty (default-off) unless the build is invoked with
# `--build-arg RB_SYS_CARGO_FEATURES=...` -- see `bench/run.rb`'s
# `BENCH_CARGO_FEATURES` for how this image is built with an opt-in Cargo
# feature enabled, to measure it against the default-off baseline
# independently (e.g. `engine/src/connection.rs`'s `combine-write`).
ARG RB_SYS_CARGO_FEATURES=
ENV RB_SYS_CARGO_FEATURES=${RB_SYS_CARGO_FEATURES}
RUN bundle exec rake compile:release

COPY bench/apps bench/apps
COPY bench/entrypoint.sh bench/entrypoint.sh

ENTRYPOINT ["bench/entrypoint.sh"]
