# syntax=docker/dockerfile:1.7

ARG RUST_IMAGE=rust:1-bookworm
ARG RUNTIME_IMAGE=gcr.io/distroless/cc-debian12:nonroot

FROM ${RUST_IMAGE} AS chef
WORKDIR /workspace
RUN cargo install cargo-chef --locked --version 0.1.71

# The recipe describes every dependency the workspace pins, and changes only when
# a manifest does.
FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS build

# Dependencies build from the recipe alone, so this layer survives any change to
# the workspace's own sources. A cache mount would hold the artifacts outside the
# image, where an ephemeral CI runner cannot reach them.
COPY --from=planner /workspace/recipe.json recipe.json
RUN cargo chef cook --locked --release --recipe-path recipe.json

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# BIN enters after the shared work, so every binary reuses one cooked dependency
# layer and one copy of the sources.
ARG BIN
RUN test -n "${BIN}"
RUN cargo build --locked --release -p "${BIN}" --bin "${BIN}" && \
    mkdir -p /out/usr/local/bin && \
    cp "target/release/${BIN}" "/out/usr/local/bin/${BIN}" && \
    ln -s "${BIN}" /out/usr/local/bin/sleepypods

FROM ${RUNTIME_IMAGE}

ARG BIN
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/ /

USER 65532:65532
ENTRYPOINT ["/usr/local/bin/sleepypods"]
