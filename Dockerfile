# syntax=docker/dockerfile:1
#
# Multi-stage build for Ledger (events/jobs vhost demux over two vendored library crates, served on
# BOTH :9150 and :9160).
#   - builder: rust:1.96-slim (Debian trixie).
#   - runtime: debian:trixie-slim (matching glibc), non-root, ca-certificates.
#
# The two surfaces embed their templates + static CSS via include_str! at COMPILE time, so the
# runtime image carries only the single statically-templated binary — no assets to ship. sqlx uses
# rustls (ring) and Tempo's reqwest client is rustls-tls, so the binary depends only on glibc — NO
# OpenSSL. ca-certificates is kept because Tempo fires HTTPS cron targets + the Klaxon notify. Ark
# (the only surface needing pg_dump) is NOT part of Ledger, so the runtime image carries no
# postgresql-client. The HEALTHCHECK uses the built-in `ledger healthcheck` subcommand, so the image
# needs no curl.

FROM rust:1.96-slim AS builder
WORKDIR /build

# Bring the whole self-contained crate (the binary + the two vendored surface crates under
# crates/) and build the release binary. The surfaces' static/ + templates/ are needed at build
# time for their include_str! embeds.
COPY Cargo.toml ./
COPY src ./src
COPY crates ./crates
RUN cargo build --release --bin ledger \
    && strip target/release/ledger

FROM debian:trixie-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Non-root runtime user (no shell, no home writes needed).
RUN useradd --system --uid 10001 --user-group --no-create-home ledger
COPY --from=builder /build/target/release/ledger /usr/local/bin/ledger

USER ledger
ENV BIND_ADDR=0.0.0.0:9150
ENV BIND_ADDR_SECONDARY=0.0.0.0:9160
EXPOSE 9150 9160

# Dependency-free liveness probe -> GET /healthz on the loopback (primary port), exit 0/1.
HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
    CMD ["ledger", "healthcheck"]

CMD ["ledger"]
