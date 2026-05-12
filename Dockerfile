# syntax=docker/dockerfile:1.7

# ── Builder ──────────────────────────────────────────────────────────────────
# Build the `xero` binary in release mode. Cache `target/` and the cargo
# registry so subsequent rebuilds only recompile changed crates.
FROM rust:1.80-bookworm AS builder

WORKDIR /workspace

# Workspace + crate manifests first so cargo can resolve and cache deps before
# the rest of the source changes. The hollow `lib.rs` / `main.rs` stubs trick
# cargo into compiling deps without recompiling our code yet.
COPY Cargo.toml Cargo.lock ./
COPY rust-toolchain.toml ./
COPY core/crates/xero-common/Cargo.toml   core/crates/xero-common/Cargo.toml
COPY core/crates/xero-auth/Cargo.toml     core/crates/xero-auth/Cargo.toml
COPY core/crates/xero-state/Cargo.toml    core/crates/xero-state/Cargo.toml
COPY core/crates/xero-client/Cargo.toml   core/crates/xero-client/Cargo.toml
COPY core/crates/xero-sync/Cargo.toml     core/crates/xero-sync/Cargo.toml
COPY core/crates/xero-http/Cargo.toml     core/crates/xero-http/Cargo.toml
COPY core/crates/xero-cli/Cargo.toml      core/crates/xero-cli/Cargo.toml

RUN mkdir -p \
        core/crates/xero-common/src \
        core/crates/xero-auth/src \
        core/crates/xero-state/src \
        core/crates/xero-client/src \
        core/crates/xero-sync/src \
        core/crates/xero-http/src \
        core/crates/xero-cli/src && \
    echo 'pub fn _stub() {}'  > core/crates/xero-common/src/lib.rs && \
    echo 'pub fn _stub() {}'  > core/crates/xero-auth/src/lib.rs && \
    echo 'pub fn _stub() {}'  > core/crates/xero-state/src/lib.rs && \
    echo 'pub fn _stub() {}'  > core/crates/xero-client/src/lib.rs && \
    echo 'pub fn _stub() {}'  > core/crates/xero-sync/src/lib.rs && \
    echo 'pub fn _stub() {}'  > core/crates/xero-http/src/lib.rs && \
    echo 'fn main() {}'       > core/crates/xero-cli/src/main.rs

# Compile deps only. BuildKit cache mounts keep the cargo registry + target
# warm across image rebuilds without bloating the final layer.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/workspace/target \
    cargo build --release --bin xero || true

# Real sources + migrations.
COPY core ./core
COPY migrations ./migrations

# Touch lib.rs / main.rs so cargo notices the change and rebuilds them.
RUN find core/crates -name 'Cargo.toml' -exec touch {} +

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/workspace/target \
    cargo build --release --bin xero && \
    cp /workspace/target/release/xero /usr/local/bin/xero

# ── Runtime ──────────────────────────────────────────────────────────────────
# Debian slim (not distroless) so the CA bundle + libssl are present for the
# Xero HTTPS + BigQuery REST calls, plus `tini` for clean PID 1 signal handling.
FROM debian:bookworm-slim AS runtime

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        tini && \
    rm -rf /var/lib/apt/lists/*

# Run as a non-root, non-login user.
RUN groupadd --system --gid 10001 xero && \
    useradd  --system --uid 10001 --gid xero --no-create-home --shell /usr/sbin/nologin xero

WORKDIR /app
COPY --from=builder /usr/local/bin/xero /app/xero
COPY migrations /app/migrations

USER xero:xero
EXPOSE 5002

ENV RUST_LOG=info,xero_cli=info,xero_state=info,xero_http=info,xero_sync=info,xero_client=info
ENV XERO_HTTP_BIND=0.0.0.0:5002

# `xero` reads `DATABASE_URL`/`REDIS_URL` etc from env. Run migrations then
# serve. tini reaps zombies and forwards SIGTERM cleanly for Cloud Run / GKE
# graceful shutdown.
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/bin/sh", "-c", "/app/xero db-migrate && exec /app/xero serve"]
