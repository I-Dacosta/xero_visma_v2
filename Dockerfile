# syntax=docker/dockerfile:1.7

# ── Builder ──────────────────────────────────────────────────────────────────
# Build the `xero` binary in release mode. Cache `target/` and the cargo
# registry so subsequent rebuilds only recompile changed crates.
FROM rust:1.80-bookworm AS builder

WORKDIR /workspace

# `aws-lc-sys` (pulled in transitively by gcloud-storage's default
# `jwt-aws-lc-rs` feature) compiles its C library from source and needs
# cmake + perl + a C compiler. gcc is already in the rust image; add the rest.
RUN apt-get update && \
    apt-get install -y --no-install-recommends cmake perl && \
    rm -rf /var/lib/apt/lists/*

# Cap parallel codegen units to keep peak RAM low on memory-constrained build
# hosts (the prod VPS is shared with ~30 containers). Slower but OOM-safe.
ENV CARGO_BUILD_JOBS=2
ENV CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16

# Workspace + crate manifests first so cargo can resolve and cache deps before
# the rest of the source changes. The hollow `lib.rs` / `main.rs` stubs trick
# cargo into compiling deps without recompiling our code yet.
COPY Cargo.toml Cargo.lock ./
COPY rust-toolchain.toml ./
COPY core/crates/xero-common/Cargo.toml   core/crates/xero-common/Cargo.toml
COPY core/crates/xero-auth/Cargo.toml     core/crates/xero-auth/Cargo.toml
COPY core/crates/xero-client/Cargo.toml   core/crates/xero-client/Cargo.toml
COPY core/crates/xero-sync/Cargo.toml     core/crates/xero-sync/Cargo.toml
COPY core/crates/xero-gcs/Cargo.toml      core/crates/xero-gcs/Cargo.toml
COPY core/crates/xero-cli/Cargo.toml      core/crates/xero-cli/Cargo.toml

RUN mkdir -p \
        core/crates/xero-common/src \
        core/crates/xero-auth/src \
        core/crates/xero-client/src \
        core/crates/xero-sync/src \
        core/crates/xero-gcs/src \
        core/crates/xero-cli/src && \
    echo 'pub fn _stub() {}'  > core/crates/xero-common/src/lib.rs && \
    echo 'pub fn _stub() {}'  > core/crates/xero-auth/src/lib.rs && \
    echo 'pub fn _stub() {}'  > core/crates/xero-client/src/lib.rs && \
    echo 'pub fn _stub() {}'  > core/crates/xero-sync/src/lib.rs && \
    echo 'pub fn _stub() {}'  > core/crates/xero-gcs/src/lib.rs && \
    echo 'fn main() {}'       > core/crates/xero-cli/src/main.rs

# Compile deps only. BuildKit cache mounts keep the cargo registry + target
# warm across image rebuilds without bloating the final layer.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/workspace/target \
    cargo build --release --bin xero || true

# Real sources.
COPY core ./core

# Touch the REAL source files (not just Cargo.toml) so cargo's mtime-based
# fingerprint detects the change and recompiles our crates. Touching only
# Cargo.toml left the dep-cache stub binary (`fn main(){}`) in target, which
# then shipped as a no-op — this is the fix for that.
RUN find core -name '*.rs' -exec touch {} +

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/workspace/target \
    rm -f /workspace/target/release/xero && \
    cargo build --release --bin xero && \
    test -x /workspace/target/release/xero && \
    cp /workspace/target/release/xero /usr/local/bin/xero

# ── Runtime ──────────────────────────────────────────────────────────────────
# Debian slim (not distroless) so the CA bundle + libssl are present for the
# Xero HTTPS + GCS REST calls, plus `tini` for clean PID 1 signal handling.
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

USER xero:xero

# Default tracing filter. Override with RUST_LOG at run time.
ENV RUST_LOG=info,xero_cli=info,xero_sync=info,xero_client=info,xero_gcs=info,xero_auth=info

# This image is a ONE-SHOT JOB, not a long-running server. There is no Postgres,
# Redis, BigQuery, or HTTP listener — the host scheduler (cron / systemd timer /
# Cloud Scheduler / Cloud Run Job) invokes the container with a `sync …` (or
# `healthcheck`) argument, the job runs to completion, writes raw pages to GCS,
# and exits. tini reaps zombies and forwards SIGTERM for clean cancellation.
#
# Examples:
#   docker run --rm --env-file .env xero-service-v2 sync --window-days 3
#   docker run --rm --env-file .env xero-service-v2 healthcheck --check-bucket
ENTRYPOINT ["/usr/bin/tini", "--", "/app/xero"]
CMD ["healthcheck"]
