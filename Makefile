.PHONY: up down db-check db-migrate build test fmt lint clean

# ── Local dev stack ───────────────────────────────────────────────────────────
up:
	docker compose up -d
	@echo "Waiting for Postgres to be ready…"
	@until docker compose exec xero-postgres pg_isready -U xero_user -d xero_v2 >/dev/null 2>&1; do sleep 1; done
	@echo "✓  Postgres ready on localhost:5434"

down:
	docker compose down

# ── Database ──────────────────────────────────────────────────────────────────
db-check: ## Verify DB connectivity and print schema status
	cargo run -p xero-cli -- db-check

db-migrate: ## Apply all migrations
	cargo run -p xero-cli -- db-migrate

# ── Quick local test flow (stack up → migrate → check) ───────────────────────
local-test: up db-migrate db-check
	cargo run -p xero-cli -- healthcheck

# ── Build ─────────────────────────────────────────────────────────────────────
build:
	cargo build --workspace

release:
	cargo build --workspace --release

# ── Tests ─────────────────────────────────────────────────────────────────────
test:
	cargo test --workspace

test-py:
	cd tooling && python -m pytest tests/ -v --tb=short

# ── Lint / format ─────────────────────────────────────────────────────────────
fmt:
	cargo fmt --all

lint:
	cargo clippy --workspace -- -D warnings
	cd tooling && ruff check src/

# ── HTTP server ───────────────────────────────────────────────────────────────
serve:
	cargo run -p xero-cli -- serve

# ── Cleanup ───────────────────────────────────────────────────────────────────
clean:
	cargo clean
	docker compose down -v
