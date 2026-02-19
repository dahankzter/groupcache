.PHONY: all build check test clippy fmt lint doc clean \
       coverage coverage-summary coverage-html

all: fmt lint test doc  ## Run fmt, lint, test, and doc (default)

# ── Build ──────────────────────────────────────────────

build:  ## Compile the workspace
	cargo build --workspace

check:  ## Type-check without codegen
	cargo check --workspace

# ── Quality ────────────────────────────────────────────

test:  ## Run all tests
	cargo test --workspace

clippy:  ## Run clippy with warnings as errors
	cargo clippy --workspace -- -D warnings

fmt:  ## Check formatting (use fmt-fix to auto-fix)
	cargo fmt --all -- --check

fmt-fix:  ## Auto-fix formatting
	cargo fmt --all

lint: clippy fmt  ## Run clippy + fmt check

# ── Documentation ──────────────────────────────────────

doc:  ## Build documentation
	cargo doc --workspace --no-deps

# ── Coverage (requires cargo-llvm-cov) ─────────────────

COV_IGNORE := --ignore-filename-regex 'groupcache_pb\.rs'

coverage:  ## Generate lcov coverage report → coverage/lcov.info
	cargo llvm-cov --workspace --all-features --lcov --output-path coverage/lcov.info $(COV_IGNORE)

coverage-summary:  ## Print per-file coverage summary to stdout
	cargo llvm-cov --workspace --all-features $(COV_IGNORE)

coverage-html:  ## Generate HTML coverage report → coverage/html/
	cargo llvm-cov --workspace --all-features --html --output-dir coverage/html $(COV_IGNORE)
	@echo "Report: coverage/html/index.html"

# ── Cleanup ────────────────────────────────────────────

clean:  ## Remove build artifacts and coverage data
	cargo clean
	cargo llvm-cov clean --workspace 2>/dev/null || true
	rm -rf coverage/

# ── Help ───────────────────────────────────────────────

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
