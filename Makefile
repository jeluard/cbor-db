SHELL := /bin/sh

CBOR_DB_SCHEMA ?= schemas/conway.cddl
export CBOR_DB_SCHEMA

CARGO ?= cargo
NODE ?= node
BUILD_PROFILE ?= dev
BENCH_PACKAGE ?= cbordb-benchmarks
BENCH_OUT_DIR ?= docs/benchmarks
BENCH_README ?= README.md
BENCH_README_RENDERER ?= scripts/render-benchmark-readme.js
BENCH_BACKENDS ?= memory,sled,rocksdb,fjall,surrealkv,tidehunter,turso
BENCH_ENTRIES ?= 4000
BENCH_KEY_SIZE ?= 24
BENCH_VALUE_SIZE ?= 256
BENCH_SUBSET_SIZE ?= 16
BENCH_NAMESPACES ?= 4
ARGS ?=

ifeq ($(BUILD_PROFILE),dev)
PROFILE_FLAG :=
else ifeq ($(BUILD_PROFILE),release)
PROFILE_FLAG := --release
else
PROFILE_FLAG := --profile $(BUILD_PROFILE)
endif

.PHONY: help build bench bench-quick test

help:
	@echo "\033[1;4mGetting Started:\033[0m"
	@grep -E '^[a-zA-Z0-9._-]+:.*## &start ' Makefile | while read -r l; do \
		printf "  \033[1;32m%s\033[0m:%s\n" "$$(echo $$l | cut -d':' -f1)" "$$(echo $$l | sed 's/^[^#]*## *\&start *//')"; \
	done
	@echo ""
	@echo "\033[1;4mBuild And Run:\033[0m"
	@grep -E '^[a-zA-Z0-9._-]+:.*## &build ' Makefile | while read -r l; do \
		printf "  \033[1;32m%s\033[0m:%s\n" "$$(echo $$l | cut -d':' -f1)" "$$(echo $$l | sed 's/^[^#]*## *\&build *//')"; \
	done
	@echo ""
	@echo "\033[1;4mDev And Test:\033[0m"
	@grep -E '^[a-zA-Z0-9._-]+:.*## &test ' Makefile | while read -r l; do \
		printf "  \033[1;32m%s\033[0m:%s\n" "$$(echo $$l | cut -d':' -f1)" "$$(echo $$l | sed 's/^[^#]*## *\&test *//')"; \
	done
	@echo ""
	@echo "\033[1;4mConfiguration:\033[0m"
	@grep -E '^[A-Z0-9_]+ \?= ' Makefile | sort | while read -r l; do \
		printf "  \033[36m%s\033[0m=%s\n" "$$(echo $$l | cut -d'=' -f1 | tr -d ' ')" "$$(echo $$l | cut -d'=' -f2-)"; \
	done

build: ## &build Build the full workspace for $(BUILD_PROFILE)
	$(CARGO) build $(PROFILE_FLAG) --workspace $(ARGS)

bench: ## &build Run backend benchmarks and refresh benchmark docs
	mkdir -p "$(BENCH_OUT_DIR)"
	CBOR_DB_BENCH_OUT_DIR="$(BENCH_OUT_DIR)" \
	CBOR_DB_BENCH_BACKENDS="$(BENCH_BACKENDS)" \
	CBOR_DB_BENCH_ENTRIES="$(BENCH_ENTRIES)" \
	CBOR_DB_BENCH_KEY_SIZE="$(BENCH_KEY_SIZE)" \
	CBOR_DB_BENCH_VALUE_SIZE="$(BENCH_VALUE_SIZE)" \
	CBOR_DB_BENCH_SUBSET_SIZE="$(BENCH_SUBSET_SIZE)" \
	CBOR_DB_BENCH_NAMESPACES="$(BENCH_NAMESPACES)" \
	$(CARGO) run --release -p $(BENCH_PACKAGE)
	$(NODE) $(BENCH_README_RENDERER) "$(BENCH_OUT_DIR)/results.json" "$(BENCH_README)"

bench-quick: ## &build Run a smaller backend benchmark pass for local iteration
	$(MAKE) bench BENCH_ENTRIES=500

test: ## &test Run the full workspace test suite
	CBOR_DB_SCHEMA="$(CBOR_DB_SCHEMA)" $(CARGO) test --workspace $(ARGS)
