help:	## Display this message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
.PHONY: help
.DEFAULT_GOAL := help

Cargo.nix: Cargo.toml
	crate2nix generate -n ./nix/packages.nix

test: ## Run tests
	cargo test
.PHONY: test

lint: ## Run linters
	cargo-clippy
.PHONY: lint

coverage: ## Run tests with coverage
	./script/run-coverage.sh
.PHONY: coverage
