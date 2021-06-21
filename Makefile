.DEFAULT_GOAL := help
.PHONY: help
help: ## shows this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: pr-commit-check
pr-check-commit: ## run pre-commit checks on demand
	pre-commit run --hook-stage commit --all-files

.PHONY: pr-check-commits
pr-check-commits: ## performs a git rebase to assert every commit conforms to pre-commit checks
	git rebase -i master --exec 'pre-commit run -s $$(git rev-parse HEAD^) -o $$(git rev-parse HEAD)'

.PHONY: pr-check
pr-check: ## performs all pre-commit checks (commit and push) on demand for the current branch (used for CI)
	pre-commit run --all-files --hook-stage commit --hook-stage push
