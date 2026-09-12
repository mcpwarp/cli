.PHONY: help build test lint snapshot clean dev-env-check dev-login dev-up dev-status dev-whoami dev-logout dev-dashboard

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-14s %s\n", $$1, $$2}'

-include Makefile.local

DEV_ENV = MCPWARP_AUTH_URL=$(DEV_AUTH_URL) MCPWARP_CONNECT_URL=$(DEV_CONNECT_URL) MCPWARP_WEB_URL=$(DEV_WEB_URL)

build: ## Build bin/mcpwarp
	go build -ldflags "-X main.version=$(shell git describe --tags --always --dirty 2>/dev/null || echo dev)" -o bin/mcpwarp ./cmd/mcpwarp

test: ## Run tests with -race
	CGO_ENABLED=1 go test -race -count=1 ./...

lint: ## go vet + gofmt check
	go vet ./...
	@fmt_out="$$(gofmt -l .)"; \
	if [ -n "$$fmt_out" ]; then \
		echo "$$fmt_out"; \
		exit 1; \
	fi

snapshot: ## goreleaser snapshot build
	goreleaser release --snapshot --clean

clean: ## Remove bin/ and dist/
	rm -rf bin dist

# dev-*: run against a dev stack; DEV_* come from Makefile.local (gitignored) or the command line; extra flags via ARGS=
dev-env-check:
	@if [ -z "$(DEV_AUTH_URL)" ] || [ -z "$(DEV_CONNECT_URL)" ] || [ -z "$(DEV_WEB_URL)" ]; then \
		echo "set DEV_AUTH_URL, DEV_CONNECT_URL and DEV_WEB_URL in Makefile.local (gitignored) or on the command line"; \
		exit 1; \
	fi

dev-login: dev-env-check build ## Log in against the dev stack
	$(DEV_ENV) ./bin/mcpwarp login $(ARGS)

dev-up: dev-env-check build ## Run up against the dev stack
	$(DEV_ENV) ./bin/mcpwarp up $(ARGS)

dev-status: dev-env-check build ## Show status against the dev stack
	$(DEV_ENV) ./bin/mcpwarp status $(ARGS)

dev-whoami: dev-env-check build ## Show who is logged in against the dev stack
	$(DEV_ENV) ./bin/mcpwarp whoami $(ARGS)

dev-logout: dev-env-check build ## Log out from the dev stack
	$(DEV_ENV) ./bin/mcpwarp logout $(ARGS)

dev-dashboard: dev-env-check build ## Open the dev dashboard in your browser
	$(DEV_ENV) ./bin/mcpwarp dashboard $(ARGS)
