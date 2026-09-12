.PHONY: build test lint snapshot clean dev-env-check dev-login dev-up dev-status dev-whoami dev-logout

-include Makefile.local

DEV_ENV = MCPWARP_AUTH_URL=$(DEV_AUTH_URL) MCPWARP_CONNECT_URL=$(DEV_CONNECT_URL) MCPWARP_WEB_URL=$(DEV_WEB_URL)

build:
	go build -ldflags "-X main.version=$(shell git describe --tags --always --dirty 2>/dev/null || echo dev)" -o bin/mcpwarp ./cmd/mcpwarp

test:
	CGO_ENABLED=1 go test -race -count=1 ./...

lint:
	go vet ./...
	@fmt_out="$$(gofmt -l .)"; \
	if [ -n "$$fmt_out" ]; then \
		echo "$$fmt_out"; \
		exit 1; \
	fi

snapshot:
	goreleaser release --snapshot --clean

clean:
	rm -rf bin dist

# dev-*: run against a dev stack; DEV_* come from Makefile.local (gitignored) or the command line; extra flags via ARGS=
dev-env-check:
	@if [ -z "$(DEV_AUTH_URL)" ] || [ -z "$(DEV_CONNECT_URL)" ] || [ -z "$(DEV_WEB_URL)" ]; then \
		echo "set DEV_AUTH_URL, DEV_CONNECT_URL and DEV_WEB_URL in Makefile.local (gitignored) or on the command line"; \
		exit 1; \
	fi

dev-login: dev-env-check build
	$(DEV_ENV) ./bin/mcpwarp login $(ARGS)

dev-up: dev-env-check build
	$(DEV_ENV) ./bin/mcpwarp up $(ARGS)

dev-status: dev-env-check build
	$(DEV_ENV) ./bin/mcpwarp status $(ARGS)

dev-whoami: dev-env-check build
	$(DEV_ENV) ./bin/mcpwarp whoami $(ARGS)

dev-logout: dev-env-check build
	$(DEV_ENV) ./bin/mcpwarp logout $(ARGS)
