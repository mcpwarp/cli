.PHONY: build test lint snapshot clean

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
