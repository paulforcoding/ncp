VERSION ?= 0.5.1
LDFLAGS := -ldflags "-X main.version=$(VERSION)"
GOBIN := $(shell go env GOPATH)/bin

.PHONY: build test unit integration lint lint-local fmt-check tools clean

build:
	go build $(LDFLAGS) -o ncp ./cmd/ncp

test: unit integration

unit:
	go test ./... -count=1 -race

integration:
	go test ./... -count=1 -race -tags=integration -run Integration

lint:
	golangci-lint run ./...

fmt-check:
	@out=$$(gofmt -l .); \
	if [ -n "$$out" ]; then \
		echo "gofmt issues in:"; echo "$$out"; exit 1; \
	fi

tools:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install honnef.co/go/tools/cmd/staticcheck@latest
	go install golang.org/x/vuln/cmd/govulncheck@latest

lint-local: fmt-check
	$(GOBIN)/golangci-lint run ./...
	$(GOBIN)/staticcheck ./...

clean:
	rm -f ncp
	go clean ./...
