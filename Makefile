VERSION ?= 0.5.6
LDFLAGS := -ldflags "-X main.version=$(VERSION)"
GOBIN := $(shell go env GOPATH)/bin

.PHONY: build build-linux-amd64 build-linux-arm64 build-darwin-amd64 build-darwin-arm64 install test unit integration lint lint-local fmt-check tools clean

build:
	go build $(LDFLAGS) -o ncp ./cmd/ncp

build-linux-amd64:
	GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o ncp-linux-amd64 ./cmd/ncp

build-linux-arm64:
	GOOS=linux GOARCH=arm64 go build $(LDFLAGS) -o ncp-linux-arm64 ./cmd/ncp

build-darwin-amd64:
	GOOS=darwin GOARCH=amd64 go build $(LDFLAGS) -o ncp-darwin-amd64 ./cmd/ncp

build-darwin-arm64:
	GOOS=darwin GOARCH=arm64 go build $(LDFLAGS) -o ncp-darwin-arm64 ./cmd/ncp

install: build
	sudo cp ncp /usr/local/bin/
	@echo ""
	@echo "ncp installed to /usr/local/bin/ncp"
	@echo "Tip: 让你的 AI agent 安装 skill/master-ncp skill"

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
	rm -f ncp ncp-linux-amd64 ncp-linux-arm64 ncp-darwin-amd64 ncp-darwin-arm64
	go clean ./...
