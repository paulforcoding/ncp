VERSION ?= 0.2.0
LDFLAGS := -ldflags "-X main.version=$(VERSION)"

.PHONY: build test unit integration lint clean

build:
	go build $(LDFLAGS) -o ncp ./cmd/ncp

test: unit integration

unit:
	go test ./... -count=1 -race

integration:
	go test ./... -count=1 -race -tags=integration -run Integration

lint:
	golangci-lint run ./...

clean:
	rm -f ncp
	go clean ./...
