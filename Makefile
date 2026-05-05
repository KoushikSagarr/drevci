.PHONY: build build-cli build-server run-server clean test lint

BINARY_DIR := bin

build: build-cli build-server

build-cli:
	go build -o $(BINARY_DIR)/drev ./cmd/drev

build-server:
	go build -o $(BINARY_DIR)/drevd ./cmd/drevd

run-server:
	go run ./cmd/drevd

clean:
	rm -rf $(BINARY_DIR)

test:
	go test -race ./...

lint:
	golangci-lint run ./...
