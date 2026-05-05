.PHONY: build build-cli build-server run-server clean test lint token example

BINARY_DIR := bin

build:
	go build -o $(BINARY_DIR)/drev ./cmd/drev
	go build -o $(BINARY_DIR)/drevd ./cmd/drevd

run-server:
	go run ./cmd/drevd

test:
	go test ./... -v -count=1

lint:
	go vet ./...

clean:
	rm -rf $(BINARY_DIR) logs/ drev.db

token:
	go run ./cmd/drev token generate

example:
	go run ./cmd/drevd &
	sleep 1
	go run ./cmd/drev run configs/example.drev.yml
