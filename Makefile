.PHONY: build build-cli build-server run-server clean test lint fmt fmt-check token example dev dev-status dev-logs dev-stop dev-test

BINARY_DIR := bin

build:
	go build -o $(BINARY_DIR)/drev ./cmd/drev
	go build -o $(BINARY_DIR)/drevd ./cmd/drevd

run-server:
	go run ./cmd/drevd

dev:
	@./scripts/dev.sh start

dev-status:
	@./scripts/dev.sh status

dev-logs:
	@./scripts/dev.sh logs $(SERVICE)

dev-stop:
	@./scripts/dev.sh stop

dev-test:
	@./test/dev_workflow_test.sh

test:
	go test ./... -v -count=1

lint:
	go vet ./...

fmt:
	gofmt -w .

fmt-check:
	@files=$$(git ls-files '*.go' | xargs gofmt -l); \
	if [ -n "$$files" ]; then \
		echo "The following tracked Go source files are not formatted:"; \
		echo "$$files"; \
		exit 1; \
	fi

clean:
	rm -rf $(BINARY_DIR) logs/ drev.db

token:
	go run ./cmd/drev token generate

example:
	go run ./cmd/drevd &
	sleep 1
	go run ./cmd/drev run configs/example.drev.yml
