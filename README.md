# Drev CI

Lightweight self-hosted CI runner. Define pipelines in 
.drev.yml, run them in Docker containers.

## Quick start

    # Start the server
    drevd --port 8080

    # In another terminal, trigger a pipeline
    drev run configs/example.drev.yml

    # Stream logs of a previous run
    drev logs <run-id> --follow

    # Check run status
    drev status <run-id>

## Pipeline syntax (full example)
```yaml
name: drev-ci-pipeline

triggers:
  - push
  - pull_request

env:
  GO_ENV: production
  APP_NAME: drev

jobs:
  - name: test
    image: golang:1.22-alpine
    steps:
      - name: checkout deps
        run: go mod download

      - name: run tests
        run: go test -race -coverprofile=coverage.out ./...

      - name: upload coverage
        run: |
          apk add --no-cache curl
          curl -sf https://codecov.io/bash | sh -s -- -f coverage.out

  - name: build
    image: golang:1.22-alpine
    depends_on:
      - test
    steps:
      - name: build binary
        run: |
          CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/drev ./cmd/drev
          CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/drevd ./cmd/drevd

      - name: print binary size
        run: ls -lh bin/
```

## API reference
- `POST /api/v1/pipelines/trigger`
  Trigger a new pipeline run.
  `curl -X POST -H "Authorization: Bearer <token>" -H "Content-Type: application/json" -d '{"pipeline_path":"configs/example.drev.yml"}' http://localhost:8080/api/v1/pipelines/trigger`
- `GET /api/v1/runs`
  List all runs.
  `curl -H "Authorization: Bearer <token>" http://localhost:8080/api/v1/runs`
- `GET /api/v1/runs/{runID}`
  Get details for a single run.
  `curl -H "Authorization: Bearer <token>" http://localhost:8080/api/v1/runs/{runID}`
- `GET /api/v1/runs/{runID}/jobs`
  List jobs for a specific run.
  `curl -H "Authorization: Bearer <token>" http://localhost:8080/api/v1/runs/{runID}/jobs`
- `GET /api/v1/runs/{runID}/logs`
  Stream logs via Server-Sent Events (SSE).
  `curl -N -H "Authorization: Bearer <token>" http://localhost:8080/api/v1/runs/{runID}/logs`
- `GET /api/v1/health`
  Health check endpoint (no auth required).
  `curl http://localhost:8080/api/v1/health`

## Roadmap
  - [ ] Git repository cloning
  - [ ] GitHub webhook integration  
  - [ ] Runner pools (multiple agents)
  - [ ] Web dashboard
  - [ ] Postgres support
  - [ ] Docker build & push steps
  - [ ] Slack / Discord notifications
  - [ ] RBAC and multi-user support
test
test
