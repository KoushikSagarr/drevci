# Contributing to Drev CI

Thanks for your interest in Drev CI! We welcome issues and pull requests.

## Development Setup

### Prerequisites

- Go 1.23+
- Docker Desktop
- Node.js 18+ (for dashboard)
- git

### Build from source

```bash
git clone https://github.com/drevci/drev.git
cd drev
go mod download

# Build binaries
go build -o bin/drev ./cmd/drev
go build -o bin/drevd ./cmd/drevd
```

### Run tests

```bash
go test ./... -v -count=1
```

Docker integration tests require Docker running. Tests skip gracefully if unavailable.

### Start development servers

**Terminal 1** (backend):
```bash
./bin/drevd --port 9090
```

**Terminal 2** (dashboard):
```bash
cd dashboard
npm install
npm run dev
# opens http://localhost:3000
```

**Terminal 3** (CLI):
```bash
export DREV_SERVER=http://localhost:9090
export DREV_TOKEN=<token-from-drevd-output>
./bin/drev run configs/example.drev.yml
```

## Code Organization

```
cmd/               Binary entrypoints (drev, drevd)
internal/
  api/             HTTP handlers and routing
  auth/            Token authentication
  notify/          Slack/Discord notifications
  parser/          YAML pipeline parser
  pool/            Worker pool and job queue
  runner/          Docker executor
  scheduler/       DAG job scheduler
  store/           SQLite persistence
  streamer/        Log streaming
  webhook/         GitHub webhook receiver
  workspace/       Git clone management
pkg/drevtypes/     Shared type definitions
dashboard/         Next.js web UI
configs/           Example pipelines and webhooks
```

## Before You Submit

- Run tests: `go test ./... -v`
- Run linter: `go vet ./...`
- Format code: `go fmt ./...`
- Test with real Docker: `go build && ./bin/drevd`
- Try the dashboard: `cd dashboard && npm run dev`

## Commit Messages

Use imperative mood: "add feature" not "added feature"

- First line: summary (50 chars or less)
- Body: explain what and why, not how

Example:

```
Add Slack notifications on pipeline completion

When a pipeline finishes, notify configured Slack
webhook with run status, duration, and dashboard link.
```

## Issues

- **Bug reports**: Include reproduction steps, Go/Docker versions, and error logs
- **Feature requests**: Explain the use case and how it helps
- **Questions**: Check existing issues first

## Questions?

Open an issue or discussion on GitHub. We're here to help!
