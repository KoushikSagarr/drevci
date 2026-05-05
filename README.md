# Drev CI

A lightweight, self-hosted CI runner written in Go.

## Architecture

| Component | Path | Purpose |
|-----------|------|---------|
| `drev` | `cmd/drev/` | CLI client for triggering and inspecting runs |
| `drevd` | `cmd/drevd/` | Server daemon — API, scheduler, log streaming |
| `parser` | `internal/parser/` | `.drev.yml` config parser & validator |
| `scheduler` | `internal/scheduler/` | DAG-based job scheduler |
| `runner` | `internal/runner/` | Docker container executor |
| `streamer` | `internal/streamer/` | SSE-based real-time log streaming |
| `store` | `internal/store/` | SQLite persistence (no CGO) |
| `auth` | `internal/auth/` | JWT authentication |

## Quick Start

```bash
# Build both binaries
make build

# Run the server
make run-server

# Run tests
make test
```

## Configuration

Pipeline definitions live in `.drev.yml`. See [`configs/example.drev.yml`](configs/example.drev.yml) for a full example.

## Tech Stack

- **Go 1.22+** — core language
- **chi** — HTTP router
- **cobra** — CLI framework
- **Docker SDK** — container execution
- **SQLite** (modernc.org/sqlite) — persistence without CGO
- **JWT** (golang-jwt/jwt/v5) — authentication
- **SSE** — real-time log streaming

## License

MIT
