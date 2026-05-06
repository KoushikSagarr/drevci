# Drev CI

⚡ Lightweight self-hosted CI runner. Define pipelines in `.drev.yml`, trigger via git push or CLI, watch logs stream live.

## Features

- **YAML pipelines** — Simple `.drev.yml` syntax with multi-job DAG support
- **Docker execution** — Isolated containers, any image
- **Git integration** — Auto-clone repos on push, GitHub webhook triggers
- **Real-time logs** — Stream logs live via SSE to CLI or web dashboard
- **Job parallelism** — Run independent jobs concurrently
- **Worker pools** — Configurable workers, queued jobs
- **Web dashboard** — View runs, logs, job status in real time
- **Notifications** — Slack and Discord alerts on pipeline completion
- **Zero setup** — Single binary, SQLite database, no external services

## Quick Start

### Install

```bash
git clone https://github.com/drevci/drev.git
cd drev
go mod download
go build -o bin/drev ./cmd/drev
go build -o bin/drevd ./cmd/drevd
```

### Start the server

```bash
./bin/drevd --port 9090
# outputs: Webhook URL and API token
```

### Run your first pipeline

```bash
export DREV_SERVER=http://localhost:9090
export DREV_TOKEN=<token-from-startup>

./bin/drev run configs/example.drev.yml
```

Watch logs stream in real time. When done:
```
✓ Pipeline succeeded
```

### View in dashboard

Open `http://localhost:3000/runs` to see all pipeline runs.

## Pipeline Syntax

Create `.drev.yml` in your repo:

```yaml
name: my-pipeline
source:
  type: git
  url: https://github.com/myorg/myrepo.git
  ref: main
jobs:
  - name: test
    image: golang:1.23-alpine
    steps:
      - name: run tests
        run: go test ./...

  - name: build
    image: golang:1.23-alpine
    depends_on: [test]
    steps:
      - name: build binary
        run: go build -o app ./cmd/app
```

### Pipeline Schema

```yaml
name: string                    # required
source:
  type: git
  url: string                   # git clone URL
  ref: string                   # branch, tag, or commit
triggers:
  - push                        # GitHub push event
  - pull_request
env:
  KEY: VALUE                    # top-level env vars
jobs:
  - name: string                # unique job name
    image: string               # Docker image
    env:
      KEY: VALUE                # job-level env vars
    depends_on: [job1, job2]    # DAG dependencies
    steps:
      - name: string
        run: string             # shell command
```

## GitHub Webhook Setup

1. Start Drev CI server and expose it (e.g., ngrok):
   ```bash
   ngrok http 9090
   ```

2. Go to your GitHub repo → Settings → Webhooks → Add webhook:
   - Payload URL: `https://<your-ngrok-url>/webhooks/github`
   - Content type: `application/json`
   - Secret: (copy from drevd startup output)
   - Events: Just the push event
   - Click Add webhook

3. Push to your repo — pipeline triggers automatically.

## Slack/Discord Notifications

Set webhook URLs to get notified when pipelines complete:

```bash
./bin/drevd \
  --port 9090 \
  --slack-webhook https://hooks.slack.com/... \
  --discord-webhook https://discordapp.com/api/...
```

Messages include run status, duration, and a link to the dashboard.

## CLI Commands

```bash
# Run a pipeline
drev run <.drev.yml path>

# Check status
drev status <run-id>

# Stream logs
drev logs <run-id> --follow

# Generate API token
drev token generate

# Show version
drev version
```

## Architecture

```
┌──────────────────────────────────────┐
│         GitHub / CLI User            │
└──────────────────────────────────────┘
                    ↓
┌──────────────────────────────────────┐
│    Drev CI API (drevd :9090)         │
│  - REST API, auth, webhooks          │
│  - Job queue & worker pool           │
│  - YAML parser & DAG scheduler       │
└──────────────────────────────────────┘
                    ↓
┌──────────────────────────────────────┐
│  Docker Engine                       │
│  - Execute jobs in containers        │
│  - Stream logs                       │
└──────────────────────────────────────┘
                    ↓
┌──────────────────────────────────────┐
│  SQLite + Log Files                  │
│  - Persist runs and history          │
└──────────────────────────────────────┘

Dashboard: http://localhost:3000
  - View all runs
  - Stream logs in real time
  - Job timeline
```

## Configuration

**Server flags:**

| Flag | Default | Description |
|:-----|:--------|:------------|
| `--port` | `9090` | HTTP port |
| `--db` | `./drev.db` | SQLite database path |
| `--log-dir` | `./logs` | Log file directory |
| `--token` | *(auto-gen)* | API token |
| `--workers` | `3` | Concurrent workers |
| `--queue-size` | `100` | Max queued pipelines |
| `--webhook-secret` | — | GitHub webhook HMAC secret |
| `--webhook-config` | `./configs/webhooks` | Per-repo pipeline configs |
| `--slack-webhook` | — | Slack webhook URL |
| `--discord-webhook` | — | Discord webhook URL |

**Environment variables:**

| Variable | Description |
|:---------|:------------|
| `DREV_SERVER` | CLI server URL (default `localhost:9090`) |
| `DREV_TOKEN` | API token |
| `DREV_WEBHOOK_SECRET` | GitHub webhook secret |
| `DREV_SLACK_WEBHOOK` | Slack webhook URL |
| `DREV_DISCORD_WEBHOOK` | Discord webhook URL |

## Testing

```bash
go test ./... -v
```

Run with Docker running for integration tests (runner tests skip gracefully if Docker unavailable).

## Status

- ✅ Core engine (YAML parser, DAG scheduler, Docker runner)
- ✅ Real-time log streaming (CLI + web dashboard)
- ✅ GitHub webhook integration
- ✅ REST API with token auth
- ✅ Job queue and worker pools
- ✅ SQLite persistence
- ✅ Slack/Discord notifications

## Roadmap

- [ ] Distributed runners (agents on multiple machines)
- [ ] PostgreSQL support
- [ ] Pipeline caching (shared volumes)
- [ ] Manual trigger/cancel from dashboard
- [ ] Metrics and observability (Prometheus)
- [ ] Multi-workspace support

## Contributing

Issues and PRs welcome. Please file an issue first to discuss major changes. See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup.

## License

MIT License — see [LICENSE](LICENSE) file.

## Author

Built by the Drev team. Questions? [Open an issue](https://github.com/drevci/drev/issues) on GitHub.
