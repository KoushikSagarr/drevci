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

### 1. Production Self-Hosted Deployment (Docker Compose)

The recommended way to deploy Drev CI in production is using Docker Compose:

```bash
# 1. Clone the repository
git clone https://github.com/drevci/drev.git
cd drev

# 2. Configure environment
cp .env.example .env
# Generate and set a secure 32-byte hex token in .env:
#   openssl rand -hex 32

# 3. Create host workspace directory
mkdir -p /tmp/drev-workspaces

# 4. Start the stack
docker compose up -d --build
```

- **Dashboard**: Open `http://localhost:3000` (bound to `127.0.0.1` by default).
- **Persistent Data**: SQLite database state and job logs are stored in project-scoped Docker volumes (`drevci_drev-data` and `drevci_drev-logs`).
- **Production Operations Guide**: For full architecture details, Docker socket security disclosures, verified safe offline backup procedures, and volume migration guides, see [docs/self-hosted.md](docs/self-hosted.md).

---

### 2. Local Development Stack (macOS / Linux)

Run the local development stack (daemon, dashboard, and ngrok tunnel) natively with a single command:

```bash
make dev          # Starts drevd (9090), dashboard (3000), and ngrok with health checks
make dev-status   # Shows process status, PIDs, health checks, and active tunnel URLs
make dev-logs     # Follows consolidated service logs (or SERVICE=drevd/dashboard/ngrok)
make dev-stop     # Gracefully shuts down only the managed processes
```

To use a stable reserved domain for GitHub webhooks, set `NGROK_DOMAIN` in `.env`:
```bash
NGROK_DOMAIN=picked-indirectly-cheetah.ngrok-free.app
```
When configured, ngrok establishes a verified stable tunnel:
- **Webhook Payload URL**: `https://picked-indirectly-cheetah.ngrok-free.app/webhooks/github`
- **Secret**: Must match `DREV_WEBHOOK_SECRET` in your `.env`

---

### 3. Manual Build & Run

```bash
git clone https://github.com/drevci/drev.git
cd drev
go mod download
go build -o bin/drev ./cmd/drev
go build -o bin/drevd ./cmd/drevd

# Start the server daemon
./bin/drevd --port 9090
```

### Run your first pipeline

```bash
export DREV_SERVER=http://localhost:3000  # or http://localhost:9090 if daemon port is published
export DREV_TOKEN="<your-drev-token>"

./bin/drev run configs/example.drev.yml
```

Watch logs stream in real time:
```text
✓ Pipeline succeeded
```

View the execution live in the web dashboard at `http://localhost:3000/runs`.

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

1. Start Drev CI server and expose it (e.g., via ngrok):
   ```bash
   ngrok http 9090
   ```

2. Go to your GitHub repo → Settings → Webhooks → Add webhook:
   - Payload URL: `https://<your-ngrok-url>/webhooks/github`
   - Content type: `application/json`
   - Secret: (copy from drevd startup output or `.env`)
   - Events: Just the push event
   - Click Add webhook

3. Push to your repo — pipeline triggers automatically.

## Slack/Discord Notifications

Set webhook URLs in `.env` or flags to get notified when pipelines complete:

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

Drev CI uses Docker-out-of-Docker (DooD) to execute jobs in ephemeral sibling containers managed directly by the host Docker engine:

```text
                       ┌─────────────────────────┐
                       │     Browser / Client    │
                       └────────────┬────────────┘
                                    │ HTTP :3000 (127.0.0.1)
                                    ▼
       ┌────────────────────────────────────────────────────────┐
       │             Next.js Dashboard Container                │
       │             - Unprivileged non-root user (nextjs)      │
       │             - Exposes port 3000 to host                │
       │             - NO Docker socket access                  │
       └────────────────────────────┬───────────────────────────┘
                                    │ HTTP :9090 (Internal network: drev-net)
                                    │ Authorization: Bearer <DREV_TOKEN>
                                    ▼
       ┌────────────────────────────────────────────────────────┐
       │               Drev CI Daemon (`drevd`)                 │
       │             - Persistent volume: SQLite database       │
       │             - Persistent volume: Log streams           │
       │             - Shared mount: /tmp/drev-workspaces       │
       └────────────────────────────┬───────────────────────────┘
                                    │
                                    │ Unix Socket: /var/run/docker.sock
                                    ▼
       ┌────────────────────────────────────────────────────────┐
       │                   Host Docker Engine                   │
       │             - Executes ephemeral CI job containers     │
       │             - Enforces CPU, Memory, Swap, & PIDs limits│
       │             - Labels: ci.drev.managed=true             │
       └────────────────────────────────────────────────────────┘
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

**Environment variables (`.env`):**

| Variable | Default | Description |
|:---------|:--------|:------------|
| `DREV_TOKEN` | — | API bearer token (required for engine and dashboard) |
| `DASHBOARD_BIND_IP` | `127.0.0.1` | Host interface bind address for the web dashboard |
| `DASHBOARD_PORT` | `3000` | Port for the Next.js web dashboard |
| `DREV_PORT` | `9090` | Daemon port (internal by default in Docker Compose) |
| `DREV_WORKSPACE_DIR` | `/tmp/drev-workspaces` | Shared host path for DooD job checkouts |
| `DREV_JOB_CPU_LIMIT` | — | Maximum CPUs per job container (e.g. `2.0` or `1.5`) |
| `DREV_JOB_MEMORY_LIMIT`| — | Maximum RAM per job container (e.g. `2GB` or `512MB`) |
| `DREV_JOB_SWAP_LIMIT` | — | Maximum swap per job container (e.g. `2GB` or `0`) |
| `DREV_JOB_PIDS_LIMIT` | — | Maximum processes/threads per job container (e.g. `512`) |
| `DREV_WEBHOOK_SECRET`| — | GitHub webhook HMAC validation secret |
| `NGROK_DOMAIN` | — | Reserved ngrok domain (e.g. `my-tunnel.ngrok-free.app`) |
| `DREV_SLACK_WEBHOOK` | — | Slack incoming webhook URL for notifications |
| `DREV_DISCORD_WEBHOOK`| — | Discord webhook URL for notifications |

## Testing

```bash
# Run unit and race tests
go test -race -count=1 ./...

# Run dashboard tests and linter
npm --prefix dashboard test
npm --prefix dashboard run lint
```

## Seeding Sample Data

To populate your SQLite database with sample pipeline runs for testing and UI evaluation:

```bash
go build -o bin/seed ./cmd/seed
./bin/seed --db-type sqlite --db-path ./drev.db
```

## Status & Roadmap

### Completed
- ✅ **Core Engine** — YAML parser, DAG scheduler, and Docker DooD runner
- ✅ **Docker Compose Deployment** — Isolated container stack, non-root dashboard, project-scoped volumes
- ✅ **Host DooD Alignment** — Coordinated workspace path mapping and exclusive Docker socket confinement
- ✅ **Resource Limits** — Enforced CPU, Memory, Swap, and PIDs limits per step container
- ✅ **Persistence & Crash Recovery** — SQLite WAL mode, ghost-run recovery, and orphan container reconciliation
- ✅ **Operational Runbooks** — Verified offline backup procedures and zero-data-loss volume migrations
- ✅ **Real-Time Streaming** — SSE log streaming to CLI and dashboard
- ✅ **Webhooks & Alerts** — GitHub push triggers with HMAC verification; Slack & Discord notifications

### Planned (Phase 3 & Beyond)
- [ ] **Dashboard Access Control** — Incoming user authentication & session management
- [ ] **Enhanced UI Experience** — Multi-pipeline filtering, run search, and job timeline navigation
- [ ] **Artifact Caching** — Shared build cache volumes and artifact persistence
- [ ] **MicroVM Isolation** — Firecracker / gVisor runners for untrusted public PR execution

## Contributing

Issues and PRs welcome. Please file an issue first to discuss major changes. See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup.

## License

MIT License — see [LICENSE](LICENSE) file.

## Author

Built by the Drev team. Questions? [Open an issue](https://github.com/drevci/drev/issues) on GitHub.
