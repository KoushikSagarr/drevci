# Drev CI

[![Go](https://img.shields.io/badge/Go-00ADD8?style=for-the-badge&logo=go&logoColor=white)](https://go.dev/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Next.js](https://img.shields.io/badge/Next.js-000000?style=for-the-badge&logo=nextdotjs&logoColor=white)](https://nextjs.org/)
[![SQLite](https://img.shields.io/badge/SQLite-003B57?style=for-the-badge&logo=sqlite&logoColor=white)](https://www.sqlite.org/)

Drev CI is a high-performance, self-hosted CI/CD orchestration engine designed for local and private infrastructure. It enables automated pipeline execution within isolated Docker containers, featuring real-time log streaming and a unified web dashboard for pipeline management.

---

## Technical Features

### Containerized Execution
Isolated job execution using Docker. Every job runs in a clean container environment defined by your preferred image (Alpine, Ubuntu, Golang, etc.).

### DAG-Based Scheduling
A Directed Acyclic Graph (DAG) scheduler resolves job dependencies, prevents circular references, and optimizes execution order through parallelized job processing.

### Hybrid Workspace Management
Intelligent workspace initialization with dual-mode support:
- **Local Proxy Mode**: High-speed local disk synchronization for development and self-testing.
- **Repository Mode**: Automated Git cloning with optimized depth-1 fetches for standard pipelines.

### Live Telemetry and Logging
Real-time log streaming from Docker containers to the dashboard via Server-Sent Events (SSE).

### Secure Webhook Integration
Direct integration with GitHub via HMAC-SHA256 signed payloads, ensuring only authenticated push events trigger pipeline execution.

---

## System Architecture

The ecosystem consists of three primary components:

1. **Drev Daemon (`drevd`)**: The Go-based core engine responsible for job scheduling, container management, and log orchestration.
2. **Web Dashboard**: A modern Next.js interface for visualizing pipeline runs, inspecting logs, and managing deployment history.
3. **Internal Router**: A centralized proxy that handles API routing and ensures compatibility with external tunneling services like ngrok.

---

## Quick Start

### Prerequisites
- Go 1.21 or higher
- Docker Engine
- Node.js & npm (for the Dashboard)

### Initialization
The system includes a hardened startup script for environment preparation:

```powershell
./start-drev.ps1
```

This script handles:
- Port cleanup and process management.
- Backend compilation and execution.
- Frontend dependency verification and startup.
- Environment variable injection.

### Configuration
Pipelines are defined using the `.drev.yml` specification:

```yaml
name: example-pipeline
jobs:
  test:
    image: golang:1.23-alpine
    steps:
      - name: install-deps
        run: go mod download
      - name: run-tests
        run: go test ./...
```

---

## API Specification

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/api/v1/runs` | GET | List all pipeline executions |
| `/api/v1/runs/{id}` | GET | Retrieve detailed run state |
| `/api/v1/runs/{id}/logs` | GET | Stream real-time container logs |
| `/webhooks/github` | POST | Receive GitHub push event payloads |

---

## Roadmap

- [x] DAG Job Scheduling
- [x] Docker Container Orchestration
- [x] Live SSE Log Streaming
- [x] Web Dashboard Integration
- [x] Hybrid Workspace Initialization
- [ ] Distributed Runner Pools
- [ ] PostgreSQL Persistence Layer
- [ ] Multi-Cloud Provider Integration
- [ ] Notification Webhooks (Slack/Discord)
