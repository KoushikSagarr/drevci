# Changelog

All notable changes to Drev CI are documented here.

## [0.1.0] - 2026-05-06

### Added

- **Core engine**: YAML pipeline parser with DAG validation and circular dependency detection
- **Docker runner**: Execute jobs in isolated containers with real-time log streaming
- **Git integration**: Auto-clone repositories on push, support for branch/tag/commit refs
- **GitHub webhooks**: HMAC-SHA256 signed webhook receiver for automatic triggers
- **REST API**: Token-authenticated endpoints for pipeline management and run history
- **Web dashboard**: Next.js UI for viewing runs, jobs, and streaming logs
- **Worker pools**: Configurable concurrent workers with job queuing
- **Job parallelism**: DAG-based scheduler runs independent jobs concurrently
- **CLI**: `drev run`, `drev status`, `drev logs` with live log tailing
- **Persistence**: SQLite database with WAL mode for concurrent access
- **Notifications**: Slack and Discord webhook alerts on pipeline completion
- **Log streaming**: Server-Sent Events (SSE) for real-time logs in dashboard and CLI

### Technical Details

- Language: Go 1.23+
- Frontend: Next.js 14 with TypeScript and Tailwind
- Database: SQLite (supports PostgreSQL swap-in)
- Container runtime: Docker
- VCS: Git

### Known Limitations

- Single-machine runner pools (distributed runners in v0.2)
- No pipeline caching yet
- Dashboard does not support manual cancel/restart
- No Prometheus metrics

## Future Releases

### v0.2.0 (Planned)

- Distributed runner agents
- Pipeline caching with shared volumes
- PostgreSQL persistence layer
- Manual trigger/cancel from dashboard
- Prometheus metrics export

### v1.0.0 (Roadmap)

- Multi-workspace/team support
- RBAC (role-based access control)
- Enterprise features (audit logs, compliance)
- Cloud provider integrations
