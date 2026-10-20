# Drev CI — Self-Hosted Production Deployment Guide

This guide covers deploying Drev CI as a self-hosted, single-host continuous integration system using Docker Compose.

---

## 1. Architecture Overview

Drev CI self-hosted consists of two tightly coupled services running on an isolated Docker bridge network (`drev-net`):

```
                       ┌─────────────────────────┐
                       │     Browser / Client    │
                       └────────────┬────────────┘
                                    │ HTTP :3000
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

---

## 2. Security Boundaries & Access Control

### Docker Socket Security & Host Isolation

> [!CAUTION]
> **Docker Socket Security Disclosure**
>
> Mounting `/var/run/docker.sock` into the `drevd` daemon gives the daemon process control over the host Docker daemon. In Docker's security model, access to the Docker socket is **effectively equivalent to root access on the host system**, as any process with socket access can create privileged containers, mount arbitrary host filesystems, or inspect running workloads.
>
> Dropping container capabilities or running as a non-root container user does **not** mitigate this underlying architectural reality: the Docker API itself is a privileged control plane.
>
> To maintain the highest achievable isolation:
> 1. The Docker socket is mounted **exclusively** into the `drevd` daemon.
> 2. The Next.js dashboard has **zero** access to the Docker socket and cannot communicate with Docker.
> 3. The daemon's API port (`9090`) is internal by default and only accessible to the dashboard and internal network. It is not published to the host interfaces by default.
> 4. Job containers execute with explicit resource limits (`NanoCPUs`, `Memory`, `MemorySwap`, and `PidsLimit`) to prevent denial-of-service against the host.

### Untrusted Code & Pipeline Workload Notice

> [!CAUTION]
> **Trusted Workloads Only — Untrusted Pull Request Risk**
>
> Pipeline jobs execute arbitrary shell commands inside container runtimes via the host Docker daemon. Because access to the Docker daemon grants effective host control:
> - **Restricted Workloads**: The current architecture is designed strictly for **trusted, single-tenant teams and private repositories**.
> - **Public PRs / Untrusted Code**: Do **NOT** allow unvetted public pull requests or untrusted third parties to trigger pipeline jobs without manual approval gates and external microVM isolation controls.
> - Multi-tenant isolation and secure execution of untrusted workloads require isolated microVM runtimes (such as Firecracker or gVisor), which are planned for future roadmap phases.

### Dashboard Authentication & Access Control (Phase 3.1)

The Next.js dashboard enforces a self-hosted authentication boundary around all operator views and upstream proxy requests.

#### Authentication Architecture

1. **Password Authentication & Stateless Sessions**:
   - Authentication is self-hosted with no external SaaS dependency (no Supabase, Auth0, or third-party OAuth required).
   - Configured via `DASHBOARD_PASSWORD` and an independent `DASHBOARD_SESSION_SECRET` (minimum 32 characters).
   - Tokens use a cryptographically secure HMAC-SHA256 signature with an expiration timestamp (`drev_session`).
   - Cookie attributes: `HttpOnly`, `SameSite=Lax`, `Path=/`, `Max-Age=86400` (24h). `Secure` flag is enforced in production HTTPS environments.

2. **Upstream Proxy Protection**:
   - The Next.js proxy (`/api/v1/*`) verifies the session cookie **before** injecting `DREV_TOKEN` or contacting the daemon.
   - Unauthenticated requests to `/api/v1/*` are rejected immediately with `401 Unauthorized` JSON. The daemon engine is never contacted for unauthenticated requests.
   - Unauthenticated page navigations (`/`, `/runs`, `/runs/[runId]`) redirect to `/login?from=...` with open-redirect protection.
   - `GET /api/v1/health` remains unauthenticated as a minimal public health check for Docker Compose container monitoring.
   - Wildcard CORS headers are disabled. State-changing requests (`POST`, `PUT`, `DELETE`) require Origin/Referer header validation to defend against CSRF.

3. **Rate Limiting & Fail-Closed Policy**:
   - Process-local login rate limiting enforces a maximum of 5 failed attempts per client per 5-minute window (`429 Too Many Requests`).
   - Timing-safe password comparison prevents timing side-channel attacks.
   - **Fail-Closed Configuration**: In production, missing or invalid authentication credentials (`DASHBOARD_PASSWORD` or `DASHBOARD_SESSION_SECRET`), secret lengths under 32 characters, or credential reuse (`DASHBOARD_PASSWORD == DREV_TOKEN`) cause the authentication service to fail closed (500 Internal Error) rather than running unauthenticated.

4. **Network Exposure Mitigation**:
   - By default, `docker-compose.yml` binds the dashboard to loopback (`127.0.0.1:3000`).
   - When exposed across external networks or public domains, use HTTPS termination (e.g., Caddy, Nginx, or Traefik) and ensure `DASHBOARD_COOKIE_SECURE=true` (or `NODE_ENV=production`).

---

## 3. Workspace Path Translation Mechanics

Drev CI uses Docker-out-of-Docker (DooD) to run job steps inside isolated containers.

1. When a pipeline executes, `drevd` creates an ephemeral workspace directory for the job checkout.
2. `drevd` instructs the host Docker engine to create a job container with a bind mount:
   ```text
   Binds: ["<workspace-dir>:/workspace"]
   ```
3. Because the host Docker daemon creates the container, `<workspace-dir>` must exist at the **exact same path on the host filesystem** as it does inside the `drevd` container.
4. In `docker-compose.yml`, this is resolved by mounting the shared directory at an identical absolute path:
   ```yaml
   volumes:
     - ${DREV_WORKSPACE_DIR:-/tmp/drev-workspaces}:${DREV_WORKSPACE_DIR:-/tmp/drev-workspaces}
   environment:
     - TMPDIR=${DREV_WORKSPACE_DIR:-/tmp/drev-workspaces}
   ```
5. When `drevd` creates temporary workspaces, Go's `os.MkdirTemp` places them under `TMPDIR` (e.g. `/tmp/drev-workspaces/drev-workspace-*`), which maps 1-to-1 to the host filesystem.

### Platform Limitations & Host Specifics

The DooD architecture relies on shared host paths and socket communication. Note the following operating-system and platform distinctions:

- **Verified Baseline**:
  - Verified on **macOS (Docker Desktop)**: Docker Desktop's VM abstraction automatically manages file sharing and ownership mapping between the host user and container root.
  - Standard rootful Docker Engine on Linux.

- **Linux UID/GID Workspace Ownership**:
  - `drevd` runs inside the container as `root` (UID 0: GID 0) and creates files in `/tmp/drev-workspaces` with root ownership.
  - Sibling step containers running as unprivileged users (e.g., `USER node` or UID 1000) may encounter write permission errors (`EACCES`) unless the step container runs as root or directory permissions are world-writable (`0777`).
  - Host users without `sudo` privileges cannot inspect or delete leftover directories under `/tmp/drev-workspaces`.

- **Rootless Docker on Linux**:
  - In rootless Docker mode, the Docker daemon socket is located in the user's runtime directory (typically `/run/user/<UID>/docker.sock` or `$XDG_RUNTIME_DIR/docker.sock`), **not** at `/var/run/docker.sock`.
  - To deploy on rootless Docker, you must override the socket volume mount in `docker-compose.yml` or configure `DOCKER_HOST=unix://${XDG_RUNTIME_DIR}/docker.sock`.

- **SELinux Enforcing Systems (RHEL, CentOS, Fedora, Rocky Linux)**:
  - Containers on SELinux-enforcing hosts require the `:z` (shared) or `:Z` (private) volume mount flag on host directory mounts to assign the required container storage label.
  - Without `:z` (e.g. `${DREV_WORKSPACE_DIR}:${DREV_WORKSPACE_DIR}:z`), access to host workspaces or the Docker socket will be denied by SELinux policy.

---

## 4. Quickstart Installation

### Prerequisites
- Docker Engine $\ge$ 24.0.0
- Docker Compose $\ge$ v2.20.0
- Git $\ge$ 2.30.0

### Step 1: Clone Repository
```bash
git clone https://github.com/drevci/drev.git
cd drev
```

### Step 2: Configure Environment
Copy the example environment template:
```bash
cp .env.example .env
```

Generate a secure 32-byte authentication token:
```bash
openssl rand -hex 32
```
Edit `.env` and set `DREV_TOKEN` to the generated value.

### Step 3: Prepare Shared Workspace Directory
Ensure the workspace directory exists and has appropriate permissions:
```bash
mkdir -p /tmp/drev-workspaces
```

### Step 4: Start the Stack
```bash
docker compose up -d --build
```

Check service status and health:
```bash
docker compose ps
```

Both `drev-engine` and `drev-dashboard` should report `healthy`.

---

## 5. Health Checks & Operational Verification

### Service Health vs. Job Execution
> [!NOTE]
> - **Service Health**: Probed by `GET /api/v1/health`. Confirms that the HTTP daemon and dashboard processes are alive, listening, and responsive.
> - **Job Execution**: Independent of service health. Pipeline job success depends on valid pipeline YAML definitions, reachable Docker images, repository access, and sufficient host resources. A healthy daemon can still report a failed pipeline if job steps exit with non-zero codes.

### Verifying Endpoints
- **Daemon Health**:
  ```bash
  docker compose exec drevd wget -qO- http://127.0.0.1:9090/api/v1/health
  ```
  Expected output:
  ```json
  {"notifications_enabled":false,"status":"ok","version":"0.1.0"}
  ```

- **Dashboard Health**:
  ```bash
  curl -s http://localhost:3000/api/v1/health
  ```

### Running a Test Pipeline
With the stack running, trigger a sample pipeline via the CLI:
```bash
export DREV_SERVER=http://localhost:3000
export DREV_TOKEN="<your-drev-token>"

# If port 9090 is internal, route requests through the dashboard proxy on port 3000:
./bin/drev run configs/example.drev.yml
```

Inspect active containers created by Drev CI:
```bash
docker ps --filter "label=ci.drev.managed=true"
```
During step execution, job containers appear with label `ci.drev.managed=true` and are automatically removed upon completion.

---

## 6. Persistence, Backups & Upgrades

### Persistent Volumes

Drev CI uses two named Docker volumes:
- `drev-data`: Mounts to `/var/lib/drev/data`. Contains `drev.db`, SQLite WAL files, and schema migrations.
- `drev-logs`: Mounts to `/var/lib/drev/logs`. Contains append-only text log streams for all historical runs.

#### Resolving Actual Volume Names
By default, Docker Compose prefixes volume names with the project name declared at the top of `docker-compose.yml` (`name: drevci`).
- When using the default project, the resolved volume names on your host are:
  - Data volume: `drevci_drev-data`
  - Logs volume: `drevci_drev-logs`
- If you run Compose with a custom project name (e.g. `docker compose -p myproject ...` or `COMPOSE_PROJECT_NAME=myproject`), the resolved volume names will be `${PROJECT_NAME}_drev-data` and `${PROJECT_NAME}_drev-logs`.

To inspect the exact resolved volume names for your active deployment:
```bash
docker compose config | grep -A5 '^volumes:'
```

---

### Backing Up Data

> [!WARNING]
> **Live SQLite WAL Copy Risk & Nonexistent Volume Traps**
>
> 1. **WAL Mode Inconsistency**: Drev CI runs SQLite in Write-Ahead Logging mode (`PRAGMA journal_mode=WAL`). Copying `drev.db` while `drevd` is running is **not** a verified safe backup method because active transactions reside in `drev.db-wal` and `drev.db-shm`.
> 2. **Silent Empty Volume Creation**: Docker's `docker run -v <vol>:/data ...` automatically creates a new empty volume if `<vol>` does not exist on the host or is mistyped. A `tar` archive created against a nonexistent volume will succeed with exit code 0 but archive an empty directory. **Never rely on the tar command's success as proof that the intended volume was backed up.**
> 3. **Archive Extraction vs. Integrity Validation**: Validating that an archive unpacks a non-empty database file confirms that file data was captured, but it is **not a substitute for SQLite database integrity validation or a full restore test**. A true restore check requires running `PRAGMA integrity_check;` on an isolated test volume.
> 4. **Hot Snapshot Tooling**: Online snapshotting (`sqlite3 .backup` or `VACUUM INTO`) is not supported out-of-the-box because the `sqlite3` CLI is omitted from the minimal production runtime container.

#### Verified Safe Offline Backup Procedure

This procedure stops the stack to flush SQLite WAL buffers, verifies volume existence before mounting, captures data and logs separately without overwriting existing archives, handles archiving or verification failures by attempting stack restart while preserving the original failure exit code, and validates the backup by extracting `drev.db` into a fresh temporary directory to verify non-zero size before restarting services.

```bash
# 1. Resolve active Compose project name and target volume names
PROJECT_NAME=$(docker compose config | grep -m1 '^name:' | awk '{print $2}')
if [ -z "${PROJECT_NAME}" ]; then
  echo "Error: Could not resolve Compose project name. Check docker-compose.yml or set PROJECT_NAME manually."
  exit 1
fi

DATA_VOL="${PROJECT_NAME}_drev-data"
LOGS_VOL="${PROJECT_NAME}_drev-logs"
TIMESTAMP=$(date +%Y%m%d%H%M%S)
BACKUP_DIR="./backups"

echo "Targeting project: ${PROJECT_NAME}"
echo "Data volume:       ${DATA_VOL}"
echo "Logs volume:       ${LOGS_VOL}"

# 2. Verify BOTH volumes exist on the Docker host BEFORE mounting or stopping
docker volume inspect "${DATA_VOL}" >/dev/null 2>&1 || {
  echo "Error: Volume '${DATA_VOL}' does not exist on this Docker host!"
  echo "Check 'docker volume ls' to confirm your active volume names."
  exit 1
}
docker volume inspect "${LOGS_VOL}" >/dev/null 2>&1 || {
  echo "Error: Volume '${LOGS_VOL}' does not exist on this Docker host!"
  echo "Check 'docker volume ls' to confirm your active volume names."
  exit 1
}

# 3. Stop containers to flush SQLite WAL buffers and freeze all database writes
docker compose -p "${PROJECT_NAME}" stop || {
  echo "Error: Failed to stop containers cleanly for project '${PROJECT_NAME}'!"
  exit 1
}

mkdir -p "${BACKUP_DIR}"
DATA_BACKUP="${BACKUP_DIR}/${PROJECT_NAME}-data-backup-${TIMESTAMP}.tar.gz"
LOGS_BACKUP="${BACKUP_DIR}/${PROJECT_NAME}-logs-backup-${TIMESTAMP}.tar.gz"

# Helper function to attempt restart on failure while preserving error status
handle_backup_failure() {
  local exit_code=$1
  local step_name=$2
  if [ -z "${exit_code}" ] || [ "${exit_code}" -eq 0 ]; then
    exit_code=1
  fi
  echo "Error: ${step_name} failed with exit code ${exit_code}!"
  echo "Attempting to restart services for project '${PROJECT_NAME}'..."
  if docker compose -p "${PROJECT_NAME}" start; then
    echo "Services restarted successfully after backup failure."
  else
    echo "CRITICAL: Services failed to restart! Inspect containers manually: docker compose -p '${PROJECT_NAME}' ps"
  fi
  exit "${exit_code}"
}

# 4. Archive SQLite data volume (read-only mount prevents accidental modification)
docker run --rm -v "${DATA_VOL}:/data:ro" -v "$(pwd)/${BACKUP_DIR}:/backup" alpine \
  tar czf "/backup/$(basename "${DATA_BACKUP}")" -C /data . || handle_backup_failure $? "Data volume archiving"

# 5. Archive logs volume
docker run --rm -v "${LOGS_VOL}:/logs:ro" -v "$(pwd)/${BACKUP_DIR}:/backup" alpine \
  tar czf "/backup/$(basename "${LOGS_BACKUP}")" -C /logs . || handle_backup_failure $? "Logs volume archiving"

# 6. Verification: Extract data archive into a fresh temporary directory to verify drev.db
TMP_VERIFY_DIR=$(mktemp -d "/tmp/drev-verify-${TIMESTAMP}-XXXXXX")

# Ensure temporary directory cleanup runs on exit or error
cleanup_verify() {
  rm -rf "${TMP_VERIFY_DIR}"
}
trap cleanup_verify EXIT

# Extract the complete data backup into the temporary directory
tar -xzf "${DATA_BACKUP}" -C "${TMP_VERIFY_DIR}" || {
  status=$?
  cleanup_verify
  trap - EXIT
  handle_backup_failure "${status}" "Backup archive extraction"
}

# Verify that the exact extracted drev.db file exists and is non-empty
if [ ! -f "${TMP_VERIFY_DIR}/drev.db" ]; then
  cleanup_verify
  trap - EXIT
  handle_backup_failure 1 "Backup verification (drev.db not found in extracted archive)"
fi

if [ ! -s "${TMP_VERIFY_DIR}/drev.db" ]; then
  cleanup_verify
  trap - EXIT
  handle_backup_failure 1 "Backup verification (extracted drev.db is empty)"
fi

cleanup_verify
trap - EXIT

# 7. Restart services once archiving and verification succeed
docker compose -p "${PROJECT_NAME}" start || {
  echo "CRITICAL: Services failed to restart after backup! Inspect containers manually."
  exit 1
}

echo "Backup successful and verified:"
echo "  Data archive: ${DATA_BACKUP}"
echo "  Logs archive: ${LOGS_BACKUP}"
```

> [!NOTE]
> **Restore Verification Disclaimer**: The extraction check above confirms that `drev.db` was captured and contains non-zero bytes. It does **not** evaluate database schema consistency or perform an end-to-end SQLite integrity check. In critical environments, test your recovery runbook by periodically unpacking backups into a disposable test volume and querying tables with `sqlite3` using `PRAGMA integrity_check;`.

---

### Upgrades & Volume Migration Guide

#### Upgrade Scenarios
- **Fresh Installations**: Require no migration. Starting the stack with `docker compose up -d` automatically creates `drevci_drev-data` and `drevci_drev-logs`.
- **Installations with Project-Scoped Volumes**: Already using `drevci_drev-data` and `drevci_drev-logs`. Standard image upgrades (`git pull && docker compose build --pull && docker compose up -d`) preserve data.
- **Existing Installations on Legacy Volume Names (`drev-data`, `drev-logs`)**:
  Early pre-release Compose manifests used explicit global names `drev-data` and `drev-logs`.
  > [!IMPORTANT]
  > Upgrading directly with `git pull && docker compose up -d` without migrating will cause Compose to create new, empty `drevci_*` volumes. The daemon will start with an empty database, making historical runs appear missing while the legacy data remains dormant on the host.

#### Migration Exclusivity & Safety Requirements
Before performing any volume migration:
1. **Exclusive Access**: Ensure no other deployment, daemon container, CI job, or external process is accessing the source (`drev-data`, `drev-logs`) or destination (`${DATA_VOL}`, `${LOGS_VOL}`) volumes.
2. **Non-Transactional Checks**: Volume inspection and container mounts are **not transactional**. Concurrent operations on the Docker host during migration can invalidate pre-checks and lead to corruption or partial copies.
3. **Equal Protection**: Both data and logs volumes must be inspected and protected identically before copying.
4. **No Merging or Deletions**: Never merge legacy data into an existing non-empty volume. Never delete legacy volumes as part of the migration script.

#### Option A: Safe Volume Migration Procedure (Recommended)

Follow this procedure to migrate existing data into project-scoped volumes. This procedure verifies source existence before mounting, guards **both** destination volumes against data overwrites, and preserves legacy volumes:

1. **Create a verified backup** of your legacy volumes following the offline backup procedure above, targeting `drev-data` and `drev-logs`. Confirm that the archive is valid and non-empty before proceeding.
2. **Stop the stack**:
   ```bash
   PROJECT_NAME=$(docker compose config | grep -m1 '^name:' | awk '{print $2}')
   if [ -z "${PROJECT_NAME}" ]; then
     echo "Error: Could not resolve Compose project name unambiguously. Aborting."
     exit 1
   fi
   docker compose -p "${PROJECT_NAME}" stop
   ```
3. **Resolve active project name and destination volume names**:
   ```bash
   DATA_VOL="${PROJECT_NAME}_drev-data"
   LOGS_VOL="${PROJECT_NAME}_drev-logs"
   ```
4. **Inspect legacy source volumes without auto-creating them**:
   Use `docker volume inspect` to confirm source volumes exist before any container mount:
   ```bash
   docker volume inspect drev-data >/dev/null 2>&1 || {
     echo "Error: Source volume 'drev-data' does not exist on this Docker host!"
     echo "Confirm your legacy volume names with 'docker volume ls'. Aborting."
     exit 1
   }
   docker volume inspect drev-logs >/dev/null 2>&1 || {
     echo "Error: Source volume 'drev-logs' does not exist on this Docker host!"
     echo "Confirm your legacy volume names with 'docker volume ls'. Aborting."
     exit 1
   }

   # Mount source read-only to verify drev.db is present and non-empty
   docker run --rm -v drev-data:/src:ro alpine ls -la /src | grep "drev.db" || {
     echo "Error: Source volume 'drev-data' exists but does not contain drev.db! Aborting."
     exit 1
   }
   ```
5. **Inspect BOTH destination volumes to prevent overwrites or data merging**:
   If either destination volume already exists on the host, verify that it is completely empty:
   ```bash
   if docker volume inspect "${DATA_VOL}" >/dev/null 2>&1; then
     DEST_DATA_COUNT=$(docker run --rm -v "${DATA_VOL}:/dst:ro" alpine ls -1A /dst | wc -l)
     if [ "${DEST_DATA_COUNT}" -gt 0 ]; then
       echo "STOP: Destination volume '${DATA_VOL}' already exists and contains data (${DEST_DATA_COUNT} items)!"
       echo "Do not overwrite or merge data automatically. Investigate or remove the destination volume manually if it was created by an unmigrated test startup."
       exit 1
     fi
   fi

   if docker volume inspect "${LOGS_VOL}" >/dev/null 2>&1; then
     DEST_LOGS_COUNT=$(docker run --rm -v "${LOGS_VOL}:/dst:ro" alpine ls -1A /dst | wc -l)
     if [ "${DEST_LOGS_COUNT}" -gt 0 ]; then
       echo "STOP: Destination volume '${LOGS_VOL}' already exists and contains data (${DEST_LOGS_COUNT} items)!"
       echo "Do not overwrite or merge data automatically. Investigate or remove the destination volume manually if it was created by an unmigrated test startup."
       exit 1
     fi
   fi
   ```
6. **Deliberately create destination volumes**:
   ```bash
   docker volume inspect "${DATA_VOL}" >/dev/null 2>&1 || docker volume create "${DATA_VOL}"
   docker volume inspect "${LOGS_VOL}" >/dev/null 2>&1 || docker volume create "${LOGS_VOL}"
   ```
7. **Copy data from legacy to project-scoped volumes**:
   ```bash
   # Copy SQLite database files, WAL state, and permissions from read-only source
   docker run --rm -v drev-data:/src:ro -v "${DATA_VOL}:/dst" alpine cp -a /src/. /dst/

   # Copy historical logs
   docker run --rm -v drev-logs:/src:ro -v "${LOGS_VOL}:/dst" alpine cp -a /src/. /dst/
   ```
8. **Verify destination contents before starting services**:
   ```bash
   docker run --rm -v "${DATA_VOL}:/dst:ro" alpine ls -la /dst | grep "drev.db" || {
     echo "Error: drev.db was not found in destination volume '${DATA_VOL}'! Aborting."
     exit 1
   }
   ```
9. **Start the upgraded stack**:
   ```bash
   docker compose -p "${PROJECT_NAME}" up -d --build
   ```
10. **Validate pipeline history**:
    Open the dashboard at `http://localhost:3000` (or run `curl -s http://127.0.0.1:3000/api/v1/runs`) to confirm historical runs and jobs are intact.
11. **Preserve legacy volumes**: Do **NOT** delete the legacy `drev-data` and `drev-logs` volumes until you have verified ongoing execution on the upgraded deployment.

#### Option B: Retaining Legacy Volume Names via Override

If you prefer not to copy volume data, you can instruct Compose to retain the legacy global volume names by creating a local `docker-compose.override.yml`:

```yaml
# docker-compose.override.yml
volumes:
  drev-data:
    name: drev-data
  drev-logs:
    name: drev-logs
```

> [!CAUTION]
> Using explicit `name:` attributes retains globally shared volume names on the host. This disables Compose project-level isolation: running `docker compose down -v` or deploying secondary stacks (`-p <name>`) sharing this configuration will target or delete these same shared volumes.

---

### Migration & Restore Limitations

> [!NOTE]
> - **Schema Migrations**: The internal SQLite store (`internal/store/store.go`) automatically applies additive migrations (`ALTER TABLE ADD COLUMN`) upon startup. Non-additive schema changes (e.g. column drops, table renames, type alters) are **not** supported by the automatic migration engine and have not been tested.
> - **Restore Automation**: Live snapshot restore procedures have not been automated in CI. Always verify data integrity from backups before deploying breaking schema releases.
> - **Crash Recovery**: Interrupted-run recovery (`ResetGhostRuns`) automatically marks runs interrupted during upgrades as failed upon startup.

---

## 7. Clean Shutdown

To stop the stack cleanly:
```bash
docker compose down
```
To remove all volumes and reset data (destructive):
```bash
docker compose down -v
rm -rf /tmp/drev-workspaces/*
```
