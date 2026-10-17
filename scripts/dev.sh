#!/usr/bin/env bash
# ==============================================================================
# Drev CI — Local Development Stack Supervisor (macOS / Linux)
#
# Manages the local dev stack:
#   - drevd:     Go daemon backend (API port 9090)
#   - dashboard: Next.js frontend (port 3000)
#   - ngrok:     Public tunnel forwarding to daemon port 9090 for webhooks
#
# Usage:
#   ./scripts/dev.sh start   (or 'make dev')
#   ./scripts/dev.sh status  (or 'make dev-status')
#   ./scripts/dev.sh logs    (or 'make dev-logs [SERVICE=drevd|dashboard|ngrok]')
#   ./scripts/dev.sh stop    (or 'make dev-stop')
# ==============================================================================

set -euo pipefail

# Determine repository root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Configuration & Directories
DEV_DIR="${DREV_DEV_DIR:-$REPO_ROOT/logs/dev}"
LOG_DIR="${DREV_LOG_DIR:-$REPO_ROOT/logs}"
DB_PATH="${DREV_DB_PATH:-$REPO_ROOT/drev.db}"

DREVD_PID_FILE="$DEV_DIR/drevd.pid"
DASHBOARD_PID_FILE="$DEV_DIR/dashboard.pid"
NGROK_PID_FILE="$DEV_DIR/ngrok.pid"

DREVD_LOG_FILE="$DEV_DIR/drevd.log"
DASHBOARD_LOG_FILE="$DEV_DIR/dashboard.log"
NGROK_LOG_FILE="$DEV_DIR/ngrok.log"

# Default ports
DREV_PORT="${DREV_PORT:-9090}"
DASHBOARD_PORT="${DASHBOARD_PORT:-3000}"
NGROK_INSPECT_PORT="${NGROK_INSPECT_PORT:-4040}"

# Colors (only if terminal attached)
if [ -t 1 ]; then
    C_RESET="\033[0m"
    C_RED="\033[31m"
    C_GREEN="\033[32m"
    C_YELLOW="\033[33m"
    C_BLUE="\033[34m"
    C_CYAN="\033[36m"
    C_BOLD="\033[1m"
else
    C_RESET=""
    C_RED=""
    C_GREEN=""
    C_YELLOW=""
    C_BLUE=""
    C_CYAN=""
    C_BOLD=""
fi

log_info()  { printf "%b[INFO]%b  %s\n" "$C_BLUE" "$C_RESET" "$*"; }
log_ok()    { printf "%b[OK]%b    %s\n" "$C_GREEN" "$C_RESET" "$*"; }
log_warn()  { printf "%b[WARN]%b  %s\n" "$C_YELLOW" "$C_RESET" "$*"; }
log_error() { printf "%b[ERROR]%b %s\n" "$C_RED" "$C_RESET" "$*" >&2; }

# ------------------------------------------------------------------------------
# 1. Environment Loading (Secure, no echoing secrets)
# ------------------------------------------------------------------------------
load_env() {
    local env_file="$REPO_ROOT/.env"
    if [ -f "$env_file" ]; then
        while IFS='=' read -r key val || [ -n "$key" ]; do
            # Skip empty lines and comments
            case "$key" in
                \#*|"") continue ;;
            esac
            key="$(echo "$key" | tr -d '[:space:]')"
            if [ -z "$key" ]; then continue; fi
            # Trim whitespace and quotes
            val="$(echo "$val" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' -e 's/^["'"'"']//' -e 's/["'"'"']$//')"
            # Only export if not already set in the current environment
            if [ -z "$(eval "printf '%s' \"\${$key:-}\"")" ]; then
                export "$key=$val"
            fi
        done < "$env_file"
    fi

    # Read NGROK_DOMAIN if provided
    NGROK_DOMAIN="${NGROK_DOMAIN:-${DREV_NGROK_DOMAIN:-}}"
}

# ------------------------------------------------------------------------------
# 2. Process & Port Ownership Helpers
# ------------------------------------------------------------------------------
get_port_listener() {
    local port="$1"
    # Returns: "<PID> <COMMAND>" or empty if no listener
    lsof -nP -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | awk 'NR>1 {print $2, $1; exit}'
}

get_descendant_pids() {
    local parent_pid="$1"
    local children
    children=$(pgrep -P "$parent_pid" 2>/dev/null || true)
    for child in $children; do
        get_descendant_pids "$child"
        echo "$child"
    done
}

is_pid_or_descendant() {
    local target_pid="$1"
    local candidate_pid="$2"

    if [ "$target_pid" = "$candidate_pid" ]; then
        return 0
    fi

    local descendants
    descendants=$(get_descendant_pids "$target_pid")
    for d in $descendants; do
        if [ "$d" = "$candidate_pid" ]; then
            return 0
        fi
    done
    return 1
}

# Verifies that a PID file exists, points to a live process, and command matches pattern
is_service_running() {
    local pid_file="$1"
    local expected_pattern="$2"

    if [ ! -f "$pid_file" ]; then
        return 1
    fi

    local pid
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [ -z "$pid" ]; then
        rm -f "$pid_file"
        return 1
    fi

    if ! kill -0 "$pid" 2>/dev/null; then
        # Process is dead, clean up stale PID file
        rm -f "$pid_file"
        return 1
    fi

    # Check process command to guard against PID recycling by OS
    local cmd
    cmd="$(ps -p "$pid" -o command= 2>/dev/null || true)"
    if echo "$cmd" | grep -E -q "$expected_pattern"; then
        return 0
    else
        # Process is alive but command does not match -> stale recycled PID
        rm -f "$pid_file"
        return 1
    fi
}

# Gracefully stops a service by PID file, including descendants
stop_service() {
    local name="$1"
    local pid_file="$2"
    local expected_pattern="$3"

    if [ ! -f "$pid_file" ]; then
        return 0
    fi

    local pid
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [ -z "$pid" ]; then
        rm -f "$pid_file"
        return 0
    fi

    if ! kill -0 "$pid" 2>/dev/null; then
        rm -f "$pid_file"
        return 0
    fi

    # Verify command ownership before terminating
    local cmd
    cmd="$(ps -p "$pid" -o command= 2>/dev/null || true)"
    if ! echo "$cmd" | grep -E -q "$expected_pattern"; then
        log_warn "PID $pid in $pid_file does not match '$name' (found '$cmd'). Removing stale PID file without killing."
        rm -f "$pid_file"
        return 0
    fi

    printf "Stopping %s (PID %s)... " "$name" "$pid"

    # Find descendant processes (e.g. Node child of npm/next)
    local descendants
    descendants=$(get_descendant_pids "$pid")

    # Send SIGTERM
    for child in $descendants; do
        kill -TERM "$child" 2>/dev/null || true
    done
    kill -TERM "$pid" 2>/dev/null || true

    # Wait for graceful shutdown (up to 12s, aligned with daemon 10s shutdown budget)
    local count=0
    local max_wait=120
    while kill -0 "$pid" 2>/dev/null && [ "$count" -lt "$max_wait" ]; do
        sleep 0.1
        count=$((count + 1))
    done

    # Force SIGKILL if still running after budget
    if kill -0 "$pid" 2>/dev/null; then
        for child in $descendants; do
            kill -KILL "$child" 2>/dev/null || true
        done
        kill -KILL "$pid" 2>/dev/null || true
        printf "%s(force killed)%s\n" "$C_YELLOW" "$C_RESET"
    else
        printf "%sdone%s\n" "$C_GREEN" "$C_RESET"
    fi

    rm -f "$pid_file"
}

# ------------------------------------------------------------------------------
# 3. Pre-flight Checks (Dependencies, Docker, Ports)
# ------------------------------------------------------------------------------
check_docker() {
    if ! command -v docker >/dev/null 2>&1; then
        log_error "Docker CLI is not installed or not in PATH."
        log_error "Please install Docker Desktop: https://www.docker.com/products/docker-desktop/"
        return 1
    fi

    # On macOS, check if Docker Desktop socket exists and set DOCKER_HOST if needed
    if [ -z "${DOCKER_HOST:-}" ] && [ "$(uname -s)" = "Darwin" ]; then
        local user_sock="$HOME/.docker/run/docker.sock"
        if [ ! -S "/var/run/docker.sock" ] && [ -S "$user_sock" ]; then
            export DOCKER_HOST="unix://$user_sock"
        fi
    fi

    # Verify Docker engine is actually responsive
    if ! docker info >/dev/null 2>&1; then
        log_error "Docker daemon is unreachable. Pipelines requiring container execution will fail."
        log_error "Please ensure Docker Desktop is running."
        if [ "$(uname -s)" = "Darwin" ]; then
            log_error "Hint: If Docker Desktop is running, try: export DOCKER_HOST=\"unix://$HOME/.docker/run/docker.sock\""
        fi
        return 1
    fi

    log_ok "Docker daemon reachable ($(docker version --format '{{.Server.Version}}' 2>/dev/null || echo 'ready'))"
    return 0
}

check_prereqs() {
    log_info "Verifying prerequisites..."

    local missing=0

    if ! command -v go >/dev/null 2>&1; then
        log_error "Go is not installed or not in PATH."
        missing=1
    else
        log_ok "Go found ($(go version | awk '{print $3}'))"
    fi

    if ! command -v node >/dev/null 2>&1; then
        log_error "Node.js is not installed or not in PATH."
        missing=1
    else
        log_ok "Node.js found ($(node --version))"
    fi

    if ! command -v npm >/dev/null 2>&1; then
        log_error "npm is not installed or not in PATH."
        missing=1
    else
        log_ok "npm found ($(npm --version))"
    fi

    if [ ! -d "$REPO_ROOT/dashboard/node_modules" ]; then
        log_error "Dashboard dependencies missing. Please run: (cd dashboard && npm install)"
        missing=1
    else
        log_ok "Dashboard dependencies installed"
    fi

    if ! command -v ngrok >/dev/null 2>&1; then
        log_error "ngrok is not installed or not in PATH."
        log_error "Install ngrok: brew install ngrok (macOS) or visit https://ngrok.com/download"
        missing=1
    else
        log_ok "ngrok found ($(ngrok version | head -n1))"
    fi

    if ! check_docker; then
        missing=1
    fi

    if [ "$missing" -ne 0 ]; then
        log_error "Prerequisite check failed. Aborting startup."
        return 1
    fi

    return 0
}

# Checks if a port is occupied.
# Returns:
#   0: Port is free
#   2: Port is occupied by our own managed service (already running)
#   1: Port is occupied by an external/unmanaged process (conflict)
check_port() {
    local port="$1"
    local service_name="$2"
    local pid_file="$3"
    local expected_pattern="$4"

    local listener
    listener=$(get_port_listener "$port")
    if [ -z "$listener" ]; then
        return 0
    fi

    local occ_pid occ_cmd
    occ_pid=$(echo "$listener" | awk '{print $1}')
    occ_cmd=$(echo "$listener" | awk '{print $2}')

    # Check if this PID belongs to our managed service
    if [ -f "$pid_file" ]; then
        local recorded_pid
        recorded_pid="$(cat "$pid_file" 2>/dev/null || true)"
        if [ -n "$recorded_pid" ] && is_pid_or_descendant "$recorded_pid" "$occ_pid"; then
            return 2
        fi
    fi

    # Conflict with unmanaged process
    log_error "Port $port ($service_name) is already occupied by unmanaged process '$occ_cmd' (PID $occ_pid)."
    log_error "Please terminate the process on port $port or configure an alternate port before starting."
    return 1
}

# ------------------------------------------------------------------------------
# 4. Service Startup & Health Checks
# ------------------------------------------------------------------------------
build_drevd() {
    log_info "Compiling drevd binary..."
    mkdir -p "$REPO_ROOT/bin"
    if ! go build -o "$REPO_ROOT/bin/drevd" "$REPO_ROOT/cmd/drevd"; then
        log_error "Failed to build drevd daemon binary."
        return 1
    fi
    log_ok "drevd binary built successfully: $REPO_ROOT/bin/drevd"
}

wait_for_drevd() {
    local port="$1"
    local timeout=10
    local elapsed=0

    while [ "$elapsed" -lt "$timeout" ]; do
        if is_service_running "$DREVD_PID_FILE" "drevd"; then
            if curl -s -f "http://localhost:$port/api/v1/health" >/dev/null 2>&1; then
                return 0
            fi
        else
            log_error "drevd daemon process terminated unexpectedly during startup."
            if [ -f "$DREVD_LOG_FILE" ]; then
                log_error "Recent logs from $DREVD_LOG_FILE:"
                tail -n 20 "$DREVD_LOG_FILE" >&2
            fi
            return 1
        fi
        sleep 0.5
        elapsed=$((elapsed + 1))
    done

    log_error "Timed out waiting for drevd health endpoint (http://localhost:$port/api/v1/health)."
    if [ -f "$DREVD_LOG_FILE" ]; then
        tail -n 20 "$DREVD_LOG_FILE" >&2
    fi
    return 1
}

wait_for_dashboard() {
    local port="$1"
    local timeout=25
    local elapsed=0

    while [ "$elapsed" -lt "$timeout" ]; do
        if is_service_running "$DASHBOARD_PID_FILE" "next|node|npm"; then
            if curl -s -f "http://localhost:$port" >/dev/null 2>&1 || \
               curl -s -f "http://localhost:$port/api/v1/health" >/dev/null 2>&1; then
                return 0
            fi
        else
            log_error "Dashboard process terminated unexpectedly during startup."
            if [ -f "$DASHBOARD_LOG_FILE" ]; then
                log_error "Recent logs from $DASHBOARD_LOG_FILE:"
                tail -n 20 "$DASHBOARD_LOG_FILE" >&2
            fi
            return 1
        fi
        sleep 0.5
        elapsed=$((elapsed + 1))
    done

    log_error "Timed out waiting for Dashboard frontend (http://localhost:$port)."
    if [ -f "$DASHBOARD_LOG_FILE" ]; then
        tail -n 20 "$DASHBOARD_LOG_FILE" >&2
    fi
    return 1
}

get_expected_ngrok_domain() {
    local raw="${1:-}"
    if [ -z "$raw" ]; then
        echo ""
        return 0
    fi
    # Strip scheme if present
    local d="${raw#*://}"
    # Strip trailing path if present
    d="${d%%/*}"
    # Strip port if present
    d="${d%%:*}"
    echo "$d"
}

get_ngrok_endpoint_url() {
    local raw="${1:-}"
    if [ -z "$raw" ]; then
        echo ""
        return 0
    fi
    local d
    d="$(get_expected_ngrok_domain "$raw")"
    if [ -n "$d" ]; then
        echo "https://$d"
    else
        echo ""
    fi
}

get_ngrok_url() {

    local inspect_port="$1"
    curl -s "http://127.0.0.1:$inspect_port/api/tunnels" 2>/dev/null | python3 -c '
import sys, json
try:
    data = json.load(sys.stdin)
    tunnels = data.get("tunnels", [])
    for t in tunnels:
        if t.get("proto") == "https":
            print(t.get("public_url", ""))
            sys.exit(0)
    if tunnels:
        print(tunnels[0].get("public_url", ""))
        sys.exit(0)
except Exception:
    pass
' 2>/dev/null || true
}

wait_for_ngrok() {
    local inspect_port="$1"
    local expected_domain="${2:-}"
    local timeout=15
    local elapsed=0

    while [ "$elapsed" -lt "$timeout" ]; do
        if is_service_running "$NGROK_PID_FILE" "ngrok"; then
            local url
            url="$(get_ngrok_url "$inspect_port")"
            if [ -n "$url" ]; then
                if [ -n "$expected_domain" ]; then
                    if echo "$url" | grep -qi "$expected_domain"; then
                        return 0
                    else
                        log_error "ngrok tunnel active with unexpected URL '$url' (expected domain '$expected_domain')."
                        return 1
                    fi
                else
                    return 0
                fi
            fi
        else
            log_error "ngrok process terminated unexpectedly during startup."
            if [ -f "$NGROK_LOG_FILE" ]; then
                log_error "Recent logs from $NGROK_LOG_FILE:"
                tail -n 20 "$NGROK_LOG_FILE" >&2
            fi
            if [ -n "$expected_domain" ]; then
                log_error "Verify that domain '$expected_domain' is assigned to your authenticated ngrok account."
            fi
            return 1
        fi
        sleep 0.5
        elapsed=$((elapsed + 1))
    done

    log_error "Timed out waiting for ngrok tunnel creation."
    if [ -f "$NGROK_LOG_FILE" ]; then
        tail -n 20 "$NGROK_LOG_FILE" >&2
    fi
    if [ -n "$expected_domain" ]; then
        log_error "Verify that domain '$expected_domain' is assigned to your authenticated ngrok account."
    fi
    return 1
}

# ------------------------------------------------------------------------------
# 5. Core Actions: start, stop, status, logs
# ------------------------------------------------------------------------------
cmd_start() {
    load_env
    mkdir -p "$DEV_DIR" "$LOG_DIR"

    # Step 1: Pre-flight checks
    if ! check_prereqs; then
        return 1
    fi

    # Step 2: Check port availability and existing instances
    log_info "Checking port availability..."
    local drevd_status=0
    local dash_status=0
    local ngrok_status=0

    check_port "$DREV_PORT" "drevd" "$DREVD_PID_FILE" "drevd" || drevd_status=$?
    check_port "$DASHBOARD_PORT" "dashboard" "$DASHBOARD_PID_FILE" "next|node|npm" || dash_status=$?
    check_port "$NGROK_INSPECT_PORT" "ngrok" "$NGROK_PID_FILE" "ngrok" || ngrok_status=$?

    # Handle unmanaged port conflicts
    if [ "$drevd_status" -eq 1 ] || [ "$dash_status" -eq 1 ] || [ "$ngrok_status" -eq 1 ]; then
        log_error "Cannot start Drev CI due to port conflicts. Resolve the conflicts above and re-try."
        return 1
    fi

    # Handle case where all managed services are already running
    if [ "$drevd_status" -eq 2 ] && [ "$dash_status" -eq 2 ] && [ "$ngrok_status" -eq 2 ]; then
        log_ok "All Drev CI services are already running and healthy."
        cmd_status
        return 0
    fi

    # Step 3: Build daemon binary
    if ! build_drevd; then
        return 1
    fi

    # Track services started in this session for cleanup on partial failure
    local started_drevd=0
    local started_dashboard=0
    local started_ngrok=0

    cleanup_on_failure() {
        log_warn "Startup failed. Cleaning up started processes..."
        if [ "$started_ngrok" -eq 1 ]; then
            stop_service "ngrok" "$NGROK_PID_FILE" "ngrok"
        fi
        if [ "$started_dashboard" -eq 1 ]; then
            stop_service "dashboard" "$DASHBOARD_PID_FILE" "next|node|npm"
        fi
        if [ "$started_drevd" -eq 1 ]; then
            stop_service "drevd" "$DREVD_PID_FILE" "drevd"
        fi
    }

    # Step 4: Start drevd daemon
    if [ "$drevd_status" -eq 0 ]; then
        log_info "Starting drevd daemon on port $DREV_PORT..."
        : > "$DREVD_LOG_FILE"
        "$REPO_ROOT/bin/drevd" \
            --port "$DREV_PORT" \
            --db "$DB_PATH" \
            --log-dir "$LOG_DIR" \
            >> "$DREVD_LOG_FILE" 2>&1 &
        echo $! > "$DREVD_PID_FILE"
        started_drevd=1

        if ! wait_for_drevd "$DREV_PORT"; then
            cleanup_on_failure
            return 1
        fi
        log_ok "drevd is running and healthy (PID $(cat "$DREVD_PID_FILE"))"
    else
        log_ok "drevd is already running (PID $(cat "$DREVD_PID_FILE"))"
    fi

    # Step 5: Start Next.js dashboard
    if [ "$dash_status" -eq 0 ]; then
        log_info "Starting dashboard on port $DASHBOARD_PORT..."
        : > "$DASHBOARD_LOG_FILE"

        local next_bin="$REPO_ROOT/dashboard/node_modules/.bin/next"
        if [ -x "$next_bin" ]; then
            (
                cd "$REPO_ROOT/dashboard"
                export DREV_BACKEND_URL="http://localhost:$DREV_PORT"
                exec "$next_bin" dev --port "$DASHBOARD_PORT"
            ) >> "$DASHBOARD_LOG_FILE" 2>&1 &
        else
            (
                cd "$REPO_ROOT/dashboard"
                export DREV_BACKEND_URL="http://localhost:$DREV_PORT"
                exec npm run dev -- --port "$DASHBOARD_PORT"
            ) >> "$DASHBOARD_LOG_FILE" 2>&1 &
        fi
        echo $! > "$DASHBOARD_PID_FILE"
        started_dashboard=1

        if ! wait_for_dashboard "$DASHBOARD_PORT"; then
            cleanup_on_failure
            return 1
        fi
        log_ok "Dashboard is running and accessible (PID $(cat "$DASHBOARD_PID_FILE"))"
    else
        log_ok "Dashboard is already running (PID $(cat "$DASHBOARD_PID_FILE"))"
    fi

    # Step 6: Start ngrok tunnel
    if [ "$ngrok_status" -eq 0 ]; then
        local expected_domain
        expected_domain="$(get_expected_ngrok_domain "$NGROK_DOMAIN")"

        log_info "Starting ngrok tunnel for port $DREV_PORT..."
        : > "$NGROK_LOG_FILE"

        local ngrok_args=( "http" "$DREV_PORT" "--log=stdout" "--log-format=term" )
        if [ -n "$expected_domain" ]; then
            local endpoint_url
            endpoint_url="$(get_ngrok_endpoint_url "$NGROK_DOMAIN")"
            log_info "Configured ngrok domain: $expected_domain ($endpoint_url)"
            ngrok_args+=( "--url=$endpoint_url" )
        fi

        ngrok "${ngrok_args[@]}" >> "$NGROK_LOG_FILE" 2>&1 &
        echo $! > "$NGROK_PID_FILE"
        started_ngrok=1

        if ! wait_for_ngrok "$NGROK_INSPECT_PORT" "$expected_domain"; then
            cleanup_on_failure
            return 1
        fi
        log_ok "ngrok tunnel established (PID $(cat "$NGROK_PID_FILE"))"
    else
        log_ok "ngrok tunnel is already active (PID $(cat "$NGROK_PID_FILE"))"
    fi

    # Step 7: Display stack status and webhook guidance
    printf "\n"
    cmd_status

    # If ATTACH or FOREGROUND is requested, follow logs
    if [ "${ATTACH:-0}" = "1" ] || [ "${FOREGROUND:-0}" = "1" ]; then
        printf "\n%bAttaching to service logs (Ctrl+C to detach; services will keep running)...%b\n\n" "$C_CYAN" "$C_RESET"
        cmd_logs ""
    fi
}

cmd_stop() {
    log_info "Stopping Drev CI services..."
    mkdir -p "$DEV_DIR"

    # Stop in reverse dependency order: ngrok -> dashboard -> drevd
    stop_service "ngrok" "$NGROK_PID_FILE" "ngrok"
    stop_service "dashboard" "$DASHBOARD_PID_FILE" "next|node|npm"
    stop_service "drevd" "$DREVD_PID_FILE" "drevd"

    log_ok "All managed Drev CI dev services stopped."
}

cmd_status() {
    load_env
    mkdir -p "$DEV_DIR"

    local drevd_run=0
    local dash_run=0
    local ngrok_run=0

    local drevd_pid="-"
    local dash_pid="-"
    local ngrok_pid="-"

    local drevd_health="DOWN"
    local dash_health="DOWN"
    local ngrok_health="DOWN"
    local public_url=""

    if is_service_running "$DREVD_PID_FILE" "drevd"; then
        drevd_run=1
        drevd_pid="$(cat "$DREVD_PID_FILE")"
        if curl -s -f "http://localhost:$DREV_PORT/api/v1/health" >/dev/null 2>&1; then
            drevd_health="${C_GREEN}HEALTHY (HTTP 200)${C_RESET}"
        else
            drevd_health="${C_YELLOW}UNRESPONSIVE${C_RESET}"
        fi
    else
        # Check if external occupant
        local l
        l=$(get_port_listener "$DREV_PORT")
        if [ -n "$l" ]; then
            drevd_health="${C_RED}PORT CONFLICT ($l)${C_RESET}"
        else
            drevd_health="${C_RESET}STOPPED"
        fi
    fi

    if is_service_running "$DASHBOARD_PID_FILE" "next|node|npm"; then
        dash_run=1
        dash_pid="$(cat "$DASHBOARD_PID_FILE")"
        if curl -s -f "http://localhost:$DASHBOARD_PORT" >/dev/null 2>&1 || \
           curl -s -f "http://localhost:$DASHBOARD_PORT/api/v1/health" >/dev/null 2>&1; then
            dash_health="${C_GREEN}HEALTHY (HTTP 200)${C_RESET}"
        else
            dash_health="${C_YELLOW}STARTING / UNRESPONSIVE${C_RESET}"
        fi
    else
        local l
        l=$(get_port_listener "$DASHBOARD_PORT")
        if [ -n "$l" ]; then
            dash_health="${C_RED}PORT CONFLICT ($l)${C_RESET}"
        else
            dash_health="${C_RESET}STOPPED"
        fi
    fi

    local expected_domain
    expected_domain="$(get_expected_ngrok_domain "$NGROK_DOMAIN")"

    if is_service_running "$NGROK_PID_FILE" "ngrok"; then
        ngrok_run=1
        ngrok_pid="$(cat "$NGROK_PID_FILE")"
        public_url="$(get_ngrok_url "$NGROK_INSPECT_PORT")"
        if [ -n "$public_url" ]; then
            if [ -n "$expected_domain" ]; then
                if echo "$public_url" | grep -qi "$expected_domain"; then
                    ngrok_health="${C_GREEN}TUNNEL ACTIVE (verified: $expected_domain)${C_RESET}"
                else
                    ngrok_health="${C_YELLOW}TUNNEL ACTIVE (mismatch: $public_url)${C_RESET}"
                fi
            else
                ngrok_health="${C_GREEN}TUNNEL ACTIVE${C_RESET}"
            fi
        else
            ngrok_health="${C_YELLOW}TUNNEL CONNECTING${C_RESET}"
        fi
    else
        local l
        l=$(get_port_listener "$NGROK_INSPECT_PORT")
        if [ -n "$l" ]; then
            ngrok_health="${C_RED}PORT CONFLICT ($l)${C_RESET}"
        elif [ -n "$expected_domain" ]; then
            ngrok_health="${C_RESET}STOPPED (configured: $expected_domain)"
        else
            ngrok_health="${C_RESET}STOPPED"
        fi
    fi

    printf "========================================================================\n"
    printf "                      %bDrev CI Dev Stack Status%b\n" "$C_BOLD" "$C_RESET"
    printf "========================================================================\n"
    printf "  %-12s %-8s %-8s %-25b\n" "SERVICE" "PID" "PORT" "STATUS"
    printf "  ----------------------------------------------------------------------\n"
    printf "  %-12s %-8s %-8s %-25b\n" "drevd" "$drevd_pid" "$DREV_PORT" "$drevd_health"
    printf "  %-12s %-8s %-8s %-25b\n" "dashboard" "$dash_pid" "$DASHBOARD_PORT" "$dash_health"
    printf "  %-12s %-8s %-8s %-25b\n" "ngrok" "$ngrok_pid" "$NGROK_INSPECT_PORT" "$ngrok_health"
    printf "========================================================================\n"

    if [ -n "$public_url" ]; then
        printf "  %bEndpoints:%b\n" "$C_BOLD" "$C_RESET"
        printf "    Dashboard:       %bhttp://localhost:%s%b\n" "$C_CYAN" "$DASHBOARD_PORT" "$C_RESET"
        printf "    Backend Daemon:  %bhttp://localhost:%s%b\n" "$C_CYAN" "$DREV_PORT" "$C_RESET"
        printf "    Forwarding URL:  %b%s%b\n" "$C_CYAN" "$public_url" "$C_RESET"
        printf "    Webhook URL:     %b%s/webhooks/github%b\n" "$C_CYAN" "$public_url" "$C_RESET"
        printf "\n"
        printf "  %bGitHub Webhook Instructions:%b\n" "$C_BOLD" "$C_RESET"
        printf "    1. Go to your repository Settings -> Webhooks\n"
        printf "    2. Set Payload URL:  %b%s/webhooks/github%b\n" "$C_YELLOW" "$public_url" "$C_RESET"
        printf "    3. Content type:     %bapplication/json%b\n" "$C_YELLOW" "$C_RESET"
        printf "    4. Secret:           (must match DREV_WEBHOOK_SECRET in ~/Desktop/drevci/.env)\n"
        if [ -n "$expected_domain" ] && echo "$public_url" | grep -qi "$expected_domain"; then
            printf "    %bNotice:%b Using verified stable domain '%s'.\n" "$C_GREEN" "$expected_domain"
            printf "    Your Webhook Payload URL is stable and does not change across restarts.\n"
        else
            printf "    %bNotice:%b If using a dynamic ngrok URL, the forwarding URL changes\n" "$C_YELLOW" "$C_RESET"
            printf "    each time ngrok restarts. Update the GitHub webhook whenever restarted.\n"
            printf "    To use a stable domain, set NGROK_DOMAIN in your .env file.\n"
        fi
        printf "========================================================================\n"
    elif [ "$drevd_run" -eq 1 ]; then
        printf "  %bEndpoints:%b\n" "$C_BOLD" "$C_RESET"
        printf "    Dashboard:       %bhttp://localhost:%s%b\n" "$C_CYAN" "$DASHBOARD_PORT" "$C_RESET"
        printf "    Backend Daemon:  %bhttp://localhost:%s%b\n" "$C_CYAN" "$DREV_PORT" "$C_RESET"
        printf "========================================================================\n"
    fi

    printf "  %bManagement Commands:%b\n" "$C_BOLD" "$C_RESET"
    printf "    Follow logs:   %bmake dev-logs%b   (or ./scripts/dev.sh logs)\n" "$C_CYAN" "$C_RESET"
    printf "    Check status:  %bmake dev-status%b (or ./scripts/dev.sh status)\n" "$C_CYAN" "$C_RESET"
    printf "    Stop stack:    %bmake dev-stop%b   (or ./scripts/dev.sh stop)\n" "$C_CYAN" "$C_RESET"
    printf "========================================================================\n"

    if [ "$drevd_run" -eq 1 ] && [ "$dash_run" -eq 1 ] && [ "$ngrok_run" -eq 1 ]; then
        return 0
    else
        return 1
    fi
}

cmd_logs() {
    local target="${1:-}"
    mkdir -p "$DEV_DIR"

    # Ensure log files exist
    touch "$DREVD_LOG_FILE" "$DASHBOARD_LOG_FILE" "$NGROK_LOG_FILE"

    case "$target" in
        drevd)
            printf "%bFollowing drevd logs (%s)... (Ctrl+C to stop)%b\n" "$C_CYAN" "$DREVD_LOG_FILE" "$C_RESET"
            tail -n 30 -f "$DREVD_LOG_FILE"
            ;;
        dashboard)
            printf "%bFollowing dashboard logs (%s)... (Ctrl+C to stop)%b\n" "$C_CYAN" "$DASHBOARD_LOG_FILE" "$C_RESET"
            tail -n 30 -f "$DASHBOARD_LOG_FILE"
            ;;
        ngrok)
            printf "%bFollowing ngrok logs (%s)... (Ctrl+C to stop)%b\n" "$C_CYAN" "$NGROK_LOG_FILE" "$C_RESET"
            tail -n 30 -f "$NGROK_LOG_FILE"
            ;;
        *)
            printf "%bFollowing consolidated logs... (Ctrl+C to stop)%b\n" "$C_CYAN" "$C_RESET"
            tail -n 25 -f "$DREVD_LOG_FILE" "$DASHBOARD_LOG_FILE" "$NGROK_LOG_FILE"
            ;;
    esac
}

# ------------------------------------------------------------------------------
# 6. Main Dispatcher
# ------------------------------------------------------------------------------
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    action="${1:-start}"

    case "$action" in
        start|up)
            cmd_start
            ;;
        stop|down)
            cmd_stop
            ;;
        status)
            cmd_status || true
            ;;
        logs)
            shift || true
            cmd_logs "${1:-}"
            ;;
        check|prereqs)
            load_env
            check_prereqs
            ;;
        check-port)
            shift || true
            check_port "$@"
            ;;
        *)
            echo "Usage: $0 {start|stop|status|logs [service]|check}"
            exit 1
            ;;
    esac
fi
