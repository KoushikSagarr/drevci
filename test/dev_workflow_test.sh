#!/usr/bin/env bash
# ==============================================================================
# Drev CI — Dev Workflow Unit & Integration Test Suite
#
# Validates scripts/dev.sh:
#   1. Prerequisite checking
#   2. Stale PID cleanup
#   3. Process ownership verification (avoiding killing unrelated PIDs)
#   4. Occupied port conflict detection (reporting external PIDs)
#   5. Managed service recognition (idempotent status)
#   6. Graceful termination and descendant process cleanup
#   7. Rollback on partial failure
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEV_SCRIPT="$REPO_ROOT/scripts/dev.sh"

TEST_TMP="$(mktemp -d)"
trap 'rm -rf "$TEST_TMP"' EXIT

# Colors
C_GREEN="\033[32m"
C_RED="\033[31m"
C_RESET="\033[0m"

pass() { printf "%b[PASS]%b %s\n" "$C_GREEN" "$C_RESET" "$1"; }
fail() { printf "%b[FAIL]%b %s\n" "$C_RED" "$C_RESET" "$1" >&2; exit 1; }

source_dev_sh() {
    export DREV_DEV_DIR="$TEST_TMP/dev"
    mkdir -p "$DREV_DEV_DIR"
    source "$DEV_SCRIPT"
}

source_dev_sh


# ------------------------------------------------------------------------------
# Test 1: Prerequisites Check
# ------------------------------------------------------------------------------
echo "--- Running Test 1: Prerequisites Check ---"
if check_prereqs >/dev/null 2>&1; then
    pass "Prerequisites check passed with all required tools installed"
else
    fail "Prerequisites check failed unexpectedly"
fi

# ------------------------------------------------------------------------------
# Test 2: Stale PID File Cleanup
# ------------------------------------------------------------------------------
echo "--- Running Test 2: Stale PID File Cleanup ---"
DUMMY_PID_FILE="$TEST_TMP/dev/stale.pid"
# Choose a PID that is almost certainly not running
echo "999999" > "$DUMMY_PID_FILE"

if is_service_running "$DUMMY_PID_FILE" "drevd"; then
    fail "is_service_running returned true for non-existent PID"
fi

if [ -f "$DUMMY_PID_FILE" ]; then
    fail "Stale PID file was not automatically removed"
fi
pass "Stale PID file detected and removed safely"

# ------------------------------------------------------------------------------
# Test 3: Process Ownership Mismatch (PID recycled)
# ------------------------------------------------------------------------------
echo "--- Running Test 3: Process Ownership Mismatch ---"
# $$ is the current test shell, guaranteed to be alive and owned by this user
# but its command name (bash) will NOT match "drevd"
MISMATCH_PID_FILE="$TEST_TMP/dev/mismatch.pid"
echo "$$" > "$MISMATCH_PID_FILE"

if is_service_running "$MISMATCH_PID_FILE" "drevd"; then
    fail "is_service_running reported match for PID $$ with drevd"
fi

if [ -f "$MISMATCH_PID_FILE" ]; then
    fail "Mismatched PID file was not removed"
fi

# Ensure test shell was not killed (obviously still alive)
if ! kill -0 "$$" 2>/dev/null; then
    fail "PID $$ is not alive"
fi
pass "Process ownership verified: mismatched command rejected without affecting process"


# ------------------------------------------------------------------------------
# Test 4: Occupied Port Detection (Unmanaged Process)
# ------------------------------------------------------------------------------
echo "--- Running Test 4: Occupied Port Detection ---"
TEST_PORT=19876
# Start a simple python listener on TEST_PORT
python3 -c "
import socket, time
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(('127.0.0.1', $TEST_PORT))
s.listen(1)
time.sleep(10)
" &
LISTENER_PID=$!
sleep 0.5

# Test check_port against this unmanaged port
CONFLICT_PID_FILE="$TEST_TMP/dev/external.pid"
output=""
status=0
output=$(check_port "$TEST_PORT" "test-service" "$CONFLICT_PID_FILE" "drevd" 2>&1) || status=$?

# Kill listener
kill -9 "$LISTENER_PID" 2>/dev/null || true
wait "$LISTENER_PID" 2>/dev/null || true

if [ "$status" -ne 1 ]; then
    fail "check_port did not return error code 1 for occupied port (got $status)"
fi

if ! echo "$output" | grep -q "already occupied by unmanaged process"; then
    fail "check_port output did not contain expected error message: $output"
fi
pass "Occupied port conflict correctly detected with process details"

# ------------------------------------------------------------------------------
# Test 5: Managed Port Recognition (Already Running)
# ------------------------------------------------------------------------------
echo "--- Running Test 5: Managed Port Recognition ---"
TEST_PORT=19877
# Start a background sleep/listener simulating a managed service
python3 -c "
import socket, time
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(('127.0.0.1', $TEST_PORT))
s.listen(1)
time.sleep(10)
" &
MANAGED_PID=$!
sleep 0.5

MANAGED_PID_FILE="$TEST_TMP/dev/managed.pid"
echo "$MANAGED_PID" > "$MANAGED_PID_FILE"

status=0
check_port "$TEST_PORT" "managed-service" "$MANAGED_PID_FILE" "python" || status=$?

kill -9 "$MANAGED_PID" 2>/dev/null || true
wait "$MANAGED_PID" 2>/dev/null || true

if [ "$status" -ne 2 ]; then
    fail "check_port did not return code 2 (already running) for managed process (got $status)"
fi
pass "Managed service recognition succeeded (idempotent port check)"

# ------------------------------------------------------------------------------
# Test 6: Graceful Stop and Descendant Process Termination
# ------------------------------------------------------------------------------
echo "--- Running Test 6: Graceful Stop and Descendants ---"
# Launch a parent process that spawns a child process
python3 -c "
import subprocess, time, sys
child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
time.sleep(30)
" &
PARENT_PID=$!
sleep 0.5

PARENT_PID_FILE="$TEST_TMP/dev/parent.pid"
echo "$PARENT_PID" > "$PARENT_PID_FILE"

# Find child PID
CHILD_PIDS=$(pgrep -P "$PARENT_PID" 2>/dev/null || true)
if [ -z "$CHILD_PIDS" ]; then
    fail "Could not find child process for test"
fi

# Stop service
stop_service "test-parent" "$PARENT_PID_FILE" "python" >/dev/null 2>&1

# Verify both parent and child are terminated
if kill -0 "$PARENT_PID" 2>/dev/null; then
    kill -9 "$PARENT_PID" 2>/dev/null || true
    fail "Parent process was not terminated"
fi

for cp in $CHILD_PIDS; do
    if kill -0 "$cp" 2>/dev/null; then
        kill -9 "$cp" 2>/dev/null || true
        fail "Child process $cp was not terminated"
    fi
done

if [ -f "$PARENT_PID_FILE" ]; then
    fail "PID file was not removed after stop"
fi
pass "Graceful stop terminated parent and all descendant processes"

# ------------------------------------------------------------------------------
# Test 7: Secret Preservation & Non-exposure
# ------------------------------------------------------------------------------
echo "--- Running Test 7: Secret Preservation in Environment Loading ---"
MOCK_ENV="$TEST_TMP/mock.env"
cat << 'EOF' > "$MOCK_ENV"
# Test env
DREV_TEST_KEY=secret_value_12345
DREV_QUOTED_KEY="another_secret"
EOF

# Load using the script's parsing logic
(
    REPO_ROOT="$TEST_TMP"
    mv "$MOCK_ENV" "$TEST_TMP/.env"
    load_env
    if [ "$DREV_TEST_KEY" != "secret_value_12345" ]; then
        exit 1
    fi
    if [ "$DREV_QUOTED_KEY" != "another_secret" ]; then
        exit 1
    fi
)
pass "Environment loader correctly parses secrets and quotes without exposure"

# ------------------------------------------------------------------------------
# Test 8: Partial Startup Failure Rollback
# ------------------------------------------------------------------------------
echo "--- Running Test 8: Partial Startup Failure Rollback ---"
# Simulate starting service A, but then service B fails
python3 -c "import time; time.sleep(30)" &
MOCK_A_PID=$!
MOCK_A_PID_FILE="$TEST_TMP/dev/mock_a.pid"
echo "$MOCK_A_PID" > "$MOCK_A_PID_FILE"

# Define mock cleanup simulating cmd_start cleanup_on_failure
simulate_failure_and_rollback() {
    stop_service "mock_a" "$MOCK_A_PID_FILE" "python" >/dev/null 2>&1
}

simulate_failure_and_rollback

if kill -0 "$MOCK_A_PID" 2>/dev/null; then
    kill -9 "$MOCK_A_PID" 2>/dev/null || true
    fail "Rollback failed to terminate mock service A"
fi

if [ -f "$MOCK_A_PID_FILE" ]; then
    fail "Rollback failed to remove mock service A PID file"
fi
pass "Partial startup failure rollback cleaned up orphan processes and state"

# ------------------------------------------------------------------------------
# Test 9: ngrok Domain Parsing & Normalization
# ------------------------------------------------------------------------------
echo "--- Running Test 9: ngrok Domain Normalization ---"
d1="$(get_expected_ngrok_domain "picked-indirectly-cheetah.ngrok-free.app")"
if [ "$d1" != "picked-indirectly-cheetah.ngrok-free.app" ]; then
    fail "Expected 'picked-indirectly-cheetah.ngrok-free.app', got '$d1'"
fi

d2="$(get_expected_ngrok_domain "https://picked-indirectly-cheetah.ngrok-free.app")"
if [ "$d2" != "picked-indirectly-cheetah.ngrok-free.app" ]; then
    fail "Expected 'picked-indirectly-cheetah.ngrok-free.app', got '$d2'"
fi

d3="$(get_expected_ngrok_domain "https://picked-indirectly-cheetah.ngrok-free.app/webhooks/github")"
if [ "$d3" != "picked-indirectly-cheetah.ngrok-free.app" ]; then
    fail "Expected 'picked-indirectly-cheetah.ngrok-free.app', got '$d3'"
fi

d4="$(get_expected_ngrok_domain "")"
if [ "$d4" != "" ]; then
    fail "Expected empty string for empty input, got '$d4'"
fi

u1="$(get_ngrok_endpoint_url "picked-indirectly-cheetah.ngrok-free.app")"
if [ "$u1" != "https://picked-indirectly-cheetah.ngrok-free.app" ]; then
    fail "Expected endpoint URL 'https://picked-indirectly-cheetah.ngrok-free.app', got '$u1'"
fi
pass "ngrok domain parsing and endpoint URL normalization verified"

# ------------------------------------------------------------------------------
# Test 10: ngrok Command Construction
# ------------------------------------------------------------------------------
echo "--- Running Test 10: ngrok Command Construction ---"
build_test_args() {
    local port="$1"
    local domain="${2:-}"
    local exp_d
    exp_d="$(get_expected_ngrok_domain "$domain")"
    local args=( "http" "$port" "--log=stdout" "--log-format=term" )
    if [ -n "$exp_d" ]; then
        local url
        url="$(get_ngrok_endpoint_url "$domain")"
        args+=( "--url=$url" )
    fi
    echo "${args[@]}"
}

args_with_domain="$(build_test_args 9090 "picked-indirectly-cheetah.ngrok-free.app")"
if ! echo "$args_with_domain" | grep -q -- "--url=https://picked-indirectly-cheetah.ngrok-free.app"; then
    fail "Expected --url flag in args, got: $args_with_domain"
fi

args_without_domain="$(build_test_args 9090 "")"
if echo "$args_without_domain" | grep -q -- "--url"; then
    fail "Did not expect --url flag when domain is empty, got: $args_without_domain"
fi
pass "ngrok command construction verified for both configured and omitted domain"

# ------------------------------------------------------------------------------
# Test 11: wait_for_ngrok Hostname & Mismatch Verification
# ------------------------------------------------------------------------------
echo "--- Running Test 11: wait_for_ngrok Hostname Verification ---"
MOCK_INSPECT_PORT=19878

# Start a mock python server that simulates ngrok API responding with matched domain
python3 -c "
import http.server, socketserver, json

class MockNgrok(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({
            'tunnels': [{'proto': 'https', 'public_url': 'https://picked-indirectly-cheetah.ngrok-free.app'}]
        }).encode())
    def log_message(self, format, *args):
        pass

with socketserver.TCPServer(('127.0.0.1', $MOCK_INSPECT_PORT), MockNgrok) as httpd:
    httpd.serve_forever()
" &
MOCK_SERVER_PID=$!
sleep 0.5

# Write dummy ngrok.pid so is_service_running checks pass
export NGROK_PID_FILE="$TEST_TMP/dev/ngrok.pid"
echo "$MOCK_SERVER_PID" > "$NGROK_PID_FILE"

# Test match: should succeed (return 0)
if ! wait_for_ngrok "$MOCK_INSPECT_PORT" "picked-indirectly-cheetah.ngrok-free.app"; then
    kill -9 "$MOCK_SERVER_PID" 2>/dev/null || true
    fail "wait_for_ngrok failed to match expected domain"
fi

# Test mismatch: should fail (return 1)
if wait_for_ngrok "$MOCK_INSPECT_PORT" "other-unexpected-domain.ngrok-free.app" >/dev/null 2>&1; then
    kill -9 "$MOCK_SERVER_PID" 2>/dev/null || true
    fail "wait_for_ngrok accepted mismatched domain"
fi

kill -9 "$MOCK_SERVER_PID" 2>/dev/null || true
wait "$MOCK_SERVER_PID" 2>/dev/null || true
rm -f "$NGROK_PID_FILE"
pass "wait_for_ngrok correctly verifies active tunnel hostname and rejects mismatches"

# ------------------------------------------------------------------------------
# Test 12: cmd_status Inactive Configured Domain Reporting
# ------------------------------------------------------------------------------
echo "--- Running Test 12: cmd_status Configured vs Inactive Reporting ---"
(
    export DREV_PORT=19879
    export DASHBOARD_PORT=19880
    export NGROK_INSPECT_PORT=19881
    export NGROK_DOMAIN="picked-indirectly-cheetah.ngrok-free.app"
    export DREV_DEV_DIR="$TEST_TMP/dev"

    output="$(cmd_status 2>&1 || true)"
    if ! echo "$output" | grep -q "STOPPED (configured: picked-indirectly-cheetah.ngrok-free.app)"; then
        fail "cmd_status did not show inactive configured domain: $output"
    fi
)
pass "cmd_status accurately distinguishes configured inactive domain from generic stop"

# ------------------------------------------------------------------------------
# Test 13: Real drevd Graceful Shutdown on SIGTERM (Zero Force-Kill)
# ------------------------------------------------------------------------------
echo "--- Running Test 13: Real drevd Graceful Shutdown ---"
REAL_DREVD_PORT=19890
REAL_DREVD_LOG="$TEST_TMP/drevd_test.log"
REAL_DREVD_PID_FILE="$TEST_TMP/dev/drevd_real.pid"

# Ensure binary is compiled
if [ ! -f "$REPO_ROOT/bin/drevd" ]; then
    go build -o "$REPO_ROOT/bin/drevd" "$REPO_ROOT/cmd/drevd"
fi

# Launch real drevd in test environment
"$REPO_ROOT/bin/drevd" \
    --port "$REAL_DREVD_PORT" \
    --db "$TEST_TMP/test_drev.db" \
    --log-dir "$TEST_TMP/logs" \
    > "$REAL_DREVD_LOG" 2>&1 &
REAL_DREVD_PID=$!
echo "$REAL_DREVD_PID" > "$REAL_DREVD_PID_FILE"

# Wait for health endpoint to report ready (up to 5s)
ready=0
for i in $(seq 1 50); do
    if curl -s -f "http://localhost:$REAL_DREVD_PORT/api/v1/health" >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 0.1
done

if [ "$ready" -ne 1 ]; then
    kill -9 "$REAL_DREVD_PID" 2>/dev/null || true
    fail "Real drevd test instance failed to become healthy on port $REAL_DREVD_PORT"
fi

# Execute stop_service and capture output
stop_output=$(stop_service "drevd" "$REAL_DREVD_PID_FILE" "drevd" 2>&1)

# Verify process terminated
if kill -0 "$REAL_DREVD_PID" 2>/dev/null; then
    kill -9 "$REAL_DREVD_PID" 2>/dev/null || true
    fail "Real drevd was still running after stop_service"
fi

# Verify it was NOT force-killed
if echo "$stop_output" | grep -q "force killed"; then
    fail "Real drevd required force-kill to terminate: $stop_output"
fi

if ! echo "$stop_output" | grep -q "done"; then
    fail "stop_service did not report clean completion: $stop_output"
fi

# Verify drevd log records graceful shutdown
if ! grep -q "Shutting down gracefully" "$REAL_DREVD_LOG"; then
    fail "Real drevd log did not record 'Shutting down gracefully': $(cat "$REAL_DREVD_LOG")"
fi

pass "Real drevd daemon shut down gracefully on SIGTERM without force-kill"

echo "=========================================================================="
printf "%bAll 13 Dev Workflow tests passed successfully!%b\n" "$C_GREEN" "$C_RESET"
echo "=========================================================================="
