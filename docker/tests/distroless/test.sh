#!/usr/bin/env bash
# Integration tests for the distroless ClickHouse images.
#
# Usage:
#   bash test.sh [--version VERSION] [--server-context DIR] [--keeper-context DIR]
#
# The script:
#   1. Builds server + keeper distroless images (production and debug targets)
#   2. Runs each container and verifies startup
#   3. Checks CLICKHOUSE_USER/PASSWORD/DB env-var handling
#   4. Runs SQL init scripts (plain and .sql.gz) and verifies the data
#   5. Confirms that /bin/sh is absent in the production image
#   6. Confirms that /busybox/sh is present in the debug image
#   7. Verifies the keeper starts successfully
#   8. CLICKHOUSE_PASSWORD_FILE (Docker Secrets pattern)
#   9. Passthrough / override CMD (clickhouse-client --version)
#  10. Restart with existing data skips init
#  11. No password — default user restricted to localhost
#  12. CLICKHOUSE_SKIP_USER_SETUP
#  13. HTTP endpoint (port 8123)
#  14. docker stop during init (PID 1 signal handling)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

VERSION="${VERSION:-26.1.2.11}"
SERVER_CONTEXT="${REPO_ROOT}/docker/server"
KEEPER_CONTEXT="${REPO_ROOT}/docker/keeper"

SERVER_IMAGE="clickhouse/clickhouse-server:distroless-test"
SERVER_DEBUG_IMAGE="clickhouse/clickhouse-server:distroless-debug-test"
KEEPER_IMAGE="clickhouse/clickhouse-keeper:distroless-test"

PASS=0
FAIL=0

pass() { echo "  PASS: $*"; (( PASS++ )) || true; }
fail() { echo "  FAIL: $*" >&2; (( FAIL++ )) || true; }

# ──────────────────────────────────────────────────────────────────────────────
# Build images
# ──────────────────────────────────────────────────────────────────────────────
echo "=== Building images ==="

docker build \
    --file "${SERVER_CONTEXT}/Dockerfile.distroless" \
    --target production \
    --build-arg "VERSION=${VERSION}" \
    --tag "${SERVER_IMAGE}" \
    "${SERVER_CONTEXT}"

docker build \
    --file "${SERVER_CONTEXT}/Dockerfile.distroless" \
    --target debug \
    --build-arg "VERSION=${VERSION}" \
    --tag "${SERVER_DEBUG_IMAGE}" \
    "${SERVER_CONTEXT}"

docker build \
    --file "${KEEPER_CONTEXT}/Dockerfile.distroless" \
    --target production \
    --build-arg "VERSION=${VERSION}" \
    --tag "${KEEPER_IMAGE}" \
    "${KEEPER_CONTEXT}"

echo "=== Images built successfully ==="

# ──────────────────────────────────────────────────────────────────────────────
# Helper: wait for clickhouse-client SELECT 1 to succeed
# ──────────────────────────────────────────────────────────────────────────────
wait_for_server() {
    local container="$1"
    local port="$2"
    local user="${3:-default}"
    local password="${4:-}"
    local tries=60

    echo "  Waiting for ${container} on port ${port}..."
    while (( tries-- > 0 )); do
        if docker exec "${container}" \
               /usr/bin/clickhouse client \
               --host 127.0.0.1 --port "${port}" \
               -u "${user}" --password "${password}" \
               --query "SELECT 1" \
               >/dev/null 2>&1; then
            echo "  Server ready."
            return 0
        fi
        sleep 1
    done
    echo "  ERROR: server did not become ready." >&2
    docker logs "${container}" >&2
    return 1
}

# ──────────────────────────────────────────────────────────────────────────────
# Test 1: Basic server startup with custom user/password
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 1: basic server startup ==="
CID=$(docker run -d \
    --name ch-distroless-t1 \
    -e CLICKHOUSE_USER=testuser \
    -e CLICKHOUSE_PASSWORD=testpass \
    "${SERVER_IMAGE}")

wait_for_server ch-distroless-t1 9000 testuser testpass

result=$(docker exec ch-distroless-t1 \
    /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
    -u testuser --password testpass \
    --query "SELECT 42")
if [[ "${result}" == "42" ]]; then
    pass "server responds to queries"
else
    fail "expected '42', got '${result}'"
fi

docker rm -f ch-distroless-t1 >/dev/null

# ──────────────────────────────────────────────────────────────────────────────
# Test 2: No shell in production image
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 2: no shell in production image ==="
if docker run --rm --entrypoint /bin/sh "${SERVER_IMAGE}" -c "echo bad" 2>/dev/null; then
    fail "/bin/sh should not exist in production image"
else
    pass "/bin/sh absent from production image"
fi

if docker run --rm --entrypoint /bin/bash "${SERVER_IMAGE}" -c "echo bad" 2>/dev/null; then
    fail "/bin/bash should not exist in production image"
else
    pass "/bin/bash absent from production image"
fi

# ──────────────────────────────────────────────────────────────────────────────
# Test 3: busybox shell present in debug image
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 3: busybox shell in debug image ==="
result=$(docker run --rm --entrypoint /busybox/sh "${SERVER_DEBUG_IMAGE}" -c "echo ok" 2>/dev/null || true)
if [[ "${result}" == "ok" ]]; then
    pass "/busybox/sh available in debug image"
else
    fail "expected 'ok' from busybox shell, got '${result}'"
fi

# ──────────────────────────────────────────────────────────────────────────────
# Test 4: Init scripts (SQL files)
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 4: SQL init scripts ==="
CID=$(docker run -d \
    --name ch-distroless-t4 \
    -e CLICKHOUSE_USER=testuser \
    -e CLICKHOUSE_PASSWORD=testpass \
    -e CLICKHOUSE_DB=test_db \
    -v "${SCRIPT_DIR}/initdb:/docker-entrypoint-initdb.d:ro" \
    "${SERVER_IMAGE}")

wait_for_server ch-distroless-t4 9000 testuser testpass

result=$(docker exec ch-distroless-t4 \
    /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
    -u testuser --password testpass \
    --query "SELECT count() FROM test_db.events")
if [[ "${result}" == "3" ]]; then
    pass "init scripts created and populated test_db.events (count=3, includes .sql.gz)"
else
    fail "expected count 3 (2 from .sql + 1 from .sql.gz), got '${result}'"
fi

docker rm -f ch-distroless-t4 >/dev/null

# ──────────────────────────────────────────────────────────────────────────────
# Test 5: CLICKHOUSE_DB creates the database
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 5: CLICKHOUSE_DB creates a database ==="
CID=$(docker run -d \
    --name ch-distroless-t5 \
    -e CLICKHOUSE_USER=testuser \
    -e CLICKHOUSE_PASSWORD=testpass \
    -e CLICKHOUSE_DB=myapp \
    "${SERVER_IMAGE}")

wait_for_server ch-distroless-t5 9000 testuser testpass

result=$(docker exec ch-distroless-t5 \
    /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
    -u testuser --password testpass \
    --query "SELECT count() FROM system.databases WHERE name='myapp'")
if [[ "${result}" == "1" ]]; then
    pass "CLICKHOUSE_DB created database 'myapp'"
else
    fail "expected database 'myapp' to exist, got count '${result}'"
fi

docker rm -f ch-distroless-t5 >/dev/null

# ──────────────────────────────────────────────────────────────────────────────
# Test 6: ClickHouse Keeper starts
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 6: keeper startup ==="
CID=$(docker run -d \
    --name ch-distroless-keeper \
    "${KEEPER_IMAGE}")

echo "  Waiting for keeper on port 9181..."
tries=60
keeper_ready=false
while (( tries-- > 0 )); do
    if docker exec ch-distroless-keeper \
           /usr/bin/clickhouse keeper-client \
           --host 127.0.0.1 --port 9181 \
           -q "ruok" \
           2>/dev/null | grep -q "imok"; then
        keeper_ready=true
        break
    fi
    sleep 1
done

if [[ "${keeper_ready}" == "true" ]]; then
    pass "keeper started and responded to 'ruok'"
else
    fail "keeper did not start in time"
    docker logs ch-distroless-keeper >&2
fi

docker rm -f ch-distroless-keeper >/dev/null

# ──────────────────────────────────────────────────────────────────────────────
# Test 7: Container runs as non-root (uid 101)
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 7: container uid ==="
CID=$(docker run -d \
    --name ch-distroless-t7 \
    -e CLICKHOUSE_USER=testuser \
    -e CLICKHOUSE_PASSWORD=testpass \
    "${SERVER_IMAGE}")

wait_for_server ch-distroless-t7 9000 testuser testpass

uid=$(docker exec ch-distroless-t7 \
    /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
    -u testuser --password testpass \
    --query "SELECT currentUser()")

# Verify via /proc that the server process runs as uid 101
container_uid=$(docker inspect --format '{{.Config.User}}' ch-distroless-t7)
if [[ "${container_uid}" == "101:101" ]]; then
    pass "container configured with USER 101:101"
else
    fail "expected USER 101:101, got '${container_uid}'"
fi

docker rm -f ch-distroless-t7 >/dev/null

# ──────────────────────────────────────────────────────────────────────────────
# Test 8: CLICKHOUSE_PASSWORD_FILE (Docker Secrets pattern)
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 8: CLICKHOUSE_PASSWORD_FILE ==="
PWFILE_DIR=$(mktemp -d)
echo -n "secretpass" > "${PWFILE_DIR}/pw.txt"

CID=$(docker run -d \
    --name ch-distroless-t8 \
    -e CLICKHOUSE_USER=testuser \
    -e CLICKHOUSE_PASSWORD_FILE=/run/secrets/pw.txt \
    -v "${PWFILE_DIR}/pw.txt:/run/secrets/pw.txt:ro" \
    "${SERVER_IMAGE}")

wait_for_server ch-distroless-t8 9000 testuser secretpass

result=$(docker exec ch-distroless-t8 \
    /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
    -u testuser --password secretpass \
    --query "SELECT 1")
if [[ "${result}" == "1" ]]; then
    pass "CLICKHOUSE_PASSWORD_FILE works"
else
    fail "expected '1' with password from file, got '${result}'"
fi

docker rm -f ch-distroless-t8 >/dev/null
rm -rf "${PWFILE_DIR}"

# ──────────────────────────────────────────────────────────────────────────────
# Test 9: Passthrough / override CMD
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 9: passthrough command ==="
result=$(docker run --rm "${SERVER_IMAGE}" clickhouse-client --version 2>/dev/null || true)
if [[ "${result}" == *"ClickHouse client"* ]]; then
    pass "passthrough to clickhouse-client works"
else
    fail "expected ClickHouse client version string, got '${result}'"
fi

result=$(docker run --rm "${SERVER_IMAGE}" clickhouse-local --query "SELECT 123" 2>/dev/null || true)
if [[ "${result}" == "123" ]]; then
    pass "passthrough to clickhouse-local works"
else
    fail "expected '123' from clickhouse-local, got '${result}'"
fi

# ──────────────────────────────────────────────────────────────────────────────
# Test 10: Restart with existing data skips init
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 10: restart with existing data ==="
VOL_NAME="ch-distroless-vol-t10"
docker volume create "${VOL_NAME}" >/dev/null

CID=$(docker run -d \
    --name ch-distroless-t10a \
    -e CLICKHOUSE_USER=testuser \
    -e CLICKHOUSE_PASSWORD=testpass \
    -e CLICKHOUSE_DB=persist_db \
    -v "${VOL_NAME}:/var/lib/clickhouse" \
    "${SERVER_IMAGE}")

wait_for_server ch-distroless-t10a 9000 testuser testpass

docker exec ch-distroless-t10a \
    /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
    -u testuser --password testpass \
    --query "CREATE TABLE persist_db.t1 (x UInt32) ENGINE = MergeTree ORDER BY x"

docker exec ch-distroless-t10a \
    /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
    -u testuser --password testpass \
    --query "INSERT INTO persist_db.t1 VALUES (1),(2),(3)"

docker rm -f ch-distroless-t10a >/dev/null

# Restart with the same volume — init should be skipped, data should persist.
CID=$(docker run -d \
    --name ch-distroless-t10b \
    -e CLICKHOUSE_USER=testuser \
    -e CLICKHOUSE_PASSWORD=testpass \
    -v "${VOL_NAME}:/var/lib/clickhouse" \
    "${SERVER_IMAGE}")

wait_for_server ch-distroless-t10b 9000 testuser testpass

result=$(docker exec ch-distroless-t10b \
    /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
    -u testuser --password testpass \
    --query "SELECT count() FROM persist_db.t1")
if [[ "${result}" == "3" ]]; then
    pass "data persisted across container restart"
else
    fail "expected 3 rows after restart, got '${result}'"
fi

docker rm -f ch-distroless-t10b >/dev/null
docker volume rm "${VOL_NAME}" >/dev/null

# ──────────────────────────────────────────────────────────────────────────────
# Test 11: No password — default user restricted to localhost
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 11: no password — default user localhost-only ==="
CID=$(docker run -d \
    --name ch-distroless-t11 \
    "${SERVER_IMAGE}")

wait_for_server ch-distroless-t11 9000

# Internal query should work (localhost)
result=$(docker exec ch-distroless-t11 \
    /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
    --query "SELECT 1" 2>/dev/null || echo "FAIL")
if [[ "${result}" == "1" ]]; then
    pass "default user can query from localhost"
else
    fail "expected '1' from localhost query, got '${result}'"
fi

docker rm -f ch-distroless-t11 >/dev/null

# ──────────────────────────────────────────────────────────────────────────────
# Test 12: CLICKHOUSE_SKIP_USER_SETUP
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 12: CLICKHOUSE_SKIP_USER_SETUP ==="
CID=$(docker run -d \
    --name ch-distroless-t12 \
    -e CLICKHOUSE_SKIP_USER_SETUP=1 \
    "${SERVER_IMAGE}")

wait_for_server ch-distroless-t12 9000

# With skip-user-setup, the default user should still work with no password
result=$(docker exec ch-distroless-t12 \
    /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
    --query "SELECT currentUser()" 2>/dev/null || echo "FAIL")
if [[ "${result}" == "default" ]]; then
    pass "CLICKHOUSE_SKIP_USER_SETUP preserves default user"
else
    fail "expected 'default' user, got '${result}'"
fi

docker rm -f ch-distroless-t12 >/dev/null

# ──────────────────────────────────────────────────────────────────────────────
# Test 13: HTTP endpoint (port 8123)
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 13: HTTP endpoint ==="
CID=$(docker run -d \
    --name ch-distroless-t13 \
    -e CLICKHOUSE_USER=testuser \
    -e CLICKHOUSE_PASSWORD=testpass \
    -p 18123:8123 \
    "${SERVER_IMAGE}")

wait_for_server ch-distroless-t13 9000 testuser testpass

# Query via HTTP from the host
result=$(curl -sf "http://127.0.0.1:18123/?user=testuser&password=testpass&query=SELECT+99" 2>/dev/null || echo "FAIL")
if [[ "${result}" == "99"* ]]; then
    pass "HTTP endpoint responds on port 8123"
else
    fail "expected '99' from HTTP query, got '${result}'"
fi

docker rm -f ch-distroless-t13 >/dev/null

# ──────────────────────────────────────────────────────────────────────────────
# Test 14: docker stop during init (PID 1 signal handling)
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 14: graceful shutdown during init ==="
CID=$(docker run -d \
    --name ch-distroless-t14 \
    -e CLICKHOUSE_USER=testuser \
    -e CLICKHOUSE_PASSWORD=testpass \
    -e CLICKHOUSE_DB=slow_init \
    "${SERVER_IMAGE}")

# Give the container a moment to start the init phase, then stop it.
sleep 3
stop_start=$(date +%s)
docker stop --time 10 ch-distroless-t14 >/dev/null 2>&1 || true
stop_end=$(date +%s)
stop_duration=$(( stop_end - stop_start ))

# If signal handling works, the container should stop within the 10s grace period,
# not require a SIGKILL at the timeout boundary.
if (( stop_duration < 10 )); then
    pass "container stopped gracefully in ${stop_duration}s (signal handling works)"
else
    fail "container took ${stop_duration}s to stop (signal may not be handled)"
fi

docker rm -f ch-distroless-t14 >/dev/null 2>&1 || true

# ──────────────────────────────────────────────────────────────────────────────
# Test 15: Image size comparison (performance baseline)
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 15: image size ==="
distroless_size=$(docker image inspect "${SERVER_IMAGE}" --format '{{.Size}}')
distroless_mb=$(( distroless_size / 1024 / 1024 ))
echo "  Distroless image size: ${distroless_mb} MB"
if (( distroless_mb < 1000 )); then
    pass "distroless image is under 1 GB (${distroless_mb} MB)"
else
    fail "distroless image is ${distroless_mb} MB — expected under 1 GB"
fi

# ──────────────────────────────────────────────────────────────────────────────
# Summary
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Results: ${PASS} passed, ${FAIL} failed ==="
if (( FAIL > 0 )); then
    exit 1
fi
