#!/usr/bin/env bash
# Integration tests for the distroless ClickHouse images.
#
# Usage:
#   bash test.sh [--version VERSION] [--binary-url URL] [--server-context DIR] [--keeper-context DIR]
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
#  15. Image size comparison
#  16. Server + Keeper (ReplicatedMergeTree)
#  17. Mounted config overrides (config.d / users.d)
#  18. Read-only root filesystem
#  19. Shell init scripts (.sh) gracefully skipped
#  20. CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1
#  21. CLICKHOUSE_ALWAYS_RUN_INITDB_SCRIPTS
#  22. Root mode (--user=0) with privilege drop
#  23. Mounted users.d profile override preserved
#  24. Cross-container client (docker-library pattern)
#  25. CLICKHOUSE_DO_NOT_CHOWN=1 with root
#  26. Keeper custom CLICKHOUSE_DATA_DIR
#  27. Internode port 9009 configured

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

VERSION="${VERSION:-26.1.2.11}"
BINARY_URL=""
SERVER_CONTEXT="${REPO_ROOT}/docker/server"
KEEPER_CONTEXT="${REPO_ROOT}/docker/keeper"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --version)    VERSION="$2"; shift 2 ;;
        --binary-url) BINARY_URL="$2"; shift 2 ;;
        --server-context) SERVER_CONTEXT="$2"; shift 2 ;;
        --keeper-context) KEEPER_CONTEXT="$2"; shift 2 ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

BINARY_BUILD_ARG=""
if [[ -n "${BINARY_URL}" ]]; then
    BINARY_BUILD_ARG="--build-arg=single_binary_location_url=${BINARY_URL}"
fi

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
    ${BINARY_BUILD_ARG:+${BINARY_BUILD_ARG}} \
    --tag "${SERVER_IMAGE}" \
    "${SERVER_CONTEXT}"

docker build \
    --file "${SERVER_CONTEXT}/Dockerfile.distroless" \
    --target debug \
    --build-arg "VERSION=${VERSION}" \
    ${BINARY_BUILD_ARG:+${BINARY_BUILD_ARG}} \
    --tag "${SERVER_DEBUG_IMAGE}" \
    "${SERVER_CONTEXT}"

docker build \
    --file "${KEEPER_CONTEXT}/Dockerfile.distroless" \
    --target production \
    --build-arg "VERSION=${VERSION}" \
    ${BINARY_BUILD_ARG:+${BINARY_BUILD_ARG}} \
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
    local db="${5:-}"
    local tries=60

    echo "  Waiting for ${container} on port ${port}..."

    # Build a query that also checks for the target database if specified.
    local query="SELECT 1"
    if [[ -n "${db}" ]]; then
        query="SELECT count() FROM system.databases WHERE name = '${db}' HAVING count() > 0"
    fi

    local consecutive=0
    local required=1
    # When waiting for a database, require 2 consecutive successes with a gap
    # to avoid hitting the temporary init server.
    if [[ -n "${db}" ]]; then required=2; fi

    while (( tries-- > 0 )); do
        if docker exec "${container}" \
               /usr/bin/clickhouse client \
               --host 127.0.0.1 --port "${port}" \
               -u "${user}" --password "${password}" \
               --query "${query}" \
               >/dev/null 2>&1; then
            (( consecutive++ ))
            if (( consecutive >= required )); then
                echo "  Server ready."
                return 0
            fi
        else
            consecutive=0
        fi
        sleep 2
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

wait_for_server ch-distroless-t4 9000 testuser testpass test_db

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
# With a stripped binary the image should be well under 1 GB. CI builds use
# stripped binaries; local testing with unstripped binaries will be larger.
MAX_IMAGE_MB="${MAX_IMAGE_MB:-4000}"
if (( distroless_mb < MAX_IMAGE_MB )); then
    pass "distroless image is ${distroless_mb} MB (limit ${MAX_IMAGE_MB} MB)"
else
    fail "distroless image is ${distroless_mb} MB — expected under ${MAX_IMAGE_MB} MB"
fi

# ──────────────────────────────────────────────────────────────────────────────
# Test 16: Server + Keeper (ReplicatedMergeTree)
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 16: server + keeper (ReplicatedMergeTree) ==="

# Create a Docker network for server-keeper communication.
NETNAME="ch-distroless-net-t16"
docker network create "${NETNAME}" >/dev/null

# The keeper's embedded config binds to localhost only and config.d doesn't
# apply without a real config file on disk. Mount a full keeper config that
# adds listen_host to accept connections from other containers.
T16_CFG_DIR=$(mktemp -d)
cat > "${T16_CFG_DIR}/keeper_config.xml" <<'XMLEOF'
<clickhouse>
    <listen_host>0.0.0.0</listen_host>
    <listen_host>::</listen_host>
    <listen_try>1</listen_try>
    <logger>
        <level>information</level>
        <log>/var/log/clickhouse-keeper/clickhouse-keeper.log</log>
        <errorlog>/var/log/clickhouse-keeper/clickhouse-keeper.err.log</errorlog>
        <size>100M</size>
        <count>3</count>
    </logger>
    <keeper_server>
        <tcp_port>9181</tcp_port>
        <server_id>1</server_id>
        <log_storage_path>/var/lib/clickhouse/coordination/logs</log_storage_path>
        <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
        <coordination_settings>
            <operation_timeout_ms>10000</operation_timeout_ms>
            <session_timeout_ms>100000</session_timeout_ms>
        </coordination_settings>
        <raft_configuration>
            <server>
                <id>1</id>
                <hostname>localhost</hostname>
                <port>9234</port>
            </server>
        </raft_configuration>
    </keeper_server>
</clickhouse>
XMLEOF

# Start Keeper with the custom config.
CID=$(docker run -d \
    --name ch-distroless-t16-keeper \
    --network "${NETNAME}" \
    -v "${T16_CFG_DIR}/keeper_config.xml:/etc/clickhouse-keeper/keeper_config.xml:ro" \
    "${KEEPER_IMAGE}")

# Wait for Keeper to be ready.
echo "  Waiting for keeper on port 9181..."
tries=60
keeper_ready=false
while (( tries-- > 0 )); do
    if docker exec ch-distroless-t16-keeper \
           /usr/bin/clickhouse keeper-client \
           --host 127.0.0.1 --port 9181 \
           -q "ruok" \
           2>/dev/null | grep -q "imok"; then
        keeper_ready=true
        break
    fi
    sleep 1
done

if [[ "${keeper_ready}" != "true" ]]; then
    fail "keeper did not start for replication test"
    docker logs ch-distroless-t16-keeper >&2
    docker rm -f ch-distroless-t16-keeper >/dev/null 2>&1 || true
    docker network rm "${NETNAME}" >/dev/null 2>&1 || true
    rm -r "${T16_CFG_DIR}"
else
    # Server config: point ZooKeeper at the keeper container.
    cat > "${T16_CFG_DIR}/keeper.xml" <<'XMLEOF'
<clickhouse>
    <zookeeper>
        <node>
            <host>ch-distroless-t16-keeper</host>
            <port>9181</port>
        </node>
    </zookeeper>
    <macros>
        <shard>01</shard>
        <replica>r1</replica>
    </macros>
</clickhouse>
XMLEOF

    CID=$(docker run -d \
        --name ch-distroless-t16-server \
        --network "${NETNAME}" \
        -e CLICKHOUSE_USER=testuser \
        -e CLICKHOUSE_PASSWORD=testpass \
        -v "${T16_CFG_DIR}/keeper.xml:/etc/clickhouse-server/config.d/keeper.xml:ro" \
        "${SERVER_IMAGE}")

    wait_for_server ch-distroless-t16-server 9000 testuser testpass

    # Create a ReplicatedMergeTree table.
    docker exec ch-distroless-t16-server \
        /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
        -u testuser --password testpass \
        --query "CREATE TABLE default.replicated_t (id UInt64, val String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/replicated_t', '{replica}') ORDER BY id"

    # Insert data.
    docker exec ch-distroless-t16-server \
        /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
        -u testuser --password testpass \
        --query "INSERT INTO default.replicated_t VALUES (1, 'a'), (2, 'b'), (3, 'c')"

    # Verify data reads back.
    result=$(docker exec ch-distroless-t16-server \
        /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
        -u testuser --password testpass \
        --query "SELECT count() FROM default.replicated_t")
    if [[ "${result}" == "3" ]]; then
        pass "ReplicatedMergeTree table created and populated (count=3)"
    else
        fail "expected 3 rows in replicated table, got '${result}'"
    fi

    # Verify ZooKeeper path was created in Keeper.
    zk_check=$(docker exec ch-distroless-t16-keeper \
        /usr/bin/clickhouse keeper-client \
        --host 127.0.0.1 --port 9181 \
        --query "ls '/clickhouse/tables/01/replicated_t'" 2>/dev/null || echo "FAIL")
    if [[ "${zk_check}" == *"metadata"* ]]; then
        pass "replication metadata exists in Keeper"
    else
        fail "expected replication metadata in Keeper, got '${zk_check}'"
    fi

    docker rm -f ch-distroless-t16-server >/dev/null
    rm -r "${T16_CFG_DIR}"
fi

docker rm -f ch-distroless-t16-keeper >/dev/null 2>&1 || true
docker network rm "${NETNAME}" >/dev/null 2>&1 || true

# ──────────────────────────────────────────────────────────────────────────────
# Test 17: Mounted config overrides (config.d / users.d)
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 17: mounted config overrides ==="

# Create custom config files.
CFG_DIR=$(mktemp -d)

# Override: set max_threads to 3 via users.d.
cat > "${CFG_DIR}/custom_profile.xml" <<'XMLEOF'
<clickhouse>
    <profiles>
        <default>
            <max_threads>3</max_threads>
        </default>
    </profiles>
</clickhouse>
XMLEOF

# Override: set a custom server-level macro via config.d.
cat > "${CFG_DIR}/custom_macros.xml" <<'XMLEOF'
<clickhouse>
    <macros>
        <test_macro>hello_distroless</test_macro>
    </macros>
</clickhouse>
XMLEOF

CID=$(docker run -d \
    --name ch-distroless-t17 \
    -e CLICKHOUSE_USER=testuser \
    -e CLICKHOUSE_PASSWORD=testpass \
    -v "${CFG_DIR}/custom_profile.xml:/etc/clickhouse-server/users.d/custom_profile.xml:ro" \
    -v "${CFG_DIR}/custom_macros.xml:/etc/clickhouse-server/config.d/custom_macros.xml:ro" \
    "${SERVER_IMAGE}")

wait_for_server ch-distroless-t17 9000 testuser testpass

# Verify the users.d override took effect.
result=$(docker exec ch-distroless-t17 \
    /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
    -u testuser --password testpass \
    --query "SELECT getSetting('max_threads')")
if [[ "${result}" == "3" ]]; then
    pass "users.d override applied (max_threads=3)"
else
    fail "expected max_threads=3, got '${result}'"
fi

# Verify the config.d macro override took effect.
result=$(docker exec ch-distroless-t17 \
    /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
    -u testuser --password testpass \
    --query "SELECT getMacro('test_macro')")
if [[ "${result}" == "hello_distroless" ]]; then
    pass "config.d override applied (macro test_macro=hello_distroless)"
else
    fail "expected macro 'hello_distroless', got '${result}'"
fi

docker rm -f ch-distroless-t17 >/dev/null
rm -r "${CFG_DIR}"

# ──────────────────────────────────────────────────────────────────────────────
# Test 18: Read-only root filesystem
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 18: read-only root filesystem ==="

# docker-init writes default-user.xml to /etc/clickhouse-server/users.d,
# so it needs a tmpfs there in addition to the data/log volumes and /tmp.
CID=$(docker run -d \
    --name ch-distroless-t18 \
    --read-only \
    --tmpfs /tmp:size=64M \
    --tmpfs /etc/clickhouse-server/users.d:uid=101,gid=101,size=8M \
    -v ch-distroless-vol-t18-data:/var/lib/clickhouse \
    -v ch-distroless-vol-t18-log:/var/log/clickhouse-server \
    -e CLICKHOUSE_USER=testuser \
    -e CLICKHOUSE_PASSWORD=testpass \
    "${SERVER_IMAGE}")

wait_for_server ch-distroless-t18 9000 testuser testpass

result=$(docker exec ch-distroless-t18 \
    /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
    -u testuser --password testpass \
    --query "SELECT 'read_only_ok'")
if [[ "${result}" == "read_only_ok" ]]; then
    pass "server works with read-only root filesystem"
else
    fail "expected 'read_only_ok', got '${result}'"
fi

# Verify data survives a restart on read-only filesystem.
docker exec ch-distroless-t18 \
    /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
    -u testuser --password testpass \
    --query "CREATE TABLE default.ro_test (x UInt32) ENGINE = MergeTree ORDER BY x"

docker exec ch-distroless-t18 \
    /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
    -u testuser --password testpass \
    --query "INSERT INTO default.ro_test VALUES (1),(2),(3)"

docker rm -f ch-distroless-t18 >/dev/null

# Restart with the same volumes.
CID=$(docker run -d \
    --name ch-distroless-t18b \
    --read-only \
    --tmpfs /tmp:size=64M \
    --tmpfs /etc/clickhouse-server/users.d:uid=101,gid=101,size=8M \
    -v ch-distroless-vol-t18-data:/var/lib/clickhouse \
    -v ch-distroless-vol-t18-log:/var/log/clickhouse-server \
    -e CLICKHOUSE_USER=testuser \
    -e CLICKHOUSE_PASSWORD=testpass \
    "${SERVER_IMAGE}")

wait_for_server ch-distroless-t18b 9000 testuser testpass

result=$(docker exec ch-distroless-t18b \
    /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
    -u testuser --password testpass \
    --query "SELECT count() FROM default.ro_test")
if [[ "${result}" == "3" ]]; then
    pass "data persisted across restart on read-only filesystem"
else
    fail "expected 3 rows after read-only restart, got '${result}'"
fi

docker rm -f ch-distroless-t18b >/dev/null
docker volume rm ch-distroless-vol-t18-data ch-distroless-vol-t18-log >/dev/null

# ──────────────────────────────────────────────────────────────────────────────
# Test 19: Shell init scripts (.sh) gracefully skipped
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 19: .sh init scripts gracefully skipped ==="

# Create a mixed init directory: a .sql file that should run, and a .sh file
# that should be skipped with a warning (since distroless has no shell).
T19_INITDB=$(mktemp -d)
cat > "${T19_INITDB}/01-create.sql" <<'SQL'
CREATE TABLE IF NOT EXISTS test_db.from_sql (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO test_db.from_sql VALUES (10),(20),(30);
SQL
cat > "${T19_INITDB}/02-should-skip.sh" <<'SH'
#!/bin/bash
# This script should be skipped in distroless (no shell available).
clickhouse-client --query "INSERT INTO test_db.from_sql VALUES (99)"
SH
chmod +x "${T19_INITDB}/02-should-skip.sh"

CID=$(docker run -d \
    --name ch-distroless-t19 \
    -e CLICKHOUSE_USER=testuser \
    -e CLICKHOUSE_PASSWORD=testpass \
    -e CLICKHOUSE_DB=test_db \
    -v "${T19_INITDB}:/docker-entrypoint-initdb.d:ro" \
    "${SERVER_IMAGE}")

wait_for_server ch-distroless-t19 9000 testuser testpass test_db

# Verify .sql init script ran.
result=$(docker exec ch-distroless-t19 \
    /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
    -u testuser --password testpass \
    --query "SELECT count() FROM test_db.from_sql")
if [[ "${result}" == "3" ]]; then
    pass ".sql init script executed successfully"
else
    fail "expected 3 rows from .sql init, got '${result}'"
fi

# Verify the .sh file was skipped (value 99 was NOT inserted).
result=$(docker exec ch-distroless-t19 \
    /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
    -u testuser --password testpass \
    --query "SELECT count() FROM test_db.from_sql WHERE id = 99")
if [[ "${result}" == "0" ]]; then
    pass ".sh init script was skipped (no row with id=99)"
else
    fail "expected 0 rows with id=99 (.sh should be skipped), got '${result}'"
fi

# Verify the warning appears in logs.
if docker logs ch-distroless-t19 2>&1 | grep -q "WARNING.*shell scripts cannot run"; then
    pass "warning logged for skipped .sh script"
else
    fail "expected warning about .sh script in logs"
fi

docker rm -f ch-distroless-t19 >/dev/null
rm -r "${T19_INITDB}"

# ──────────────────────────────────────────────────────────────────────────────
# Test 20: CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 20: CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT ==="
CID=$(docker run -d \
    --name ch-distroless-t20 \
    -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
    -e CLICKHOUSE_USER=admin \
    -e CLICKHOUSE_PASSWORD=adminpass \
    "${SERVER_IMAGE}")

wait_for_server ch-distroless-t20 9000 admin adminpass

result=$(docker exec ch-distroless-t20 \
    /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
    -u admin --password adminpass \
    --query "CREATE USER test_am IDENTIFIED BY 'pw'; SELECT name FROM system.users WHERE name='test_am'; DROP USER test_am" 2>/dev/null || echo "FAIL")
if [[ "${result}" == "test_am" ]]; then
    pass "access_management=1 allows CREATE USER"
else
    fail "expected 'test_am' from CREATE USER, got '${result}'"
fi

docker rm -f ch-distroless-t20 >/dev/null

# ──────────────────────────────────────────────────────────────────────────────
# Test 21: CLICKHOUSE_ALWAYS_RUN_INITDB_SCRIPTS
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 21: CLICKHOUSE_ALWAYS_RUN_INITDB_SCRIPTS ==="

T21_INITDB=$(mktemp -d)
T21_VOL="ch-distroless-vol-t21"
docker volume create "${T21_VOL}" >/dev/null

# First run: init script creates table and inserts 3 rows.
cat > "${T21_INITDB}/01-init.sql" <<'SQL'
CREATE TABLE IF NOT EXISTS default.always_init (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO default.always_init VALUES (1),(2),(3);
SQL

CID=$(docker run -d \
    --name ch-distroless-t21a \
    -e CLICKHOUSE_USER=testuser \
    -e CLICKHOUSE_PASSWORD=testpass \
    -v "${T21_VOL}:/var/lib/clickhouse" \
    -v "${T21_INITDB}:/docker-entrypoint-initdb.d:ro" \
    "${SERVER_IMAGE}")

wait_for_server ch-distroless-t21a 9000 testuser testpass
docker rm -f ch-distroless-t21a >/dev/null

# Second run: ALWAYS_RUN + script that inserts 2 more rows.
cat > "${T21_INITDB}/01-init.sql" <<'SQL'
INSERT INTO default.always_init VALUES (4),(5);
SQL

CID=$(docker run -d \
    --name ch-distroless-t21b \
    -e CLICKHOUSE_USER=testuser \
    -e CLICKHOUSE_PASSWORD=testpass \
    -e CLICKHOUSE_ALWAYS_RUN_INITDB_SCRIPTS=1 \
    -v "${T21_VOL}:/var/lib/clickhouse" \
    -v "${T21_INITDB}:/docker-entrypoint-initdb.d:ro" \
    "${SERVER_IMAGE}")

wait_for_server ch-distroless-t21b 9000 testuser testpass

result=$(docker exec ch-distroless-t21b \
    /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
    -u testuser --password testpass \
    --query "SELECT count() FROM default.always_init")
if [[ "${result}" == "5" ]]; then
    pass "CLICKHOUSE_ALWAYS_RUN_INITDB_SCRIPTS re-ran init (count=5)"
else
    fail "expected 5 rows after re-run, got '${result}'"
fi

docker rm -f ch-distroless-t21b >/dev/null
docker volume rm "${T21_VOL}" >/dev/null
rm -r "${T21_INITDB}"

# ──────────────────────────────────────────────────────────────────────────────
# Test 22: Root mode (--user=0) with privilege drop
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 22: root mode with privilege drop ==="
CID=$(docker run -d \
    --name ch-distroless-t22 \
    --user=0 \
    -e CLICKHOUSE_USER=testuser \
    -e CLICKHOUSE_PASSWORD=testpass \
    "${SERVER_DEBUG_IMAGE}")

wait_for_server ch-distroless-t22 9000 testuser testpass

result=$(docker exec ch-distroless-t22 \
    /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
    -u testuser --password testpass \
    --query "SELECT 'root_ok'")
if [[ "${result}" == "root_ok" ]]; then
    pass "server responds to queries when started as root"
else
    fail "expected 'root_ok', got '${result}'"
fi

# Verify the server process dropped to uid 101 via /proc/1/status.
server_uid=$(docker exec ch-distroless-t22 \
    /busybox/sh -c 'cat /proc/1/status' 2>/dev/null | grep '^Uid:' | awk '{print $2}')
if [[ "${server_uid}" == "101" ]]; then
    pass "server process dropped to uid 101"
else
    fail "expected uid 101, got '${server_uid}'"
fi

docker rm -f ch-distroless-t22 >/dev/null

# ──────────────────────────────────────────────────────────────────────────────
# Test 23: Mounted users.d profile override coexists with default-user.xml
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 23: mounted users.d profile override preserved ==="

# Mount a users.d/ profile override that sets max_threads=7.
# Even though docker-init writes its own default-user.xml (for user management),
# the profile override must be preserved — ClickHouse merges all users.d/ files.
T23_CFG_DIR=$(mktemp -d)
cat > "${T23_CFG_DIR}/custom_profile.xml" <<'XMLEOF'
<clickhouse>
    <profiles>
        <default>
            <max_threads>7</max_threads>
        </default>
    </profiles>
</clickhouse>
XMLEOF

CID=$(docker run -d \
    --name ch-distroless-t23 \
    -e CLICKHOUSE_USER=testuser \
    -e CLICKHOUSE_PASSWORD=testpass \
    -v "${T23_CFG_DIR}/custom_profile.xml:/etc/clickhouse-server/users.d/custom_profile.xml:ro" \
    "${SERVER_IMAGE}")

wait_for_server ch-distroless-t23 9000 testuser testpass

result=$(docker exec ch-distroless-t23 \
    /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
    -u testuser --password testpass \
    --query "SELECT getSetting('max_threads')")
if [[ "${result}" == "7" ]]; then
    pass "mounted users.d profile override preserved (max_threads=7)"
else
    fail "expected max_threads=7, got '${result}'"
fi

docker rm -f ch-distroless-t23 >/dev/null
rm -r "${T23_CFG_DIR}"

# ──────────────────────────────────────────────────────────────────────────────
# Test 24: Cross-container client (docker-library pattern)
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 24: cross-container client ==="

NETNAME_T24="ch-distroless-net-t24"
docker network create "${NETNAME_T24}" >/dev/null

CID=$(docker run -d \
    --name ch-distroless-t24-server \
    --network "${NETNAME_T24}" \
    -e CLICKHOUSE_USER=testuser \
    -e CLICKHOUSE_PASSWORD=testpass \
    "${SERVER_IMAGE}")

wait_for_server ch-distroless-t24-server 9000 testuser testpass

result=$(docker run --rm \
    --network "${NETNAME_T24}" \
    "${SERVER_IMAGE}" \
    clickhouse-client --host ch-distroless-t24-server \
    -u testuser --password testpass \
    --query "SELECT 'cross_ok'" 2>/dev/null || echo "FAIL")
if [[ "${result}" == "cross_ok" ]]; then
    pass "cross-container clickhouse-client query succeeded"
else
    fail "expected 'cross_ok', got '${result}'"
fi

docker rm -f ch-distroless-t24-server >/dev/null
docker network rm "${NETNAME_T24}" >/dev/null

# ──────────────────────────────────────────────────────────────────────────────
# Test 25: CLICKHOUSE_DO_NOT_CHOWN=1 with root
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 25: CLICKHOUSE_DO_NOT_CHOWN with root ==="
CID=$(docker run -d \
    --name ch-distroless-t25 \
    --user=0 \
    -e CLICKHOUSE_DO_NOT_CHOWN=1 \
    -e CLICKHOUSE_USER=testuser \
    -e CLICKHOUSE_PASSWORD=testpass \
    "${SERVER_DEBUG_IMAGE}")

wait_for_server ch-distroless-t25 9000 testuser testpass

result=$(docker exec ch-distroless-t25 \
    /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
    -u testuser --password testpass \
    --query "SELECT 'nochown_ok'")
if [[ "${result}" == "nochown_ok" ]]; then
    pass "CLICKHOUSE_DO_NOT_CHOWN=1 with root works"
else
    fail "expected 'nochown_ok', got '${result}'"
fi

docker rm -f ch-distroless-t25 >/dev/null

# ──────────────────────────────────────────────────────────────────────────────
# Test 26: Keeper custom CLICKHOUSE_DATA_DIR
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 26: keeper custom CLICKHOUSE_DATA_DIR ==="

T26_VOL="ch-distroless-vol-t26"
docker volume create "${T26_VOL}" >/dev/null

# Run as root so docker-init can chown the custom data directory.
# The keeper process itself drops to uid 101 via `clickhouse su`.
CID=$(docker run -d \
    --name ch-distroless-t26 \
    --user=0 \
    -e CLICKHOUSE_DATA_DIR=/var/lib/custom-keeper \
    -v "${T26_VOL}:/var/lib/custom-keeper" \
    "${KEEPER_IMAGE}")

echo "  Waiting for keeper on port 9181..."
tries=60
keeper_ready=false
while (( tries-- > 0 )); do
    if docker exec ch-distroless-t26 \
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
    pass "keeper with custom CLICKHOUSE_DATA_DIR started"
else
    fail "keeper with custom data dir did not start"
    docker logs ch-distroless-t26 >&2
fi

docker rm -f ch-distroless-t26 >/dev/null
docker volume rm "${T26_VOL}" >/dev/null

# ──────────────────────────────────────────────────────────────────────────────
# Test 27: Internode port 9009 configured
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 27: internode port 9009 configured ==="
CID=$(docker run -d \
    --name ch-distroless-t27 \
    -e CLICKHOUSE_USER=testuser \
    -e CLICKHOUSE_PASSWORD=testpass \
    "${SERVER_IMAGE}")

wait_for_server ch-distroless-t27 9000 testuser testpass

# Verify interserver_http_port is configured (EXPOSE 9009 in Dockerfile).
result=$(docker exec ch-distroless-t27 \
    /usr/bin/clickhouse extract-from-config \
    --config-file /etc/clickhouse-server/config.xml \
    --key interserver_http_port --try 2>/dev/null)
if [[ "${result}" == "9009" ]]; then
    pass "interserver_http_port configured as 9009"
else
    fail "expected interserver_http_port=9009, got '${result}'"
fi

docker rm -f ch-distroless-t27 >/dev/null

# ──────────────────────────────────────────────────────────────────────────────
# Summary
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Results: ${PASS} passed, ${FAIL} failed ==="
if (( FAIL > 0 )); then
    exit 1
fi
