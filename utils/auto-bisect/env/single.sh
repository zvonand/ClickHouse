#!/bin/bash
set -e

if [ -z "$1" ]; then
  echo "Usage: $0 <WORK_TREE>"
  exit 1
fi

WORK_TREE="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Check if the script is located in a directory named 'env'
if [ "$(basename "$SCRIPT_DIR")" = "env" ]; then
  SCRIPT_DIR="$(dirname "$SCRIPT_DIR")"
fi

CH_PATH=${CH_PATH:=$(command -v clickhouse || true)}

if [ -z "$CH_PATH" ] || [ ! -s "$CH_PATH" ]; then
  echo "Can't find clickhouse binary at '$CH_PATH'"
  exit 1
fi

source ${SCRIPT_DIR}/helpers/lib.sh

PID_FILE="$SCRIPT_DIR/data/ch.pid"
LOG_FILE="$SCRIPT_DIR/data/clickhouse.log"

(
  cd $WORK_TREE || exit 1
  echo "Installing ClickHouse config"

  rm -rf /etc/clickhouse-server/* /etc/clickhouse-client/*
  mkdir -p /etc/clickhouse-server/config.d /etc/clickhouse-server/users.d /etc/clickhouse-client

  # Copy base server configs, dereferencing symlinks so absolute paths are not needed
  cp -rL programs/server/. /etc/clickhouse-server/

  # Remove configs not needed for single-node bisect
  rm -f \
    /etc/clickhouse-server/config.d/keeper_port.xml \
    /etc/clickhouse-server/config.d/azure_storage_conf.xml \
    /etc/clickhouse-server/config.d/azure_storage_policy_by_default.xml \
    /etc/clickhouse-server/config.d/distributed_cache_server.xml \
    /etc/clickhouse-server/config.d/distributed_cache_client.xml

  # Overlay our bisect-specific user settings
  cp $SCRIPT_DIR/env/config/users_single.xml /etc/clickhouse-server/users.d/
)

set +e
# Something may write data during rm, causing "Directory not empty" — retry a few times
rm -rf $SCRIPT_DIR/data/ch
rm -rf $SCRIPT_DIR/data/ch
rm -rf $SCRIPT_DIR/data/ch
set -e
mkdir -p $SCRIPT_DIR/data/ch

kill -9 "$(cat $PID_FILE 2>/dev/null)" 2>/dev/null || true
rm -f $PID_FILE
# Kill all local ClickHouse servers
(ps aux | grep -E '[c]lickhouse[- ]server' | awk '{print $2}' | xargs kill -9) 2>/dev/null || true
sleep 1

(
  cd $SCRIPT_DIR/data/ch || exit 1
  echo "Starting ClickHouse"
  $CH_PATH server --config /etc/clickhouse-server/config.xml --pid-file=$PID_FILE \
    -- --path="$SCRIPT_DIR/data/ch" > "$LOG_FILE" 2>&1 &
)

wait_ch_start $CH_PATH 9000
