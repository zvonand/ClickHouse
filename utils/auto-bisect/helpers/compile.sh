#!/bin/bash

set -e
export SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source ${SCRIPT_DIR}/lib.sh

if [[ -z "$GIT_WORK_TREE" || -z "$BUILD_DIR" ]]; then
    echo "Either GIT_WORK_TREE or BUILD_DIR is empty" >&2
    exit 1
fi

if [ -z "$1" ]; then
  echo "COMMIT_SHA is not provided. Correct usage: $0 <COMMIT_SHA>"
  exit 1
fi

COMMIT_SHA="$1"

# First try to find this in cache.
if $SCRIPT_DIR/cache.sh has $COMMIT_SHA; then
  echo "Found binary in cache for commit ${COMMIT_SHA}"
  $SCRIPT_DIR/cache.sh get $CH_PATH $COMMIT_SHA;
  exit 0;
fi

echo "Will compile the binary from ${COMMIT_SHA}."
cd $GIT_WORK_TREE

(git reset --hard $COMMIT_SHA) > /dev/null 2>&1
(git clean -xfd) > /dev/null 2>&1
(git submodule sync --recursive && git submodule update --init --recursive && git submodule foreach git reset --hard && git submodule foreach git clean -xfd) > /dev/null 2>&1

cd $BUILD_DIR
# Disabling RUST as it is very unstable.
(cmake -DCMAKE_C_COMPILER=/usr/bin/clang-19 -DCMAKE_CXX_COMPILER=/usr/bin/clang++-19 -DCMAKE_BUILD_TYPE=Debug -DENABLE_THINLTO=0 -DENABLE_RUST=0 -DENABLE_TESTS=0 -DENABLE_EXAMPLES=0 $GIT_WORK_TREE) > /dev/null 2>&1

# Sometimes the build may fail. In this case we stop the process,
# ask the user to fix the source code and retry.
run_with_retry "ninja"

(git stash) > /dev/null 2>&1
(git clean -xfd) > /dev/null 2>&1

strip --strip-unneeded $BUILD_DIR/programs/clickhouse
$SCRIPT_DIR/cache.sh add $BUILD_DIR/programs/clickhouse $COMMIT_SHA;
$SCRIPT_DIR/cache.sh get $CH_PATH $COMMIT_SHA;

chmod +x $CH_PATH
$CH_PATH --query 'SELECT 1'

exit 0
