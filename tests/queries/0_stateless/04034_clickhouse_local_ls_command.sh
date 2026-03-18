#!/usr/bin/env bash
# Test the `ls` command in clickhouse-local.
#
# `ls` is a client-side meta-command (not SQL). It lists files in the current
# working directory and is internally rewritten to a query using `file()`.
#
# The test checks:
# - `ls` works
# - `ls;` works
# - `ls` with arguments is not supported and results in an error.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

F1="ls_test_file_.1"
F2="ls_test_file_.2"
F3="ls_test_file_.3"

TESTDIR="$CURDIR/ls_test_dir"

cleanup() {
rm -rf "$TESTDIR"
}
trap cleanup EXIT

mkdir "$TESTDIR"

echo "-- prepare files"
touch "$TESTDIR/$F1" "$TESTDIR/$F2" "$TESTDIR/$F3"

echo "-- ls"
(
cd "$TESTDIR"
OUT=$($CLICKHOUSE_LOCAL -q "ls")
echo "$OUT" | grep -F "$F1"
echo "$OUT" | grep -F "$F2"
echo "$OUT" | grep -F "$F3"
)
echo "OK"

echo "-- ls;"
(
cd "$TESTDIR"
OUT=$($CLICKHOUSE_LOCAL -q "ls;")
echo "$OUT" | grep -F "$F1"
echo "$OUT" | grep -F "$F2"
echo "$OUT" | grep -F "$F3"
)
echo "OK"

echo "-- ls x"
(
cd "$TESTDIR"
OUT=$($CLICKHOUSE_LOCAL -q "ls x")
)
echo "Not OK"