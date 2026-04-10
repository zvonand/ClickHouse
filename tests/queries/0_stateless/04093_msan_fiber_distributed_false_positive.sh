#!/usr/bin/env bash
# Tags: no-fasttest, no-asan, no-tsan, no-ubsan
# Tag no-fasttest: needs remote() with multiple addresses
# Tags no-asan, no-tsan, no-ubsan: this test is specifically for MSan on ARM

# Reproducer for ARM MSan false positive in ConnectionPoolWithFailover::getMany.
# The error manifests as "use-of-uninitialized-value" inside vector::erase
# called from erase_if in PoolWithFailoverBase::getMany, running on a fiber
# (AsyncTaskExecutor → boost::context).
#
# The root cause: on the fiber's heap-allocated stack, compiler-generated
# temporaries from inlined functions (e.g. getLogger returning shared_ptr)
# leave dirty MSan shadow from struct padding. This shadow persists across
# function calls on the fiber stack and contaminates later stack allocations,
# such as the vector<TryResult> in getMany.
#
# The test runs concurrent distributed queries via remote() which go through
# RemoteQueryExecutor → ConnectionPoolWithFailover on fibers.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

for _ in $(seq 1 2000); do
    ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM remote('127.0.0.{1,2}', numbers(100)) FORMAT Null" &
    ${CLICKHOUSE_CLIENT} -q "SELECT sum(number) FROM remote('127.0.0.{1,2}', numbers(500)) GROUP BY number % 10 FORMAT Null" &
    ${CLICKHOUSE_CLIENT} -q "SELECT number % 3, sum(number) FROM remote('127.0.0.{1,2}', numbers(100)) GROUP BY ROLLUP(number % 3) SETTINGS group_by_use_nulls=1 FORMAT Null" &
    ${CLICKHOUSE_CLIENT} -q "SELECT uniq(number) FROM remote('127.0.0.{1,2}', numbers(1000)) FORMAT Null" &

    # Keep max 16 concurrent queries
    if (( _ % 4 == 0 )); then wait; fi
done

wait

echo "OK"
