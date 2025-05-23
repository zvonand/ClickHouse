---
description: 'System table containing profiling information on the processors level
  (which can be found in `EXPLAIN PIPELINE`)'
keywords: ['system table', 'processors_profile_log', 'EXPLAIN PIPELINE']
slug: /operations/system-tables/processors_profile_log
title: 'system.processors_profile_log'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

# system.processors_profile_log

<SystemTableCloud/>

This table contains profiling on processors level (that you can find in [`EXPLAIN PIPELINE`](../../sql-reference/statements/explain.md#explain-pipeline)).

Columns:

- `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Hostname of the server executing the query.
- `event_date` ([Date](../../sql-reference/data-types/date.md)) — The date when the event happened.
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — The date and time when the event happened.
- `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — The date and time with microseconds precision when the event happened.
- `id` ([UInt64](../../sql-reference/data-types/int-uint.md)) — ID of processor
- `parent_ids` ([Array(UInt64)](../../sql-reference/data-types/array.md)) — Parent processors IDs
- `plan_step` ([UInt64](../../sql-reference/data-types/int-uint.md)) — ID of the query plan step which created this processor. The value is zero if the processor was not added from any step.
- `plan_group` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Group of the processor if it was created by query plan step. A group is a logical partitioning of processors added from the same query plan step. Group is used only for beautifying the result of EXPLAIN PIPELINE result.
- `initial_query_id` ([String](../../sql-reference/data-types/string.md)) — ID of the initial query (for distributed query execution).
- `query_id` ([String](../../sql-reference/data-types/string.md)) — ID of the query
- `name` ([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md)) — Name of the processor.
- `elapsed_us` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Number of microseconds this processor was executed.
- `input_wait_elapsed_us` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Number of microseconds this processor was waiting for data (from other processor).
- `output_wait_elapsed_us` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Number of microseconds this processor was waiting because output port was full.
- `input_rows` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The number of rows consumed by processor.
- `input_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The number of bytes consumed by processor.
- `output_rows` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The number of rows generated by processor.
- `output_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The number of bytes generated by processor.
**Example**

Query:

```sql
EXPLAIN PIPELINE
SELECT sleep(1)
┌─explain─────────────────────────┐
│ (Expression)                    │
│ ExpressionTransform             │
│   (SettingQuotaAndLimits)       │
│     (ReadFromStorage)           │
│     SourceFromSingleChunk 0 → 1 │
└─────────────────────────────────┘

SELECT sleep(1)
SETTINGS log_processors_profiles = 1
Query id: feb5ed16-1c24-4227-aa54-78c02b3b27d4
┌─sleep(1)─┐
│        0 │
└──────────┘
1 rows in set. Elapsed: 1.018 sec.

SELECT
    name,
    elapsed_us,
    input_wait_elapsed_us,
    output_wait_elapsed_us
FROM system.processors_profile_log
WHERE query_id = 'feb5ed16-1c24-4227-aa54-78c02b3b27d4'
ORDER BY name ASC
```

Result:

```text
┌─name────────────────────┬─elapsed_us─┬─input_wait_elapsed_us─┬─output_wait_elapsed_us─┐
│ ExpressionTransform     │    1000497 │                  2823 │                    197 │
│ LazyOutputFormat        │         36 │               1002188 │                      0 │
│ LimitsCheckingTransform │         10 │               1002994 │                    106 │
│ NullSource              │          5 │               1002074 │                      0 │
│ NullSource              │          1 │               1002084 │                      0 │
│ SourceFromSingleChunk   │         45 │                  4736 │                1000819 │
└─────────────────────────┴────────────┴───────────────────────┴────────────────────────┘
```

Here you can see:

- `ExpressionTransform` was executing `sleep(1)` function, so it `work` will takes 1e6, and so `elapsed_us` > 1e6.
- `SourceFromSingleChunk` need to wait, because `ExpressionTransform` does not accept any data during execution of `sleep(1)`, so it will be in `PortFull` state for 1e6 us, and so `output_wait_elapsed_us` > 1e6.
- `LimitsCheckingTransform`/`NullSource`/`LazyOutputFormat` need to wait until `ExpressionTransform` will execute `sleep(1)` to process the result, so `input_wait_elapsed_us` > 1e6.

**See Also**

- [`EXPLAIN PIPELINE`](../../sql-reference/statements/explain.md#explain-pipeline)
