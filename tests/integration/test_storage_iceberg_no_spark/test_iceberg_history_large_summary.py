"""Reproducer for https://github.com/ClickHouse/ClickHouse/issues/94176.

`system.iceberg_history` parses snapshot summary fields (`added-data-files`,
`added-records`, `added-files-size`, `changed-partition-count`) as `Int32`,
even though writers such as Spark/Databricks routinely produce values that
exceed `INT32_MAX` (e.g. a multi-GB `added-files-size`). Reading such a
table causes a `Not a valid integer` exception, the table is logged as
broken and skipped, and `system.iceberg_history` returns no rows for it.
"""

import json
import re

import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
)


INSERT_SETTINGS = {"allow_insert_into_iceberg": 1}


def _metadata_dir(table_name):
    return f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/metadata"


def _read_latest_metadata(instance, table_name):
    metadata_dir = _metadata_dir(table_name)
    latest = instance.exec_in_container(
        ["bash", "-c", f"ls -v {metadata_dir}/v*.metadata.json | tail -1"]
    ).strip()
    raw = instance.exec_in_container(["cat", latest])
    return json.loads(raw), latest


def _write_next_metadata(instance, table_name, meta, prev_path):
    metadata_dir = _metadata_dir(table_name)
    version_match = re.search(r"/v(\d+)[^/]*\.metadata\.json$", prev_path)
    new_version = int(version_match.group(1)) + 1
    new_path = f"{metadata_dir}/v{new_version}.metadata.json"
    new_content = json.dumps(meta, indent=4)
    instance.exec_in_container(
        ["bash", "-c", f"cat > {new_path} << 'JSONEOF'\n{new_content}\nJSONEOF"]
    )


@pytest.mark.parametrize("format_version", [1, 2])
def test_iceberg_history_summary_overflow(started_cluster_iceberg_no_spark, format_version):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_iceberg_history_summary_overflow_" + get_uuid_str()

    create_iceberg_table(
        "local",
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(x Int)",
        format_version,
    )
    instance.query(f"INSERT INTO {table_name} VALUES (1);", settings=INSERT_SETTINGS)

    meta, prev = _read_latest_metadata(instance, table_name)
    assert meta.get("snapshots"), "snapshot must be present after INSERT"

    # The exact value from the bug report; > INT32_MAX (2147483647).
    huge = "6986350573"
    for snap in meta["snapshots"]:
        summary = snap.setdefault("summary", {})
        summary["added-data-files"] = huge
        summary["added-records"] = huge
        summary["added-files-size"] = huge
        summary["changed-partition-count"] = huge

    _write_next_metadata(instance, table_name, meta, prev)

    count = instance.query(
        f"SELECT count() FROM system.iceberg_history "
        f"WHERE database = 'default' AND table = '{table_name}'"
    ).strip()
    assert count == "1", (
        f"system.iceberg_history must return the snapshot even when summary "
        f"values exceed INT32_MAX, got count={count}. See issue #94176."
    )
