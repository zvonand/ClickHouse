import json
import os
import re
import time

import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    default_upload_directory,
    get_uuid_str,
)


ICEBERG_SETTINGS = {
    "allow_insert_into_iceberg": 1,
    "allow_iceberg_remove_orphan_files": 1,
}
FAR_PAST = "2020-01-01 00:00:00"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_iceberg_metadata(instance, table_name):
    metadata_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/metadata"
    latest = instance.exec_in_container(
        ["bash", "-c", f"ls -v {metadata_dir}/v*.metadata.json | tail -1"]
    ).strip()
    raw = instance.exec_in_container(["cat", latest])
    return json.loads(raw), latest


def read_iceberg_metadata(instance, table_name):
    meta, _ = _read_iceberg_metadata(instance, table_name)
    return meta


def create_and_populate(cluster, instance, storage_type, table_name, n_rows, format_version=2):
    create_iceberg_table(
        storage_type, instance, table_name, cluster, "(x Int)", format_version
    )
    for val in range(1, n_rows + 1):
        instance.query(
            f"INSERT INTO {table_name} VALUES ({val});",
            settings=ICEBERG_SETTINGS,
        )


def remove_orphan_files(instance, table_name, **kwargs):
    args_parts = []
    if "older_than" in kwargs:
        args_parts.append(f"older_than = '{kwargs['older_than']}'")
    if "location" in kwargs:
        args_parts.append(f"location = '{kwargs['location']}'")
    if "dry_run" in kwargs:
        args_parts.append(f"dry_run = {kwargs['dry_run']}")
    if "positional_ts" in kwargs:
        args_str = f"'{kwargs['positional_ts']}'"
        if args_parts:
            args_str += ", " + ", ".join(args_parts)
    else:
        args_str = ", ".join(args_parts)

    result = instance.query(
        f"ALTER TABLE {table_name} EXECUTE remove_orphan_files({args_str});",
        settings=ICEBERG_SETTINGS,
    )
    return result


def parse_result(result):
    counts = {}
    for line in result.strip().split("\n"):
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) == 2:
            counts[parts[0]] = int(parts[1])
    return counts


def create_orphan_file(instance, table_name, subdir="data", filename="orphan-00000.parquet"):
    table_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"
    target_dir = f"{table_dir}/{subdir}"
    instance.exec_in_container(
        ["bash", "-c", f"mkdir -p {target_dir} && echo 'orphan_data' > {target_dir}/{filename}"]
    )


def create_orphan_metadata_file(instance, table_name, filename="orphan-v99.metadata.json"):
    table_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"
    target_dir = f"{table_dir}/metadata"
    instance.exec_in_container(
        ["bash", "-c", f"echo '{{}}' > {target_dir}/{filename}"]
    )


def file_exists(instance, table_name, subdir, filename):
    table_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"
    path = f"{table_dir}/{subdir}/{filename}"
    ret = instance.exec_in_container(
        ["bash", "-c", f"test -f {path} && echo 'exists' || echo 'missing'"]
    ).strip()
    return ret == "exists"


def list_files(instance, table_name):
    table_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"
    output = instance.exec_in_container(
        ["bash", "-c", f"find {table_dir} -type f 2>/dev/null | sort"]
    ).strip()
    if not output:
        return []
    return output.split("\n")


def assert_data_intact(instance, table_name, n_rows):
    expected = "".join(f"{i}\n" for i in range(1, n_rows + 1))
    assert instance.query(f"SELECT * FROM {table_name} ORDER BY x") == expected


def make_table_name(prefix, storage_type):
    return f"{prefix}_{storage_type}_{get_uuid_str()}"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("storage_type", ["local"])
def test_remove_orphan_files_basic(started_cluster_iceberg_with_spark, storage_type):
    """Create orphan files, run remove_orphan_files, verify they are removed
    and legitimate files are preserved."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_orphan_basic", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 3
    )

    create_orphan_file(instance, TABLE_NAME, "data", "orphan-data-001.parquet")
    create_orphan_file(instance, TABLE_NAME, "data", "orphan-data-002.parquet")

    time.sleep(2)

    files_before = list_files(instance, TABLE_NAME)
    assert any("orphan-data-001.parquet" in f for f in files_before)
    assert any("orphan-data-002.parquet" in f for f in files_before)

    result = remove_orphan_files(
        instance, TABLE_NAME, older_than=time.strftime("%Y-%m-%d %H:%M:%S")
    )
    counts = parse_result(result)
    assert len(counts) == 8, f"Expected 8 metrics, got {counts}"
    assert counts["deleted_data_files_count"] >= 2, f"Expected at least 2 deleted data files, got {counts}"

    assert not file_exists(instance, TABLE_NAME, "data", "orphan-data-001.parquet")
    assert not file_exists(instance, TABLE_NAME, "data", "orphan-data-002.parquet")

    assert_data_intact(instance, TABLE_NAME, 3)


@pytest.mark.parametrize("storage_type", ["local"])
def test_remove_orphan_files_no_orphans(started_cluster_iceberg_with_spark, storage_type):
    """When there are no orphan files, counts should all be zero."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_orphan_no_orphans", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 2
    )
    time.sleep(2)

    result = remove_orphan_files(
        instance, TABLE_NAME, older_than=time.strftime("%Y-%m-%d %H:%M:%S")
    )
    counts = parse_result(result)
    assert all(v == 0 for v in counts.values()), f"Expected all zeros, got {counts}"
    assert_data_intact(instance, TABLE_NAME, 2)


@pytest.mark.parametrize("storage_type", ["local"])
def test_remove_orphan_files_older_than(started_cluster_iceberg_with_spark, storage_type):
    """Orphan files newer than older_than threshold should be preserved."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_orphan_older_than", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 1
    )

    past_timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    time.sleep(2)

    create_orphan_file(instance, TABLE_NAME, "data", "orphan-new.parquet")
    time.sleep(1)

    result = remove_orphan_files(instance, TABLE_NAME, older_than=past_timestamp)
    counts = parse_result(result)
    assert file_exists(instance, TABLE_NAME, "data", "orphan-new.parquet"), \
        "Orphan newer than older_than should NOT be deleted"

    time.sleep(1)
    future_timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    result = remove_orphan_files(instance, TABLE_NAME, older_than=future_timestamp)
    counts = parse_result(result)
    assert counts["deleted_data_files_count"] >= 1
    assert not file_exists(instance, TABLE_NAME, "data", "orphan-new.parquet"), \
        "Orphan older than threshold should be deleted"

    assert_data_intact(instance, TABLE_NAME, 1)


@pytest.mark.parametrize("storage_type", ["local"])
def test_remove_orphan_files_dry_run(started_cluster_iceberg_with_spark, storage_type):
    """dry_run=1 should report counts but not delete files."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_orphan_dry_run", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 1
    )

    create_orphan_file(instance, TABLE_NAME, "data", "orphan-dry.parquet")
    time.sleep(2)

    result = remove_orphan_files(
        instance, TABLE_NAME,
        older_than=time.strftime("%Y-%m-%d %H:%M:%S"),
        dry_run=1,
    )
    counts = parse_result(result)
    assert counts["deleted_data_files_count"] >= 1, f"Expected at least 1 orphan reported, got {counts}"

    assert file_exists(instance, TABLE_NAME, "data", "orphan-dry.parquet"), \
        "dry_run should NOT delete files"

    result = remove_orphan_files(
        instance, TABLE_NAME,
        older_than=time.strftime("%Y-%m-%d %H:%M:%S"),
        dry_run=0,
    )
    counts = parse_result(result)
    assert counts["deleted_data_files_count"] >= 1
    assert not file_exists(instance, TABLE_NAME, "data", "orphan-dry.parquet"), \
        "Without dry_run, orphan should be deleted"


@pytest.mark.parametrize("storage_type", ["local"])
def test_remove_orphan_files_location(started_cluster_iceberg_with_spark, storage_type):
    """location parameter should restrict the scan to a subdirectory."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_orphan_location", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 1
    )

    create_orphan_file(instance, TABLE_NAME, "data", "orphan-data.parquet")
    create_orphan_metadata_file(instance, TABLE_NAME, "orphan-meta.metadata.json")
    time.sleep(2)

    now_ts = time.strftime("%Y-%m-%d %H:%M:%S")

    result = remove_orphan_files(instance, TABLE_NAME, older_than=now_ts, location="data/")
    counts = parse_result(result)
    assert counts["deleted_data_files_count"] >= 1

    assert not file_exists(instance, TABLE_NAME, "data", "orphan-data.parquet"), \
        "Data orphan in scanned location should be deleted"
    assert file_exists(instance, TABLE_NAME, "metadata", "orphan-meta.metadata.json"), \
        "Metadata orphan outside scanned location should survive"

    result = remove_orphan_files(instance, TABLE_NAME, older_than=now_ts, location="metadata/")
    counts = parse_result(result)
    assert counts["deleted_metadata_files_count"] >= 1
    assert not file_exists(instance, TABLE_NAME, "metadata", "orphan-meta.metadata.json"), \
        "Metadata orphan in scanned location should be deleted"

    assert_data_intact(instance, TABLE_NAME, 1)


@pytest.mark.parametrize("storage_type", ["local"])
def test_remove_orphan_files_many_orphans(started_cluster_iceberg_with_spark, storage_type):
    """remove_orphan_files should delete multiple orphan files in one run."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_orphan_concurrent", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 1
    )

    for i in range(10):
        create_orphan_file(instance, TABLE_NAME, "data", f"orphan-par-{i:03d}.parquet")
    time.sleep(2)

    result = remove_orphan_files(instance, TABLE_NAME, older_than=time.strftime("%Y-%m-%d %H:%M:%S"))
    counts = parse_result(result)
    assert counts["deleted_data_files_count"] >= 10

    for i in range(10):
        assert not file_exists(instance, TABLE_NAME, "data", f"orphan-par-{i:03d}.parquet")

    assert_data_intact(instance, TABLE_NAME, 1)


@pytest.mark.parametrize("storage_type", ["local"])
def test_remove_orphan_files_multiple_snapshots(started_cluster_iceberg_with_spark, storage_type):
    """Files referenced by any snapshot are not considered orphans."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_orphan_multi_snap", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 3
    )

    meta = read_iceberg_metadata(instance, TABLE_NAME)
    assert len(meta["snapshots"]) >= 3

    files_before = list_files(instance, TABLE_NAME)
    create_orphan_file(instance, TABLE_NAME, "data", "orphan-multi.parquet")
    time.sleep(2)

    result = remove_orphan_files(
        instance, TABLE_NAME, older_than=time.strftime("%Y-%m-%d %H:%M:%S")
    )
    counts = parse_result(result)
    assert counts["deleted_data_files_count"] >= 1

    assert not file_exists(instance, TABLE_NAME, "data", "orphan-multi.parquet")
    assert_data_intact(instance, TABLE_NAME, 3)


@pytest.mark.parametrize("storage_type", ["local"])
def test_remove_orphan_files_positional_arg(started_cluster_iceberg_with_spark, storage_type):
    """Positional older_than argument should work the same as named."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_orphan_positional", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 1
    )

    create_orphan_file(instance, TABLE_NAME, "data", "orphan-pos.parquet")
    time.sleep(2)

    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    result = remove_orphan_files(instance, TABLE_NAME, positional_ts=ts)
    counts = parse_result(result)
    assert counts["deleted_data_files_count"] >= 1
    assert not file_exists(instance, TABLE_NAME, "data", "orphan-pos.parquet")
    assert_data_intact(instance, TABLE_NAME, 1)


@pytest.mark.parametrize("storage_type", ["local"])
def test_remove_orphan_files_gate_setting(started_cluster_iceberg_with_spark, storage_type):
    """Without allow_iceberg_remove_orphan_files, the command should fail."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_orphan_gate", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 1
    )

    error = instance.query_and_get_error(
        f"ALTER TABLE {TABLE_NAME} EXECUTE remove_orphan_files();",
        settings={"allow_insert_into_iceberg": 1, "allow_iceberg_remove_orphan_files": 0},
    )
    assert "SUPPORT_IS_DISABLED" in error, f"Expected SUPPORT_IS_DISABLED error, got: {error}"


@pytest.mark.parametrize("storage_type", ["s3"])
def test_remove_orphan_files_s3(started_cluster_iceberg_with_spark, storage_type):
    """Basic orphan removal on S3 (MinIO) backend."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_orphan_s3", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 2
    )
    time.sleep(2)

    result = remove_orphan_files(
        instance, TABLE_NAME, older_than=time.strftime("%Y-%m-%d %H:%M:%S")
    )
    counts = parse_result(result)
    assert len(counts) == 8, f"Expected 8 metrics, got {counts}"
    assert all(v >= 0 for v in counts.values())
    assert_data_intact(instance, TABLE_NAME, 2)


@pytest.mark.parametrize("storage_type", ["azure"])
def test_remove_orphan_files_azure(started_cluster_iceberg_with_spark, storage_type):
    """Basic orphan removal on Azure (Azurite) backend."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_orphan_azure", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 2
    )
    time.sleep(2)

    result = remove_orphan_files(
        instance, TABLE_NAME, older_than=time.strftime("%Y-%m-%d %H:%M:%S")
    )
    counts = parse_result(result)
    assert len(counts) == 8, f"Expected 8 metrics, got {counts}"
    assert all(v >= 0 for v in counts.values())
    assert_data_intact(instance, TABLE_NAME, 2)
