import logging
import os

from helpers.iceberg_utils import get_uuid_str

AZURE_CONTAINER = "testcontainer"


def test_spark_write_and_read(started_cluster_iceberg):
    """Verify Spark can write to and read from Azurite via WASB emulator mode."""
    spark = started_cluster_iceberg.spark_session

    TABLE_NAME = "test_spark_roundtrip_" + get_uuid_str()

    # Write
    spark.sql(
        f"""
            CREATE TABLE {TABLE_NAME} (
                number INT
            )
            USING iceberg
            OPTIONS('format-version'='2');
        """
    )

    spark.sql(
        f"""
            INSERT INTO {TABLE_NAME}
            SELECT id as number FROM range(100)
        """
    )

    # Read back
    df = spark.sql(f"SELECT count(*) as cnt FROM {TABLE_NAME}").collect()
    count = df[0].cnt
    logging.info(f"Spark read back {count} rows")
    assert count == 100, f"Expected 100 rows, got {count}"

    # List blobs to see what paths Spark actually wrote
    blob_client = started_cluster_iceberg.blob_service_client
    container_client = blob_client.get_container_client(AZURE_CONTAINER)
    blobs = list(container_client.list_blobs())
    print(f"Blobs in container ({len(blobs)}):")
    for blob in blobs[:20]:
        print(f"  {blob.name}")

    assert len(blobs) > 0, "No blobs written to Azurite!"

    # Now try ClickHouse reading the same data
    instance = started_cluster_iceberg.instances["node1"]
    azurite_url = started_cluster_iceberg.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]

    # Try both with and without leading slash
    blob_prefix = blobs[0].name.split("/default/")[0] if "/default/" in blobs[0].name else "iceberg_data"
    table_path = f"{blob_prefix}/default/{TABLE_NAME}/"
    print(f"Using blob_path: {table_path}")

    instance.query(
        f"""
        CREATE TABLE {TABLE_NAME}
        ENGINE=IcebergAzure(azure,
            container = '{AZURE_CONTAINER}',
            storage_account_url = '{azurite_url}',
            blob_path = '{table_path}')
        """
    )

    rows = int(instance.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert rows == 100, f"Expected 100 rows, got {rows}"


def test_spark_write_ch_read_append(started_cluster_iceberg):
    """Spark writes, CH reads, Spark appends, CH reads updated data."""
    instance = started_cluster_iceberg.instances["node1"]
    spark = started_cluster_iceberg.spark_session

    TABLE_NAME = "test_spark_append_" + get_uuid_str()
    azurite_url = started_cluster_iceberg.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]
    blob_path = f"iceberg_data/default/{TABLE_NAME}/"

    # Spark creates the table and inserts initial data
    spark.sql(
        f"""
            CREATE TABLE {TABLE_NAME} (
                number INT
            )
            USING iceberg
            OPTIONS('format-version'='2');
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} SELECT id as number FROM range(100)")

    # Create ClickHouse table pointing to the same Azurite location
    instance.query(
        f"""
        CREATE TABLE {TABLE_NAME}
        ENGINE=IcebergAzure(azure,
            container = '{AZURE_CONTAINER}',
            storage_account_url = '{azurite_url}',
            blob_path = '{blob_path}')
        """
    )

    # CH reads Spark's data
    rows = int(instance.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert rows == 100, f"Expected 100 rows, got {rows}"

    result = instance.query(f"SELECT sum(number) FROM {TABLE_NAME}")
    assert int(result) == 4950, f"Expected sum 4950, got {result.strip()}"

    # Spark appends more data
    spark.sql(f"INSERT INTO {TABLE_NAME} SELECT id + 100 as number FROM range(50)")

    # CH reads the updated data
    rows = int(instance.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert rows == 150, f"Expected 150 rows after append, got {rows}"


def test_ch_write_spark_read_simple(started_cluster_iceberg):
    """Spark creates a table, CH appends data, Spark reads all data back."""
    instance = started_cluster_iceberg.instances["node1"]
    spark = started_cluster_iceberg.spark_session

    TABLE_NAME = "test_ch_write_simple_" + get_uuid_str()
    azurite_url = started_cluster_iceberg.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]
    blob_path = f"iceberg_data/default/{TABLE_NAME}/"

    # Spark creates the table and inserts initial data
    spark.sql(
        f"""
            CREATE TABLE {TABLE_NAME} (
                number INT
            )
            USING iceberg
            OPTIONS('format-version'='2');
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} SELECT id as number FROM range(10)")

    # ClickHouse table pointing to the same Azurite location
    instance.query(
        f"""
        CREATE TABLE {TABLE_NAME}
        ENGINE=IcebergAzure(azure,
            container = '{AZURE_CONTAINER}',
            storage_account_url = '{azurite_url}',
            blob_path = '{blob_path}')
        SETTINGS iceberg_use_version_hint = 1
        """
    )

    # CH reads Spark's data
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 10

    # CH writes more data
    insert_settings = {
        "allow_insert_into_iceberg": 1,
        "write_full_path_in_iceberg_metadata": 1,
    }
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (42)", settings=insert_settings)
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (123)", settings=insert_settings)

    # CH reads its own writes
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 12
    assert int(instance.query(f"SELECT sum(number) FROM {TABLE_NAME}")) == sum(range(10)) + 42 + 123

    # Spark reads all data (restart session to clear HadoopCatalog metadata cache)
    started_cluster_iceberg.spark_session._restart()
    spark = started_cluster_iceberg.spark_session

    df = spark.sql(f"SELECT * FROM {TABLE_NAME}").collect()
    assert len(df) == 12, f"Spark expected 12 rows, got {len(df)}"
    spark_values = sorted([row.number for row in df])
    assert 42 in spark_values
    assert 123 in spark_values


def test_ch_write_spark_read(started_cluster_iceberg):
    """ClickHouse writes to an Iceberg table on Azurite that Spark created.
    Tests that CH can correctly resolve Spark's wasb:// metadata paths and
    append new data while preserving the existing Spark-written data."""
    instance = started_cluster_iceberg.instances["node1"]
    spark = started_cluster_iceberg.spark_session

    TABLE_NAME = "test_ch_write_" + get_uuid_str()
    azurite_url = started_cluster_iceberg.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]
    blob_path = f"iceberg_data/default/{TABLE_NAME}/"

    # Spark creates the table and inserts initial data
    spark.sql(
        f"""
            CREATE TABLE {TABLE_NAME} (
                number INT
            )
            USING iceberg
            OPTIONS('format-version'='2');
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} SELECT id as number FROM range(10)")

    # Create ClickHouse table pointing to the same Azurite location.
    # iceberg_use_version_hint writes version-hint.text so Spark's HadoopCatalog
    # can discover the latest metadata version after session restart.
    instance.query(
        f"""
        CREATE TABLE {TABLE_NAME}
        ENGINE=IcebergAzure(azure,
            container = '{AZURE_CONTAINER}',
            storage_account_url = '{azurite_url}',
            blob_path = '{blob_path}')
        SETTINGS iceberg_use_version_hint = 1
        """
    )

    # CH reads Spark's data
    rows = int(instance.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert rows == 10, f"Expected 10 rows from Spark, got {rows}"

    # ClickHouse writes more data.
    # write_full_path_in_iceberg_metadata is needed so Spark can resolve the
    # data file paths (Spark expects wasb:// URIs, not relative paths).
    insert_settings = {
        "allow_insert_into_iceberg": 1,
        "write_full_path_in_iceberg_metadata": 1,
    }
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (42)", settings=insert_settings)
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (123)", settings=insert_settings)

    # ClickHouse can read its own writes (10 from Spark + 2 from CH)
    rows = int(instance.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert rows == 12, f"Expected 12 rows, got {rows}"

    result = instance.query(f"SELECT sum(number) FROM {TABLE_NAME}")
    expected_sum = sum(range(10)) + 42 + 123  # 45 + 42 + 123 = 210
    assert int(result) == expected_sum, f"Expected sum {expected_sum}, got {result.strip()}"

    # List all blobs to see what CH wrote
    blob_client = started_cluster_iceberg.blob_service_client
    container_client = blob_client.get_container_client(AZURE_CONTAINER)
    blobs = sorted([b.name for b in container_client.list_blobs(name_starts_with=blob_path)])
    logging.info(f"Blobs after CH writes ({len(blobs)}):")
    for b in blobs:
        logging.info(f"  {b}")

    # Check version-hint content
    version_hint_path = f"{blob_path}metadata/version-hint.text"
    try:
        hint_blob = container_client.get_blob_client(version_hint_path)
        hint_content = hint_blob.download_blob().readall().decode()
        logging.info(f"version-hint.text content: '{hint_content}'")
    except Exception as e:
        logging.info(f"version-hint.text not found or error: {e}")

    # Dump key fields from v4.metadata.json
    import json as json_mod
    v4_path = f"{blob_path}metadata/v4.metadata.json"
    try:
        v4_blob = container_client.get_blob_client(v4_path)
        v4_content = v4_blob.download_blob().readall().decode()
        v4 = json_mod.loads(v4_content)
        logging.info(f"v4 location: {v4.get('location')}")
        logging.info(f"v4 current-snapshot-id: {v4.get('current-snapshot-id')}")
        for snap in v4.get("snapshots", []):
            logging.info(f"  snapshot {snap.get('snapshot-id')}: manifest-list={snap.get('manifest-list')}")
    except Exception as e:
        logging.info(f"v4.metadata.json error: {e}")

    # Spark should also see the data written by ClickHouse.
    # Read via direct path to bypass catalog caching entirely.
    started_cluster_iceberg.spark_session._restart()
    spark = started_cluster_iceberg.spark_session

    wasb_table_path = (
        f"wasb://{AZURE_CONTAINER}@devstoreaccount1.blob.core.windows.net/{blob_path}"
    )
    # Try reading snapshots metadata to see which version Spark loaded
    try:
        snapshots_df = spark.read.format("iceberg").load(f"{wasb_table_path}#snapshots").collect()
        logging.info(f"Spark sees {len(snapshots_df)} snapshots")
        for s in snapshots_df:
            logging.info(f"  snapshot: {s}")
    except Exception as e:
        logging.info(f"Cannot read snapshots: {e}")

    # Try reading individual parquet files to check if Spark can access CH-written data
    for b in blobs:
        if b.endswith(".parquet"):
            wasb_file = f"wasb://{AZURE_CONTAINER}@devstoreaccount1.blob.core.windows.net/{b}"
            try:
                pq_df = spark.read.parquet(wasb_file)
                pq_rows = pq_df.collect()
                logging.info(f"Parquet {b}: {len(pq_rows)} rows, schema={pq_df.schema}, values={pq_rows}")
            except Exception as e:
                logging.info(f"Parquet {b}: FAILED - {e}")

    # Use Spark to read manifest lists as avro files directly
    snap_blobs = [b for b in blobs if "snap-" in b and b.endswith(".avro")]
    for snap_blob in snap_blobs:
        wasb_snap = f"wasb://{AZURE_CONTAINER}@devstoreaccount1.blob.core.windows.net/{snap_blob}"
        try:
            avro_df = spark.read.format("avro").load(wasb_snap)
            rows = avro_df.collect()
            logging.info(f"ManifestList {snap_blob}: {len(rows)} entries")
            for r in rows:
                logging.info(f"  entry: {r.asDict()}")
        except Exception as e:
            logging.info(f"ManifestList {snap_blob}: FAILED - {e}")

    # Read CH-written manifest files (not manifest lists) as avro
    manifest_blobs = [b for b in blobs if b.endswith(".avro") and "snap-" not in b and "/metadata/" in b]
    for manifest_blob in manifest_blobs:
        wasb_manifest = f"wasb://{AZURE_CONTAINER}@devstoreaccount1.blob.core.windows.net/{manifest_blob}"
        try:
            avro_df = spark.read.format("avro").load(wasb_manifest)
            rows = avro_df.collect()
            logging.info(f"ManifestFile {manifest_blob}: {len(rows)} entries, schema={avro_df.schema.simpleString()}")
            for r in rows:
                logging.info(f"  data_file entry: {r.asDict()}")
        except Exception as e:
            logging.info(f"ManifestFile {manifest_blob}: FAILED - {e}")

    # Read with each snapshot to isolate which one is broken
    for snap in snapshots_df:
        sid = snap.snapshot_id
        try:
            snap_df = spark.read.format("iceberg").option("snapshot-id", sid).load(wasb_table_path).collect()
            logging.info(f"Snapshot {sid}: {len(snap_df)} rows")
        except Exception as e:
            logging.info(f"Snapshot {sid}: FAILED - {e}")

    df = spark.read.format("iceberg").load(wasb_table_path).collect()

    # Dump Spark log for Iceberg debug info
    spark_log = os.path.join(started_cluster_iceberg.instances_dir, "spark.log")
    if os.path.exists(spark_log):
        with open(spark_log) as f:
            for line in f:
                if "iceberg" in line.lower() or "manifest" in line.lower() or "scan" in line.lower():
                    logging.info(f"SPARK: {line.rstrip()}")

    assert len(df) == 12, f"Spark expected 12 rows, got {len(df)}"

    spark_values = sorted([row.number for row in df])
    assert 42 in spark_values, f"Spark missing CH-written value 42: {spark_values}"
    assert 123 in spark_values, f"Spark missing CH-written value 123: {spark_values}"
