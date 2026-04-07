import io
import logging
import time
from urllib import parse

import avro.schema
import pytest
from confluent_kafka.avro.cached_schema_registry_client import (
    CachedSchemaRegistryClient,
)
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer

from helpers.cluster import ClickHouseCluster, ClickHouseInstance, is_arm

# Skip on ARM due to Confluent/Kafka
if is_arm():
    pytestmark = pytest.mark.skip


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("dummy", with_kafka=True, with_secrets=True)
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def run_query(instance, query, data=None, settings=None):
    # type: (ClickHouseInstance, str, object, dict) -> str

    logging.info("Running query '{}'...".format(query))
    # use http to force parsing on server
    if not data:
        data = " "  # make POST request
    result = instance.http_query(query, data=data, params=settings)
    logging.info("Query finished")

    return result


def test_select(started_cluster):
    # type: (ClickHouseCluster) -> None

    reg_url = "http://localhost:{}".format(started_cluster.schema_registry_port)
    arg = {"url": reg_url}

    schema_registry_client = CachedSchemaRegistryClient(arg)
    serializer = MessageSerializer(schema_registry_client)

    schema = avro.schema.make_avsc_object(
        {
            "name": "test_record1",
            "type": "record",
            "fields": [{"name": "value", "type": "long"}],
        }
    )

    buf = io.BytesIO()
    for x in range(0, 3):
        message = serializer.encode_record_with_schema(
            "test_subject1", schema, {"value": x}
        )
        buf.write(message)
    data = buf.getvalue()

    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    schema_registry_url = "http://{}:{}".format(
        started_cluster.schema_registry_host, started_cluster.schema_registry_port
    )

    run_query(instance, "create table avro_data(value Int64) engine = Memory()")
    settings = {"format_avro_schema_registry_url": schema_registry_url}
    run_query(instance, "insert into avro_data format AvroConfluent", data, settings)
    stdout = run_query(instance, "select * from avro_data")
    assert list(map(str.split, stdout.splitlines())) == [
        ["0"],
        ["1"],
        ["2"],
    ]


def test_select_auth(started_cluster):
    # type: (ClickHouseCluster) -> None

    reg_url = "http://localhost:{}".format(started_cluster.schema_registry_auth_port)
    arg = {
        "url": reg_url,
        "basic.auth.credentials.source": "USER_INFO",
        "basic.auth.user.info": "schemauser:letmein",
    }

    schema_registry_client = CachedSchemaRegistryClient(arg)
    serializer = MessageSerializer(schema_registry_client)

    schema = avro.schema.make_avsc_object(
        {
            "name": "test_record_auth",
            "type": "record",
            "fields": [{"name": "value", "type": "long"}],
        }
    )

    buf = io.BytesIO()
    for x in range(0, 3):
        message = serializer.encode_record_with_schema(
            "test_subject_auth", schema, {"value": x}
        )
        buf.write(message)
    data = buf.getvalue()

    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    schema_registry_url = "http://{}:{}@{}:{}".format(
        "schemauser",
        "letmein",
        started_cluster.schema_registry_auth_host,
        started_cluster.schema_registry_auth_port,
    )

    run_query(instance, "create table avro_data_auth(value Int64) engine = Memory()")
    settings = {"format_avro_schema_registry_url": schema_registry_url}
    run_query(
        instance, "insert into avro_data_auth format AvroConfluent", data, settings
    )
    stdout = run_query(instance, "select * from avro_data_auth")
    assert list(map(str.split, stdout.splitlines())) == [
        ["0"],
        ["1"],
        ["2"],
    ]


def test_select_auth_encoded(started_cluster):
    # type: (ClickHouseCluster) -> None

    reg_url = "http://localhost:{}".format(started_cluster.schema_registry_auth_port)
    arg = {
        "url": reg_url,
        "basic.auth.credentials.source": "USER_INFO",
        "basic.auth.user.info": "schemauser:letmein",
    }

    schema_registry_client = CachedSchemaRegistryClient(arg)
    serializer = MessageSerializer(schema_registry_client)

    schema = avro.schema.make_avsc_object(
        {
            "name": "test_record_auth_encoded",
            "type": "record",
            "fields": [{"name": "value", "type": "long"}],
        }
    )

    buf = io.BytesIO()
    for x in range(0, 3):
        message = serializer.encode_record_with_schema(
            "test_subject_auth_encoded", schema, {"value": x}
        )
        buf.write(message)
    data = buf.getvalue()

    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    schema_registry_url = "http://{}:{}@{}:{}".format(
        parse.quote_plus("schemauser/slash"),
        parse.quote_plus("letmein"),
        started_cluster.schema_registry_auth_host,
        started_cluster.schema_registry_auth_port,
    )

    run_query(
        instance, "create table avro_data_auth_encoded(value Int64) engine = Memory()"
    )
    settings = {"format_avro_schema_registry_url": schema_registry_url}
    run_query(
        instance,
        "insert into avro_data_auth_encoded format AvroConfluent",
        data,
        settings,
    )
    stdout = run_query(instance, "select * from avro_data_auth_encoded")
    assert list(map(str.split, stdout.splitlines())) == [
        ["0"],
        ["1"],
        ["2"],
    ]


def test_output(started_cluster):
    # type: (ClickHouseCluster) -> None

    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    schema_registry_url = "http://{}:{}".format(
        started_cluster.schema_registry_host, started_cluster.schema_registry_port
    )

    # Write rows using AvroConfluent output format (registers schema automatically),
    # then read them back using AvroConfluent input format.
    run_query(instance, "create table avro_output_source(value Int64) engine = Memory()")
    run_query(instance, "insert into avro_output_source values (10),(20),(30)")

    settings = {
        "format_avro_schema_registry_url": schema_registry_url,
        "output_format_avro_confluent_subject": "test_output_subject",
    }
    data = run_query(
        instance, "select * from avro_output_source format AvroConfluent", settings=settings
    )

    # Feed the Confluent-framed binary back into a table via AvroConfluent input
    run_query(instance, "create table avro_output_sink(value Int64) engine = Memory()")
    settings_in = {"format_avro_schema_registry_url": schema_registry_url}
    run_query(
        instance,
        "insert into avro_output_sink format AvroConfluent",
        data,
        settings_in,
    )
    stdout = run_query(instance, "select * from avro_output_sink")
    assert list(map(str.split, stdout.splitlines())) == [
        ["10"],
        ["20"],
        ["30"],
    ]


def test_output_multiple_columns(started_cluster):
    # type: (ClickHouseCluster) -> None

    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    schema_registry_url = "http://{}:{}".format(
        started_cluster.schema_registry_host, started_cluster.schema_registry_port
    )

    run_query(
        instance,
        "create table avro_output_multi_source(id Int64, name String) engine = Memory()",
    )
    run_query(
        instance,
        "insert into avro_output_multi_source values (1,'alice'),(2,'bob'),(3,'charlie')",
    )

    settings = {
        "format_avro_schema_registry_url": schema_registry_url,
        "output_format_avro_confluent_subject": "test_output_multi_subject",
        "output_format_avro_string_column_pattern": "^name$",
    }
    data = run_query(
        instance,
        "select * from avro_output_multi_source format AvroConfluent",
        settings=settings,
    )

    run_query(
        instance,
        "create table avro_output_multi_sink(id Int64, name String) engine = Memory()",
    )
    settings_in = {"format_avro_schema_registry_url": schema_registry_url}
    run_query(
        instance,
        "insert into avro_output_multi_sink format AvroConfluent",
        data,
        settings_in,
    )
    stdout = run_query(instance, "select * from avro_output_multi_sink")
    assert list(map(str.split, stdout.splitlines())) == [
        ["1", "alice"],
        ["2", "bob"],
        ["3", "charlie"],
    ]


def test_select_auth_encoded_complex(started_cluster):
    # type: (ClickHouseCluster) -> None

    reg_url = "http://localhost:{}".format(started_cluster.schema_registry_auth_port)
    arg = {
        "url": reg_url,
        "basic.auth.credentials.source": "USER_INFO",
        "basic.auth.user.info": "schemauser:letmein",
    }

    schema_registry_client = CachedSchemaRegistryClient(arg)
    serializer = MessageSerializer(schema_registry_client)

    schema = avro.schema.make_avsc_object(
        {
            "name": "test_record_auth_encoded_complex",
            "type": "record",
            "fields": [{"name": "value", "type": "long"}],
        }
    )

    buf = io.BytesIO()
    for x in range(0, 3):
        message = serializer.encode_record_with_schema(
            "test_subject_auth_encoded_complex", schema, {"value": x}
        )
        buf.write(message)
    data = buf.getvalue()

    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    schema_registry_url = "http://{}:{}@{}:{}".format(
        parse.quote_plus("complexschemauser"),
        parse.quote_plus("letmein%@:/"),
        started_cluster.schema_registry_auth_host,
        started_cluster.schema_registry_auth_port,
    )

    run_query(
        instance,
        "create table avro_data_auth_encoded_complex(value Int64) engine = Memory()",
    )
    settings = {"format_avro_schema_registry_url": schema_registry_url}
    run_query(
        instance,
        "insert into avro_data_auth_encoded_complex format AvroConfluent",
        data,
        settings,
    )
    stdout = run_query(instance, "select * from avro_data_auth_encoded_complex")
    assert list(map(str.split, stdout.splitlines())) == [
        ["0"],
        ["1"],
        ["2"],
    ]
