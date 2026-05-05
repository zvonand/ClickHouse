from helpers.kafka.common_direct import *
from helpers.kafka.common_direct import _VarintBytes
import helpers.kafka.common as k
import os

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka.xml", "configs/named_collection.xml", "configs/placement.xml"],
    user_configs=["configs/users.xml"],
    with_kafka=True,
    with_minio=True,  # needed for resolver image only
    with_zookeeper=True,  # needed for Kafka2
    env_variables={
        "AWS_EC2_METADATA_SERVICE_ENDPOINT": "http://resolver:8080",
    },
    clickhouse_path_dir="clickhouse_path",
)

# Runs custom python-based S3 endpoint.
def run_endpoint(cluster):
    logging.info("Starting metadata mock services")
    container_id = cluster.get_container_id("resolver")
    current_dir = os.path.dirname(__file__)
    cluster.copy_file_to_container(
        container_id,
        os.path.join(current_dir, "s3_endpoint", "endpoint.py"),
        "endpoint.py",
    )
    cluster.exec_in_container(
        container_id,
        ["python", "endpoint.py", "--service", "aws", "--port", "8080"],
        detach=True,
    )
    cluster.exec_in_container(
        container_id,
        ["python", "endpoint.py", "--service", "gcp", "--port", "80"],
        detach=True,
    )

    for port in [8080, 80]:
        num_attempts = 100
        for attempt in range(num_attempts):
            logging.info(f"waiting for metadata service on port {port}, attempt {attempt}")
            ping_response = cluster.exec_in_container(
                container_id,
                ["curl", "-s", f"http://localhost:{port}/ping"],
                nothrow=True,
            )
            if ping_response == "OK":
                break
            if attempt == num_attempts - 1:
                assert ping_response == "OK", 'Expected "OK", but got "{}"'.format(
                    ping_response
                )
            time.sleep(1)

    logging.info("Metadata mock services started")

    # `getGCPAvailabilityZoneOrException` is hardcoded to `metadata.google.internal`.
    # Overriding `/etc/hosts` here keeps the test aligned with the production path
    # instead of introducing a test-only dependency on a hypothetical `GCE_METADATA_HOST`.
    instance.append_hosts("metadata.google.internal", cluster.get_instance_ip("resolver"))


def produce_test_messages(kafka_cluster, topic_name):
    k.kafka_produce(
        kafka_cluster,
        topic_name,
        [
            '{"t": 123, "e": {"x": "woof"} }',
            '{"t": 123, "e": {"x": "woof"} }',
            '{"t": 124, "e": {"x": "test"} }',
        ],
    )


def create_kafka_mv(create_query_generator, kafka_table, topic_name, autodetect_facility):
    instance.query(
        f"""
        CREATE TABLE test.persistent_{kafka_table} (
            time UInt64,
            some_string String
        )
        ENGINE = MergeTree()
        ORDER BY time;

        {create_query_generator(
            kafka_table,
            "t UInt64, `e.x` String",
            brokers='kafka1:19092',
            topic_list=topic_name,
            consumer_group=topic_name,
            format='JSONEachRow',
            settings={
                'kafka_autodetect_client_rack': autodetect_facility,
                'input_format_import_nested_json': 1,
            },
        )};

        CREATE MATERIALIZED VIEW test.persistent_{kafka_table}_mv TO test.persistent_{kafka_table} AS
        SELECT
            `t` AS `time`,
            `e.x` AS `some_string`
        FROM test.{kafka_table};
    """
    )


def wait_for_messages(kafka_table):
    instance.query_with_retry(
        f"SELECT count() FROM test.persistent_{kafka_table}",
        retry_count=30,
        sleep_time=1,
        check_callback=lambda x: int(x) >= 3,
    )


def assert_expected_messages(kafka_table):
    result = instance.query(
        f"SELECT * FROM test.persistent_{kafka_table} ORDER BY time"
    )

    expected = """\
123	woof
123	woof
124	test
"""
    assert TSV(result) == TSV(expected)


def drop_kafka_mv(kafka_table):
    instance.query(
        f"""
        DROP TABLE test.persistent_{kafka_table};
        DROP TABLE test.persistent_{kafka_table}_mv;
    """
    )


# Fixtures
@pytest.fixture(scope="module")
def kafka_cluster():
    try:
        cluster.start()
        kafka_id = instance.cluster.kafka_docker_id
        print(("kafka_id is {}".format(kafka_id)))
        run_endpoint(cluster)
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def kafka_setup_teardown():
    instance.query("DROP DATABASE IF EXISTS test SYNC; CREATE DATABASE test;")
    admin_client = k.get_admin_client(cluster)

    def get_topics_to_delete():
        return [t for t in admin_client.list_topics() if not t.startswith("_")]

    topics = get_topics_to_delete()
    logging.debug(f"Deleting topics: {topics}")
    result = admin_client.delete_topics(topics)
    for topic, error in result.topic_error_codes:
        if error != 0:
            logging.warning(f"Received error {error} while deleting topic {topic}")
        else:
            logging.info(f"Deleted topic {topic}")

    retries = 0
    topics = get_topics_to_delete()
    while len(topics) != 0:
        logging.info(f"Existing topics: {topics}")
        if retries >= 5:
            raise Exception(f"Failed to delete topics {topics}")
        retries += 1
        time.sleep(0.5)
    yield  # run test


# Tests
@pytest.mark.parametrize(
    "create_query_generator",
    [
        (k.generate_old_create_table_query),
        (k.generate_new_create_table_query),
    ],
)
def test_kafka_zone_awareness(kafka_cluster, create_query_generator):
    suffix = k.random_string(6)
    kafka_table = f"kafka_{suffix}"
    topname = "zone_awareness"
    instance.rotate_logs()

    produce_test_messages(kafka_cluster, topname)
    create_kafka_mv(create_query_generator, kafka_table, topname, "AWS_ZONE_ID")
    wait_for_messages(kafka_table)
    assert_expected_messages(kafka_table)
    drop_kafka_mv(kafka_table)
    assert instance.contains_in_log("client.rack set to euc1-az2")


@pytest.mark.parametrize(
    ("autodetect_facility", "expected_rack", "topic_name"),
    [
        ("AWS_ZONE_NAME_THEN_GCP_ZONE", "europe-central2-a", "zone_awareness_gcp_fallback"),
        ("CLICKHOUSE", "clickhouse-test-az", "zone_awareness_clickhouse"),
    ],
)
@pytest.mark.parametrize(
    "create_query_generator",
    [
        (k.generate_old_create_table_query),
        (k.generate_new_create_table_query),
    ],
)
def test_kafka_zone_awareness_additional_providers(
    kafka_cluster, create_query_generator, autodetect_facility, expected_rack, topic_name
):
    suffix = k.random_string(6)
    kafka_table = f"kafka_{suffix}"
    instance.rotate_logs()

    produce_test_messages(kafka_cluster, topic_name)
    create_kafka_mv(create_query_generator, kafka_table, topic_name, autodetect_facility)
    wait_for_messages(kafka_table)
    assert_expected_messages(kafka_table)
    drop_kafka_mv(kafka_table)

    assert instance.contains_in_log(f"client.rack set to {expected_rack}")


@pytest.mark.parametrize(
    "create_query_generator",
    [
        (k.generate_old_create_table_query),
        (k.generate_new_create_table_query),
    ],
)
def test_kafka_zone_awareness_unknown_facility_logs_error(kafka_cluster, create_query_generator):
    suffix = k.random_string(6)
    kafka_table = f"kafka_{suffix}"
    topname = f"zone_awareness_unknown_{suffix}"
    instance.rotate_logs()

    produce_test_messages(kafka_cluster, topname)
    create_kafka_mv(create_query_generator, kafka_table, topname, "UNKNOWN_ZONE")
    wait_for_messages(kafka_table)
    assert_expected_messages(kafka_table)
    assert instance.wait_for_log_line("Unknown kafka_autodetect_client_rack facility.")
    assert instance.grep_in_log(f"{kafka_table}.*client.rack set to").strip() == ""
    drop_kafka_mv(kafka_table)
