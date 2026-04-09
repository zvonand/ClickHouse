import pytest
import datetime
import logging
import time
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

instance = cluster.add_instance(
    "instance",
    with_rabbitmq=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_last_event_time(logger_name, message):
    instance.query("SYSTEM FLUSH LOGS")
    ts = instance.query(
        f"""SELECT
                event_time_microseconds
            FROM merge(system, '^text_log')
            WHERE
                logger_name  = '{logger_name}' AND
                message = '{ message}'
            ORDER BY event_time_microseconds DESC
            LIMIT 1"""
    ).strip()
    logging.info(
        f"logger_name='{logger_name}', message='{message}', last_event_time='{ts}'"
    )
    return datetime.datetime.fromisoformat(ts)


def test_shutdown_rabbitmq_with_materialized_view(started_cluster):
    """
    Test that restarting server during active RabbitMQ consumption
    doesn't cause "Table is shutting down" errors.
    """

    instance.query("DROP DATABASE IF EXISTS test SYNC")
    instance.query("CREATE DATABASE test ENGINE=Atomic")

    # Create destination table
    instance.query(
        """
        CREATE TABLE test.destination (
            key UInt64,
            value String,
            _timestamp DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY key
    """
    )

    # Create RabbitMQ queue table
    logging.info("Creating RabbitMQ table...")
    instance.query(
        """
        CREATE TABLE test.rabbitmq_queue (
            key UInt64,
            value String
        ) ENGINE = RabbitMQ
        SETTINGS
            rabbitmq_host_port = 'rabbitmq1:5672',
            rabbitmq_exchange_name = 'test_shutdown_exchange',
            rabbitmq_exchange_type = 'fanout',
            rabbitmq_format = 'JSONEachRow',
            rabbitmq_username = 'root',
            rabbitmq_password = 'clickhouse',
            rabbitmq_num_consumers = 3,
            rabbitmq_flush_interval_ms = 100,
            rabbitmq_max_block_size = 100
    """
    )

    # Wait for RabbitMQ connection
    time.sleep(10)

    # Create materialized view
    instance.query(
        """
        CREATE MATERIALIZED VIEW test.test_view TO test.destination AS
        SELECT key, value FROM test.rabbitmq_queue
    """
    )

    # Publish by inserting directly into RabbitMQ table
    logging.info("Publishing messages by inserting into RabbitMQ table")
    instance.query(
        """
        INSERT INTO test.rabbitmq_queue
        SELECT number AS key, toString(number) AS value
        FROM numbers(1000)
    """
    )

    # Wait for messages to be published and consumed back
    time.sleep(10)

    # Check messages
    count_before = int(instance.query("SELECT count() FROM test.destination").strip())
    logging.info(f"Messages in destination before restart: {count_before}")

    # Publish more messages
    for batch in range(5):
        instance.query(
            f"""
            INSERT INTO test.rabbitmq_queue
            SELECT number + {1000 + batch * 100} AS key,
                   concat('batch_', toString({batch}), '_', toString(number)) AS value
            FROM numbers(100)
        """
        )
        time.sleep(0.5)

    time.sleep(3)

    instance.restart_clickhouse()

    registry_shutdown_queue_time = get_last_event_time(
        "StreamingStorageRegistry", "Will shutdown 1 queue storages"
    )

    rabbit_shutdown_time = get_last_event_time(
        "StorageRabbitMQ (test.rabbitmq_queue)", "Shutdown finished"
    )

    registry_already_shutdown_queue_time = get_last_event_time(
        "StreamingStorageRegistry", "Already shutdown 1 queue storages"
    )
    assert registry_shutdown_queue_time < rabbit_shutdown_time
    assert rabbit_shutdown_time < registry_already_shutdown_queue_time


@pytest.mark.skip(reason="There is data race in Rabbit MQ storage when shutting down")
def test_attach_detach_rabbitmq_with_materialized_view(started_cluster):
    """
    Test that restarting server during active RabbitMQ consumption
    doesn't cause "Table is shutting down" errors.
    """

    instance.query("DROP DATABASE IF EXISTS test SYNC")
    instance.query("CREATE DATABASE test ENGINE=Atomic")

    # Create RabbitMQ queue table
    logging.info("Creating RabbitMQ table...")
    instance.query(
        """
        CREATE TABLE test.rabbitmq_queue (
            key UInt64,
            value String
        ) ENGINE = RabbitMQ
        SETTINGS
            rabbitmq_host_port = 'rabbitmq1:5672',
            rabbitmq_exchange_name = 'test_shutdown_exchange',
            rabbitmq_exchange_type = 'fanout',
            rabbitmq_format = 'JSONEachRow',
            rabbitmq_username = 'root',
            rabbitmq_password = 'clickhouse',
            rabbitmq_num_consumers = 3,
            rabbitmq_flush_interval_ms = 100,
            rabbitmq_max_block_size = 100
    """
    )

    # Wait for RabbitMQ connection
    time.sleep(10)

    instance.query_with_retry(
        "SELECT count() FROM system.tables WHERE name='rabbitmq_queue' AND database='test'",
        check_callback=lambda x: x.strip() == "1",
    )

    instance.query("DETACH TABLE test.rabbitmq_queue")

    instance.query_with_retry(
        "SELECT count() FROM system.tables WHERE name='rabbitmq_queue' AND database='test'",
        check_callback=lambda x: x.strip() == "0",
    )

    rabbit_shutdown_time_after_detach = get_last_event_time(
        "StorageRabbitMQ (test.rabbitmq_queue)", "Shutdown finished"
    )
    instance.restart_clickhouse()

    registry_no_queue_to_shutdown_time1 = get_last_event_time(
        "StreamingStorageRegistry", "There are no queue storages to shutdown"
    )

    assert registry_no_queue_to_shutdown_time1 > rabbit_shutdown_time_after_detach

    instance.query_with_retry(
        "SELECT count() FROM system.tables WHERE name='rabbitmq_queue' AND database='test'",
        check_callback=lambda x: x.strip() == "1",
    )

    instance.restart_clickhouse()

    registry_shutdown_queue_time = get_last_event_time(
        "StreamingStorageRegistry", "Will shutdown 1 queue storages"
    )

    rabbit_shutdown_time_after_restart = get_last_event_time(
        "StorageRabbitMQ (test.rabbitmq_queue)", "Shutdown finished"
    )

    registry_already_shutdown_queue_time = get_last_event_time(
        "StreamingStorageRegistry", "Already shutdown 1 queue storages"
    )

    assert registry_shutdown_queue_time > registry_no_queue_to_shutdown_time1
    assert rabbit_shutdown_time_after_restart != rabbit_shutdown_time_after_detach

    assert rabbit_shutdown_time_after_restart > registry_shutdown_queue_time
    assert registry_already_shutdown_queue_time > rabbit_shutdown_time_after_restart


def test_rabbitmq_virtual_column_table(started_cluster):
    """
    Test that the `_table` virtual column returns the table name
    for the RabbitMQ engine.
    """

    instance.query("DROP DATABASE IF EXISTS test_virt SYNC")
    instance.query("CREATE DATABASE test_virt ENGINE=Atomic")

    exchange_name = "test_virtual_table_exchange"

    instance.query(
        f"""
        CREATE TABLE test_virt.rabbitmq_source (
            key UInt64,
            value String
        ) ENGINE = RabbitMQ
        SETTINGS
            rabbitmq_host_port = 'rabbitmq1:5672',
            rabbitmq_exchange_name = '{exchange_name}',
            rabbitmq_exchange_type = 'fanout',
            rabbitmq_format = 'JSONEachRow',
            rabbitmq_username = 'root',
            rabbitmq_password = 'clickhouse',
            rabbitmq_flush_interval_ms = 100,
            rabbitmq_max_block_size = 100,
            rabbitmq_commit_on_select = 1
    """
    )

    time.sleep(10)

    instance.query(
        """
        INSERT INTO test_virt.rabbitmq_source
        SELECT number AS key, toString(number) AS value
        FROM numbers(10)
        """
    )

    result = ""
    for _ in range(100):
        result += instance.query(
            """
            SELECT key, value, _exchange_name, _table
            FROM test_virt.rabbitmq_source
            SETTINGS stream_like_engine_allow_direct_select=1
            """
        )
        lines = [l for l in result.strip().split("\n") if l]
        if len(lines) == 10:
            break
        time.sleep(0.5)

    lines = [l for l in result.strip().split("\n") if l]
    assert len(lines) == 10, f"Expected 10 rows, got {len(lines)}"

    for line in lines:
        parts = line.split("\t")
        assert parts[2] == exchange_name, f"Expected exchange '{exchange_name}', got '{parts[2]}'"
        assert parts[3] == "rabbitmq_source", f"Expected table 'rabbitmq_source', got '{parts[3]}'"

    instance.query("DROP DATABASE test_virt SYNC")
    instance.query(
        f"""
         DROP TABLE {db}.consumer;
         DROP TABLE {db}.view;
         DROP TABLE {db}.rabbitmq;
     """
    )

    # 100 = first poll should return 100 messages (and rows)
    # not waiting for stream_flush_interval_ms
    assert (
        int(result) == 100
    ), "Messages from rabbitmq should be flushed when block of size rabbitmq_max_block_size is formed!"


def test_rabbitmq_flush_by_time(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        DROP TABLE IF EXISTS {db}.view;
        DROP TABLE IF EXISTS {db}.consumer;

        CREATE TABLE {db}.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_flush_by_time',
                     rabbitmq_queue_base = '{unique}_flush_by_time',
                     rabbitmq_max_block_size = 100,
                     rabbitmq_flush_interval_ms = 5000,
                     rabbitmq_format = 'JSONEachRow';

        CREATE TABLE {db}.view (key UInt64, value UInt64, ts DateTime64(3) MATERIALIZED now64(3))
            ENGINE = MergeTree()
            ORDER BY key;
    """
    )

    cancel = threading.Event()

    def produce():
        credentials = pika.PlainCredentials("root", "clickhouse")
        parameters = pika.ConnectionParameters(
            rabbitmq_cluster.rabbitmq_ip,
            rabbitmq_cluster.rabbitmq_port,
            "/",
            credentials,
        )
        connection = pika.BlockingConnection(parameters)

        while not cancel.is_set():
            try:
                channel = connection.channel()
                channel.basic_publish(
                    exchange=f"{unique}_flush_by_time",
                    routing_key="",
                    body=json.dumps({"key": 0, "value": 0}),
                )
                logging.debug("Produced a message")
                time.sleep(0.8)
            except Exception as e:
                logging.debug(f"Got error: {str(e)}")

    produce_thread = threading.Thread(target=produce)
    produce_thread.start()

    instance.query(
        f"""
        CREATE MATERIALIZED VIEW {db}.consumer TO {db}.view AS
            SELECT * FROM {db}.rabbitmq;
    """
    )

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        time.sleep(0.2)
        total_count = instance.query(
            f"SELECT count() FROM system.parts WHERE database = '{db}' AND table = 'view'"
        )
        logging.debug(f"kssenii total count: {total_count}")
        count = int(
            instance.query(
                f"SELECT count() FROM system.parts WHERE database = '{db}' AND table = 'view' AND name = 'all_1_1_0'"
            )
        )
        logging.debug(f"kssenii count: {count}")
        if count > 0:
            break
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The part 'all_1_1_0' is still missing."
        )

    time.sleep(12)
    result = instance.query(f"SELECT uniqExact(ts) FROM {db}.view")

    cancel.set()
    produce_thread.join()

    instance.query(
        f"""
        DROP TABLE {db}.consumer;
        DROP TABLE {db}.view;
        DROP TABLE {db}.rabbitmq;
    """
    )

    assert int(result) == 3


def test_rabbitmq_handle_error_mode_stream(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        DROP TABLE IF EXISTS {db}.rabbitmq;
        DROP TABLE IF EXISTS {db}.view;
        DROP TABLE IF EXISTS {db}.data;
        DROP TABLE IF EXISTS {db}.errors;
        DROP TABLE IF EXISTS {db}.errors_view;

        CREATE TABLE {db}.rabbit (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = '{rabbitmq_cluster.rabbitmq_host}:5672',
                     rabbitmq_exchange_name = '{unique}_select',
                     rabbitmq_commit_on_select = 1,
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n',
                     rabbitmq_handle_error_mode = 'stream';


        CREATE TABLE {db}.errors (error Nullable(String), broken_message Nullable(String))
             ENGINE = MergeTree()
             ORDER BY tuple();

        CREATE MATERIALIZED VIEW {db}.errors_view TO {db}.errors AS
                SELECT _error as error, _raw_message as broken_message FROM {db}.rabbit where not isNull(_error);

        CREATE TABLE {db}.data (key UInt64, value UInt64)
             ENGINE = MergeTree()
             ORDER BY key;

        CREATE MATERIALIZED VIEW {db}.view TO {db}.data AS
                SELECT key, value FROM {db}.rabbit;
        """
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = []
    num_rows = 50
    for i in range(num_rows):
        if i % 2 == 0:
            messages.append(json.dumps({"key": i, "value": i}))
        else:
            messages.append("Broken message " + str(i))

    for message in messages:
        channel.basic_publish(exchange=f"{unique}_select", routing_key="", body=message)

    connection.close()
    # The order of messages in select * from {db}.rabbitmq is not guaranteed, so sleep to collect everything in one select
    time.sleep(1)

    check_expected_result_polling(num_rows, f"SELECT count() FROM {db}.data")

    result = instance.query(f"SELECT * FROM {db}.data ORDER by key")
    expected = "0\t0\n" * (num_rows // 2)
    for i in range(num_rows):
        if i % 2 == 0:
            expected += str(i) + "\t" + str(i) + "\n"

    assert result == expected

    check_expected_result_polling(num_rows / 2, f"SELECT count() FROM {db}.errors")

    broken_messages = instance.query(
        f"SELECT broken_message FROM {db}.errors order by broken_message"
    )
    expected = []
    for i in range(num_rows):
        if i % 2 != 0:
            expected.append("Broken message " + str(i) + "\n")

    expected = "".join(sorted(expected))
    assert broken_messages == expected


def test_attach_broken_table(rabbitmq_cluster, db, unique):
    table_name = f"rabbit_queue_{uuid.uuid4().hex}"
    instance.query(
        f"""
        DROP TABLE IF EXISTS {table_name};
        ATTACH TABLE {table_name} UUID '{uuid.uuid4()}' (`payload` String) ENGINE = RabbitMQ SETTINGS rabbitmq_host_port = 'nonexisting:5671', rabbitmq_format = 'JSONEachRow', rabbitmq_username = 'test', rabbitmq_password = 'test'
        """
    )

    error = instance.query_and_get_error(f"SELECT * FROM {table_name}")
    assert "CANNOT_CONNECT_RABBITMQ" in error
    error = instance.query_and_get_error(f"INSERT INTO {table_name} VALUES ('test')")
    assert "CANNOT_CONNECT_RABBITMQ" in error


def test_rabbitmq_nack_failed_insert(rabbitmq_cluster, db, unique):
    table_name = f"nack_failed_insert_{uuid.uuid4().hex}"
    exchange = f"{table_name}_exchange"

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    deadl_exchange = f"{unique}_deadl"
    channel.exchange_declare(exchange=deadl_exchange)

    result = channel.queue_declare(queue=f"{unique}_deadq")
    queue_name = result.method.queue
    channel.queue_bind(exchange=deadl_exchange, routing_key="", queue=queue_name)

    instance.query(
        f"""
        CREATE TABLE {db}.{table_name} (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = '{rabbitmq_cluster.rabbitmq_host}:5672',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_exchange_name = '{exchange}',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_queue_settings_list='x-dead-letter-exchange={deadl_exchange}';

        DROP TABLE IF EXISTS {db}.view;
        CREATE TABLE {db}.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;

        DROP TABLE IF EXISTS {db}.consumer;
        CREATE MATERIALIZED VIEW {db}.consumer TO {db}.view AS
            SELECT intDiv(key, if(key < 25, 0, 1)) as key, value FROM {db}.{table_name};
        """
    )

    num_rows = 25
    for i in range(num_rows):
        message = json.dumps({"key": i, "value": i}) + "\n"
        channel.basic_publish(exchange=exchange, routing_key="", body=message)

    instance.wait_for_log_line(
        "Failed to push to views. Error: Code: 153. DB::Exception: Division by zero"
    )

    count = [0]

    def on_consume(channel, method, properties, body):
        data = json.loads(body)
        message = json.dumps({"key": data["key"] + 100, "value": data["value"]}) + "\n"
        channel.basic_publish(exchange=exchange, routing_key="", body=message)
        count[0] += 1
        if count[0] == num_rows:
            channel.stop_consuming()

    channel.basic_consume(queue_name, on_consume)
    channel.start_consuming()

    check_expected_result_polling(num_rows, f"SELECT count() FROM {db}.view")

    instance.query(
        f"""
        DROP TABLE {db}.consumer;
        DROP TABLE {db}.view;
        DROP TABLE {db}.{table_name};
    """
    )
    connection.close()


def view_test(expected_num_messages, _exchange_name, db):
    result = instance.query(f"SELECT COUNT(1) FROM {db}.errors")

    assert int(result) == expected_num_messages


def dead_letter_queue_test(expected_num_messages, exchange_name, _db):
    result = instance.query(f"SELECT * FROM system.dead_letter_queue FORMAT Vertical")

    logging.debug(f"system.dead_letter_queue content is {result}")

    rows = int(
        instance.query(
            f"SELECT count() FROM system.dead_letter_queue WHERE rabbitmq_exchange_name = '{exchange_name}'"
        )
    )
    assert rows == expected_num_messages


def rabbitmq_reject_broken_messages(
    rabbitmq_cluster, db, unique, handle_error_mode, additional_dml, check_method, broken_messages_rejected
):
    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    deadletter_exchange = f"{unique}_deadletter_exchange_{handle_error_mode}"
    deadletter_queue = f"{unique}_deadletter_queue_{handle_error_mode}"
    channel.exchange_declare(exchange=deadletter_exchange)

    exchange = f"{unique}_select_{handle_error_mode}_{int(time.time())}"

    result = channel.queue_declare(queue=deadletter_queue)
    channel.queue_bind(
        exchange=deadletter_exchange, routing_key="", queue=deadletter_queue
    )

    instance.query(
        f"""
        DROP TABLE IF EXISTS {db}.rabbitmq;
        DROP TABLE IF EXISTS {db}.view;
        DROP TABLE IF EXISTS {db}.data;
        DROP TABLE IF EXISTS {db}.errors;
        DROP TABLE IF EXISTS {db}.errors_view;

        CREATE TABLE {db}.rabbit (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = '{rabbitmq_cluster.rabbitmq_host}:5672',
                     rabbitmq_exchange_name = '{exchange}',
                     rabbitmq_commit_on_select = 1,
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n',
                     rabbitmq_handle_error_mode = '{handle_error_mode}',
                     rabbitmq_queue_settings_list='x-dead-letter-exchange={deadletter_exchange}';


        CREATE TABLE {db}.errors (error Nullable(String), broken_message Nullable(String))
             ENGINE = MergeTree()
             ORDER BY tuple();

        CREATE TABLE {db}.data (key UInt64, value UInt64)
             ENGINE = MergeTree()
             ORDER BY key;

        CREATE MATERIALIZED VIEW {db}.view TO {db}.data AS
                SELECT key, value FROM {db}.rabbit;

        {additional_dml};

        """
    )

    messages = []
    num_rows = 50
    num_good_messages = 0

    for i in range(num_rows):
        if (i+1) % 2 == 0:   # let's finish on good message to not miss the latest one
            messages.append(json.dumps({"key": i, "value": i}))
            num_good_messages += 1
        else:
            messages.append("Broken message " + str(i))

    for message in messages:
        channel.basic_publish(exchange=exchange, routing_key="", body=message)

    time.sleep(1)

    expected_num_rows = num_good_messages if broken_messages_rejected else num_rows

    check_expected_result_polling(expected_num_rows, f"SELECT count() FROM {db}.data")

    dead_letters = []
    num_bad_messages = num_rows - num_good_messages

    def on_dead_letter(channel, method, properties, body):
        dead_letters.append(body)
        if len(dead_letters) == num_bad_messages:
            channel.stop_consuming()

    channel.basic_consume(deadletter_queue, on_dead_letter)
    channel.start_consuming()

    assert len(dead_letters) == num_bad_messages

    i = 0
    for letter in dead_letters:
        assert f"Broken message {i}" in str(letter)
        i += 2

    result = instance.query(f"SELECT * FROM {db}.errors FORMAT Vertical")
    logging.debug(f"{db}.errors contains {result}")

    check_method(len(dead_letters), exchange, db)

    connection.close()


def test_rabbitmq_reject_broken_messages_stream(rabbitmq_cluster, db, unique):
    rabbitmq_reject_broken_messages(
        rabbitmq_cluster,
        db,
        unique,
        "stream",
        f"CREATE MATERIALIZED VIEW {db}.errors_view TO {db}.errors AS SELECT _error as error, _raw_message as broken_message FROM {db}.rabbit where not isNull(_error)",
        view_test,
        broken_messages_rejected = False,
    )


def test_rabbitmq_reject_broken_messages_dead_letter_queue(rabbitmq_cluster, db, unique):
    rabbitmq_reject_broken_messages(
        rabbitmq_cluster,
        db,
        unique,
        "dead_letter_queue",
        "",
        dead_letter_queue_test,
        broken_messages_rejected = True,
    )


def test_rabbitmq_json_type(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        SET enable_json_type=1;
        CREATE TABLE {db}.rabbitmq (data JSON)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_json_type',
                     rabbitmq_format = 'JSONAsObject',
                     rabbitmq_commit_on_select = 1,
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_queue_base = '{unique}_json_type',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE {db}.view (a Int64)
            ENGINE = MergeTree()
            ORDER BY a;
        CREATE MATERIALIZED VIEW {db}.consumer TO {db}.view AS
            SELECT data.a::Int64 as a FROM {db}.rabbitmq;
        """
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = [
        '{"a" : 1}',
        '{"a" : 2}',
    ]

    for message in messages:
        channel.basic_publish(exchange=f"{unique}_json_type", routing_key="", body=message)
    connection.close()

    while int(instance.query(f"SELECT count() FROM {db}.view")) < 2:
        time.sleep(1)

    result = instance.query(f"SELECT * FROM {db}.view ORDER BY a;")

    expected = """\
1
2
"""

    assert TSV(result) == TSV(expected)

    instance.query(
        f"""
        DROP TABLE {db}.view;
        DROP TABLE {db}.consumer;
        DROP TABLE {db}.rabbitmq;
    """
    )


def test_hiding_credentials(rabbitmq_cluster, db, unique):
    table_name = "test_hiding_credentials"
    exchange = f"{unique}_{table_name}"
    instance.query(
        f"""
        DROP TABLE IF EXISTS {db}.{table_name};
        CREATE TABLE {db}.{table_name} (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = '{rabbitmq_cluster.rabbitmq_host}:{cluster.rabbitmq_port}',
                     rabbitmq_exchange_name = '{exchange}',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_username = 'clickhouse',
                     rabbitmq_password = 'rabbitmq',
                     rabbitmq_address = 'amqp://root:clickhouse@rabbitmq1:5672/';
        """
    )

    instance.query("SYSTEM FLUSH LOGS")
    message = instance.query(f"SELECT message FROM system.text_log WHERE message ILIKE '%CREATE TABLE {db}.{table_name}%'")
    assert "rabbitmq_password = \\'[HIDDEN]\\'" in  message
    assert "rabbitmq_address = \\'amqp://root:[HIDDEN]@rabbitmq1:5672/\\'" in  message


def test_rabbitmq_default_mode_nack_on_parse_error(rabbitmq_cluster, db, unique):
    """When rabbitmq_handle_error_mode = 'default' and a message fails to parse,
    the message must be properly nack'd (not left permanently unacked).
    Regression test for https://github.com/ClickHouse/ClickHouse/issues/73541
    """
    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    deadletter_exchange = f"{unique}_dlx"
    deadletter_queue = f"{unique}_dlq"
    channel.exchange_declare(exchange=deadletter_exchange)
    channel.queue_declare(queue=deadletter_queue)
    channel.queue_bind(exchange=deadletter_exchange, routing_key="", queue=deadletter_queue)

    exchange = f"{unique}_exchange"

    instance.query(
        f"""
        CREATE TABLE {db}.rabbit (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = '{rabbitmq_cluster.rabbitmq_host}:5672',
                     rabbitmq_exchange_name = '{exchange}',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_flush_interval_ms = 1000,
                     rabbitmq_queue_settings_list = 'x-dead-letter-exchange={deadletter_exchange}';

        CREATE TABLE {db}.data (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;

        CREATE MATERIALIZED VIEW {db}.view TO {db}.data AS
            SELECT key, value FROM {db}.rabbit;
        """
    )

    num_bad = 10
    for i in range(num_bad):
        # String value for a UInt64 column triggers a parse error in DEFAULT mode
        channel.basic_publish(
            exchange=exchange, routing_key="",
            body=json.dumps({"key": f"not_a_number_{i}", "value": i}),
        )

    # Wait for the error to appear in logs, proving the messages were consumed
    instance.wait_for_log_line("Failed to push to views.*Cannot parse input")

    # Bad messages must arrive in the dead-letter queue (proving they were nack'd)
    dead_letters = []

    def on_dead_letter(ch, method, properties, body):
        dead_letters.append(body)
        if len(dead_letters) == num_bad:
            ch.stop_consuming()

    channel.basic_consume(deadletter_queue, on_dead_letter, auto_ack=True)
    deadline = time.monotonic() + 30
    while len(dead_letters) < 1 and time.monotonic() < deadline:
        connection.process_data_events(time_limit=1)

    # In DEFAULT mode each streaming iteration processes one bad message before
    # the exception aborts the pipeline, so collecting all 10 takes many cycles.
    # Asserting >= 1 is sufficient: before the fix we would get 0 (messages
    # stayed permanently unacked instead of being nack'd to the DLX).
    assert len(dead_letters) >= 1, (
        "No dead-lettered messages received within 30 seconds. "
        "Messages were likely left permanently unacked instead of being nack'd."
    )

    # Now publish good messages and verify they are consumed
    num_good = 10
    for i in range(num_good):
        channel.basic_publish(
            exchange=exchange, routing_key="",
            body=json.dumps({"key": i, "value": i}),
        )

    check_expected_result_polling(num_good, f"SELECT count() FROM {db}.data")

    channel.queue_delete(deadletter_queue)
    channel.exchange_delete(deadletter_exchange)
    connection.close()


def test_connection_info_logging_with_rabbitmq_address(started_cluster):
    """Verify that reconnection logs show the actual connection address,
    not ':0', when rabbitmq_address is used instead of rabbitmq_host_port."""

    # Create a table using rabbitmq_address (connection string)
    instance.query("""
        CREATE TABLE test.rmq_addr_log (key UInt64, value String)
        ENGINE = RabbitMQ
        SETTINGS rabbitmq_address = 'amqp://root:clickhouse@{}:5672/',
                rabbitmq_exchange_name = 'addr_log_exchange',
                rabbitmq_format = 'JSONEachRow';
    """.format(cluster.rabbitmq_host))

    # Force a disconnect/reconnect by briefly stopping RabbitMQ
    cluster.pause_container('rabbitmq1')
    time.sleep(3)
    cluster.unpause_container('rabbitmq1')
    time.sleep(5)

    # Check server logs for the reconnection message
    log = instance.grep_in_log("Trying to restore connection to")
    assert ':0' not in log, \
        f"Log contains ':0' instead of actual address: {log}"
    assert 'rabbitmq' in log.lower() or '5672' in log, \
        f"Log should contain the actual connection address: {log}"
    assert 'root' not in log
    assert 'clickhouse' not in log

    instance.query("DROP TABLE test.rmq_addr_log")
