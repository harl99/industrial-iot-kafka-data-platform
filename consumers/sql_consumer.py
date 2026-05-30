import json
import os
import signal
from typing import Any, Dict

import psycopg2
from confluent_kafka import Consumer, KafkaException


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONSUMER_GROUP_ID = os.getenv("SQL_CONSUMER_GROUP_ID", "sql-consumer-group")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "industrial_iot")
POSTGRES_USER = os.getenv("POSTGRES_USER", "iot_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "iot_password")

TOPICS = [
    "sensor-readings",
    "maintenance-events",
    "operator-events",
    "alerts",
    "actuator-commands",
]

running = True


def handle_shutdown(signum, frame):
    global running
    print("Shutdown signal received. Stopping SQL consumer...")
    running = False


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


def get_postgres_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def get_kafka_consumer() -> Consumer:
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    return Consumer(config)


def insert_sensor_reading(cursor, event: Dict[str, Any]):
    payload = event["payload"]

    cursor.execute(
        """
        INSERT INTO sensor_readings (
            event_id,
            event_timestamp,
            machine_id,
            sensor_id,
            sensor_type,
            value,
            unit,
            status,
            raw_event
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING;
        """,
        (
            event["event_id"],
            event["event_timestamp"],
            payload["machine_id"],
            payload["sensor_id"],
            payload["sensor_type"],
            payload["value"],
            payload["unit"],
            payload["status"],
            json.dumps(event),
        ),
    )


def insert_maintenance_event(cursor, event: Dict[str, Any]):
    payload = event["payload"]

    cursor.execute(
        """
        INSERT INTO maintenance_events (
            event_id,
            event_timestamp,
            machine_id,
            event_category,
            description,
            technician,
            severity,
            raw_event
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING;
        """,
        (
            event["event_id"],
            event["event_timestamp"],
            payload["machine_id"],
            payload["event_category"],
            payload["description"],
            payload.get("technician"),
            payload["severity"],
            json.dumps(event),
        ),
    )


def insert_operator_event(cursor, event: Dict[str, Any]):
    payload = event["payload"]

    cursor.execute(
        """
        INSERT INTO operator_events (
            event_id,
            event_timestamp,
            machine_id,
            operator_id,
            event_type,
            notes,
            raw_event
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING;
        """,
        (
            event["event_id"],
            event["event_timestamp"],
            payload["machine_id"],
            payload["operator_id"],
            payload["event_type"],
            payload.get("notes"),
            json.dumps(event),
        ),
    )


def insert_alert(cursor, event: Dict[str, Any]):
    payload = event["payload"]

    cursor.execute(
        """
        INSERT INTO alerts (
            event_id,
            source_event_id,
            event_timestamp,
            machine_id,
            sensor_type,
            alert_type,
            alert_message,
            severity,
            threshold_value,
            observed_value,
            raw_event
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING;
        """,
        (
            event["event_id"],
            payload["source_event_id"],
            event["event_timestamp"],
            payload["machine_id"],
            payload["sensor_type"],
            payload["alert_type"],
            payload["alert_message"],
            payload["severity"],
            payload["threshold_value"],
            payload["observed_value"],
            json.dumps(event),
        ),
    )


def insert_actuator_command(cursor, event: Dict[str, Any]):
    payload = event["payload"]

    cursor.execute(
        """
        INSERT INTO actuator_commands (
            event_id,
            event_timestamp,
            machine_id,
            actuator_id,
            actuator_type,
            command,
            reason,
            raw_event
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING;
        """,
        (
            event["event_id"],
            event["event_timestamp"],
            payload["machine_id"],
            payload["actuator_id"],
            payload["actuator_type"],
            payload["command"],
            payload["reason"],
            json.dumps(event),
        ),
    )


def process_message(cursor, topic: str, event: Dict[str, Any]):
    if topic == "sensor-readings":
        insert_sensor_reading(cursor, event)
    elif topic == "maintenance-events":
        insert_maintenance_event(cursor, event)
    elif topic == "operator-events":
        insert_operator_event(cursor, event)
    elif topic == "alerts":
        insert_alert(cursor, event)
    elif topic == "actuator-commands":
        insert_actuator_command(cursor, event)
    else:
        print(f"Skipping unsupported topic: {topic}")


def main():
    print("Starting SQL consumer")
    print(f"KAFKA_BOOTSTRAP_SERVERS={KAFKA_BOOTSTRAP_SERVERS}")
    print(f"CONSUMER_GROUP_ID={CONSUMER_GROUP_ID}")
    print(f"POSTGRES_HOST={POSTGRES_HOST}")
    print(f"POSTGRES_DB={POSTGRES_DB}")
    print(f"TOPICS={TOPICS}")
    print("-" * 80)

    consumer = get_kafka_consumer()
    connection = get_postgres_connection()
    connection.autocommit = False

    consumer.subscribe(TOPICS)

    try:
        while running:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            topic = msg.topic()
            partition = msg.partition()
            offset = msg.offset()

            try:
                event = json.loads(msg.value().decode("utf-8"))

                with connection.cursor() as cursor:
                    process_message(cursor, topic, event)

                connection.commit()
                consumer.commit(message=msg)

                print(
                    f"Inserted event_id={event.get('event_id')} "
                    f"topic={topic} partition={partition} offset={offset}"
                )

            except Exception as exc:
                connection.rollback()
                print(
                    f"ERROR processing topic={topic} "
                    f"partition={partition} offset={offset}: {exc}"
                )

    finally:
        print("Closing SQL consumer...")
        consumer.close()
        connection.close()


if __name__ == "__main__":
    main()
