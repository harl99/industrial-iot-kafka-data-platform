import json
import os
import signal
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from confluent_kafka import Consumer, KafkaException


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONSUMER_GROUP_ID = os.getenv("DATA_LAKE_CONSUMER_GROUP_ID", "data-lake-consumer-group")
DATA_LAKE_ROOT = Path(os.getenv("DATA_LAKE_ROOT", "data_lake"))

TOPICS = [
    "sensor-readings",
    "maintenance-events",
    "operator-events",
    "alerts",
    "actuator-commands",
    "processed-events",
]

TOPIC_TO_FOLDER = {
    "sensor-readings": "raw/sensor_readings",
    "maintenance-events": "raw/maintenance_events",
    "operator-events": "raw/operator_events",
    "alerts": "processed/alerts",
    "actuator-commands": "processed/actuator_commands",
    "processed-events": "processed/processed_events",
}

running = True


def handle_shutdown(signum, frame):
    global running
    print("Shutdown signal received. Stopping data lake consumer...")
    running = False


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


def get_kafka_consumer() -> Consumer:
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    return Consumer(config)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def safe_event_id(event: Dict[str, Any]) -> str:
    event_id = event.get("event_id", "unknown-event-id")
    return str(event_id).replace("/", "_").replace("\\", "_")


def build_output_path(topic: str, event: Dict[str, Any], partition: int, offset: int) -> Path:
    now = utc_now()

    folder = TOPIC_TO_FOLDER.get(topic, "unknown")

    output_dir = (
        DATA_LAKE_ROOT
        / folder
        / f"year={now.year:04d}"
        / f"month={now.month:02d}"
        / f"day={now.day:02d}"
        / f"hour={now.hour:02d}"
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    event_id = safe_event_id(event)
    filename = f"{topic}_partition-{partition}_offset-{offset}_{event_id}.json"

    return output_dir / filename


def write_event_to_data_lake(topic: str, event: Dict[str, Any], partition: int, offset: int) -> Path:
    output_path = build_output_path(topic, event, partition, offset)

    data_lake_record = {
        "data_lake_metadata": {
            "ingested_at": utc_now().isoformat(),
            "kafka_topic": topic,
            "kafka_partition": partition,
            "kafka_offset": offset,
            "storage_format": "json",
        },
        "event": event,
    }

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(data_lake_record, file, indent=2, ensure_ascii=False)

    return output_path


def main():
    print("Starting local data lake consumer")
    print(f"KAFKA_BOOTSTRAP_SERVERS={KAFKA_BOOTSTRAP_SERVERS}")
    print(f"CONSUMER_GROUP_ID={CONSUMER_GROUP_ID}")
    print(f"DATA_LAKE_ROOT={DATA_LAKE_ROOT}")
    print(f"TOPICS={TOPICS}")
    print("-" * 80)

    consumer = get_kafka_consumer()
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
                output_path = write_event_to_data_lake(topic, event, partition, offset)
                consumer.commit(message=msg)

                print(
                    f"Wrote event_id={event.get('event_id')} "
                    f"topic={topic} partition={partition} offset={offset} "
                    f"path={output_path}"
                )

            except Exception as exc:
                print(
                    f"ERROR writing event to data lake "
                    f"topic={topic} partition={partition} offset={offset}: {exc}"
                )

    finally:
        print("Closing data lake consumer...")
        consumer.close()


if __name__ == "__main__":
    main()
