import json
import os
import signal
import socket
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from confluent_kafka import Consumer, KafkaException, Producer


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONSUMER_GROUP_ID = os.getenv("ALERT_CONSUMER_GROUP_ID", "alert-consumer-group")

INPUT_TOPIC = "sensor-readings"
OUTPUT_TOPIC = "alerts"

running = True


ALERT_RULES = {
    "temperature_sensor": {
        "operator": ">",
        "threshold": 100,
        "severity": "critical",
        "alert_type": "high_temperature",
        "message": "Temperature exceeded safe threshold.",
    },
    "pressure_sensor": {
        "operator": ">",
        "threshold": 120,
        "severity": "critical",
        "alert_type": "high_pressure",
        "message": "Pressure exceeded safe threshold.",
    },
    "vibration_sensor": {
        "operator": ">",
        "threshold": 0.8,
        "severity": "critical",
        "alert_type": "high_vibration",
        "message": "Vibration exceeded safe threshold.",
    },
    "flow_sensor": {
        "operator": "<",
        "threshold": 20,
        "severity": "critical",
        "alert_type": "low_flow_rate",
        "message": "Flow rate dropped below safe threshold.",
    },
    "energy_sensor": {
        "operator": ">",
        "threshold": 500,
        "severity": "warning",
        "alert_type": "high_energy_consumption",
        "message": "Energy consumption exceeded expected operating range.",
    },
}


def handle_shutdown(signum, frame):
    global running
    print("Shutdown signal received. Stopping alert consumer...")
    running = False


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_kafka_consumer() -> Consumer:
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    return Consumer(config)


def get_kafka_producer() -> Producer:
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": f"alert-consumer-{socket.gethostname()}",
    }
    return Producer(config)


def delivery_report(err, msg):
    if err is not None:
        print(f"Alert delivery failed: {err}")
    else:
        print(
            f"Alert delivered to topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


def rule_triggered(value: float, operator: str, threshold: float) -> bool:
    if operator == ">":
        return value > threshold
    if operator == "<":
        return value < threshold
    raise ValueError(f"Unsupported operator: {operator}")


def build_alert(sensor_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    payload = sensor_event["payload"]

    sensor_type = payload["sensor_type"]
    value = float(payload["value"])

    rule = ALERT_RULES.get(sensor_type)

    if rule is None:
        return None

    if not rule_triggered(value, rule["operator"], rule["threshold"]):
        return None

    alert_id = str(uuid.uuid4())

    return {
        "event_id": alert_id,
        "event_type": "alerts",
        "event_timestamp": utc_now_iso(),
        "source": "alert-consumer",
        "message_key": payload["machine_id"],
        "payload": {
            "source_event_id": sensor_event["event_id"],
            "source_event_timestamp": sensor_event["event_timestamp"],
            "machine_id": payload["machine_id"],
            "sensor_id": payload["sensor_id"],
            "sensor_type": sensor_type,
            "alert_type": rule["alert_type"],
            "alert_message": rule["message"],
            "severity": rule["severity"],
            "threshold_value": rule["threshold"],
            "observed_value": value,
            "unit": payload["unit"],
            "operator": rule["operator"],
        },
    }


def publish_alert(producer: Producer, alert: Dict[str, Any]):
    producer.produce(
        topic=OUTPUT_TOPIC,
        key=alert["message_key"],
        value=json.dumps(alert).encode("utf-8"),
        callback=delivery_report,
    )
    producer.poll(0)
    producer.flush(5)


def main():
    print("Starting alert consumer")
    print(f"KAFKA_BOOTSTRAP_SERVERS={KAFKA_BOOTSTRAP_SERVERS}")
    print(f"CONSUMER_GROUP_ID={CONSUMER_GROUP_ID}")
    print(f"INPUT_TOPIC={INPUT_TOPIC}")
    print(f"OUTPUT_TOPIC={OUTPUT_TOPIC}")
    print("-" * 80)

    consumer = get_kafka_consumer()
    producer = get_kafka_producer()

    consumer.subscribe([INPUT_TOPIC])

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
                sensor_event = json.loads(msg.value().decode("utf-8"))
                alert = build_alert(sensor_event)

                if alert:
                    publish_alert(producer, alert)
                    print(
                        f"ALERT generated "
                        f"machine_id={alert['payload']['machine_id']} "
                        f"sensor_type={alert['payload']['sensor_type']} "
                        f"observed_value={alert['payload']['observed_value']} "
                        f"threshold={alert['payload']['threshold_value']} "
                        f"alert_type={alert['payload']['alert_type']}"
                    )
                else:
                    payload = sensor_event.get("payload", {})
                    print(
                        f"No alert "
                        f"machine_id={payload.get('machine_id')} "
                        f"sensor_type={payload.get('sensor_type')} "
                        f"value={payload.get('value')} "
                        f"topic={topic} partition={partition} offset={offset}"
                    )

                consumer.commit(message=msg)

            except Exception as exc:
                print(
                    f"ERROR processing sensor event "
                    f"topic={topic} partition={partition} offset={offset}: {exc}"
                )

    finally:
        print("Closing alert consumer...")
        consumer.close()


if __name__ == "__main__":
    main()
