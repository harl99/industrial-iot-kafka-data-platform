import json
import os
import signal
import socket
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

from confluent_kafka import Consumer, KafkaException, Producer


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONSUMER_GROUP_ID = os.getenv("ACTUATOR_CONSUMER_GROUP_ID", "actuator-consumer-group")

INPUT_TOPIC = "actuator-commands"
OUTPUT_TOPIC = "processed-events"

running = True


def handle_shutdown(signum, frame):
    global running
    print("Shutdown signal received. Stopping actuator consumer...")
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
        "client.id": f"actuator-consumer-{socket.gethostname()}",
    }
    return Producer(config)


def delivery_report(err, msg):
    if err is not None:
        print(f"Processed event delivery failed: {err}")
    else:
        print(
            f"Processed event delivered to topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


def simulate_alarm_siren(command_payload: Dict[str, Any]) -> Dict[str, Any]:
    command = command_payload["command"]

    if command == "activate":
        result = "Alarm siren activated."
        actuator_state = "active"
    elif command == "deactivate":
        result = "Alarm siren deactivated."
        actuator_state = "inactive"
    else:
        result = f"Unsupported command for alarm siren: {command}"
        actuator_state = "unchanged"

    return {
        "action_result": result,
        "actuator_state": actuator_state,
    }


def simulate_cooling_fan(command_payload: Dict[str, Any]) -> Dict[str, Any]:
    command = command_payload["command"]

    if command == "activate":
        result = "Cooling fan activated to reduce machine temperature."
        actuator_state = "active"
    elif command == "deactivate":
        result = "Cooling fan deactivated."
        actuator_state = "inactive"
    else:
        result = f"Unsupported command for cooling fan: {command}"
        actuator_state = "unchanged"

    return {
        "action_result": result,
        "actuator_state": actuator_state,
    }


def simulate_emergency_shutdown(command_payload: Dict[str, Any]) -> Dict[str, Any]:
    command = command_payload["command"]

    if command == "activate":
        result = "Emergency shutdown sequence initiated."
        actuator_state = "shutdown_active"
    elif command == "deactivate":
        result = "Emergency shutdown reset requested."
        actuator_state = "shutdown_reset"
    else:
        result = f"Unsupported command for emergency shutdown: {command}"
        actuator_state = "unchanged"

    return {
        "action_result": result,
        "actuator_state": actuator_state,
    }


def simulate_pressure_relief_valve(command_payload: Dict[str, Any]) -> Dict[str, Any]:
    command = command_payload["command"]

    if command == "open":
        result = "Pressure relief valve opened."
        actuator_state = "open"
    elif command == "close":
        result = "Pressure relief valve closed."
        actuator_state = "closed"
    else:
        result = f"Unsupported command for pressure relief valve: {command}"
        actuator_state = "unchanged"

    return {
        "action_result": result,
        "actuator_state": actuator_state,
    }


def simulate_maintenance_ticket_system(command_payload: Dict[str, Any]) -> Dict[str, Any]:
    command = command_payload["command"]

    if command == "create_ticket":
        ticket_id = f"TICKET-{uuid.uuid4().hex[:8].upper()}"
        result = f"Maintenance ticket created: {ticket_id}"
        actuator_state = "ticket_created"
    else:
        ticket_id = None
        result = f"Unsupported command for maintenance ticket system: {command}"
        actuator_state = "unchanged"

    return {
        "action_result": result,
        "actuator_state": actuator_state,
        "ticket_id": ticket_id,
    }


def simulate_actuator(command_event: Dict[str, Any]) -> Dict[str, Any]:
    payload = command_event["payload"]
    actuator_type = payload["actuator_type"]

    if actuator_type == "alarm_siren":
        simulation_result = simulate_alarm_siren(payload)
    elif actuator_type == "cooling_fan":
        simulation_result = simulate_cooling_fan(payload)
    elif actuator_type == "emergency_shutdown":
        simulation_result = simulate_emergency_shutdown(payload)
    elif actuator_type == "pressure_relief_valve":
        simulation_result = simulate_pressure_relief_valve(payload)
    elif actuator_type == "maintenance_ticket_system":
        simulation_result = simulate_maintenance_ticket_system(payload)
    else:
        simulation_result = {
            "action_result": f"Unsupported actuator type: {actuator_type}",
            "actuator_state": "unsupported",
        }

    processed_event_id = str(uuid.uuid4())

    return {
        "event_id": processed_event_id,
        "event_type": "processed-events",
        "event_timestamp": utc_now_iso(),
        "source": "actuator-consumer",
        "message_key": payload["machine_id"],
        "payload": {
            "source_event_id": command_event["event_id"],
            "source_event_timestamp": command_event["event_timestamp"],
            "machine_id": payload["machine_id"],
            "actuator_id": payload["actuator_id"],
            "actuator_type": payload["actuator_type"],
            "command": payload["command"],
            "reason": payload["reason"],
            "action_result": simulation_result.get("action_result"),
            "actuator_state": simulation_result.get("actuator_state"),
            "ticket_id": simulation_result.get("ticket_id"),
        },
    }


def publish_processed_event(producer: Producer, processed_event: Dict[str, Any]):
    producer.produce(
        topic=OUTPUT_TOPIC,
        key=processed_event["message_key"],
        value=json.dumps(processed_event).encode("utf-8"),
        callback=delivery_report,
    )
    producer.poll(0)
    producer.flush(5)


def main():
    print("Starting actuator consumer")
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
                command_event = json.loads(msg.value().decode("utf-8"))
                processed_event = simulate_actuator(command_event)
                publish_processed_event(producer, processed_event)

                payload = processed_event["payload"]

                print(
                    f"ACTUATOR processed "
                    f"machine_id={payload['machine_id']} "
                    f"actuator_type={payload['actuator_type']} "
                    f"command={payload['command']} "
                    f"state={payload['actuator_state']} "
                    f"result={payload['action_result']} "
                    f"source_topic={topic} partition={partition} offset={offset}"
                )

                consumer.commit(message=msg)

            except Exception as exc:
                print(
                    f"ERROR processing actuator command "
                    f"topic={topic} partition={partition} offset={offset}: {exc}"
                )

    finally:
        print("Closing actuator consumer...")
        consumer.close()


if __name__ == "__main__":
    main()
