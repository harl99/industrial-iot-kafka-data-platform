import json
import os
import socket
import uuid
from datetime import datetime, timezone
from typing import Literal, Optional

from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


app = FastAPI(
    title="Industrial IoT Kafka Data Platform",
    description="Local industrial IoT / Data Engineering simulation using FastAPI, Kafka, PostgreSQL and Data Lake patterns.",
    version="0.1.0",
)


producer_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": socket.gethostname(),
}

producer = Producer(producer_config)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def delivery_report(err, msg):
    if err is not None:
        print(f"Kafka delivery failed: {err}")
    else:
        print(
            f"Kafka message delivered to topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


def get_message_key(event: dict, event_id: str) -> str:
    """
    Choose a Kafka message key.

    For industrial IoT events, machine_id is preferred because it preserves
    ordering per machine. If machine_id is not available, event_id is used
    as a safe fallback.
    """
    return event.get("machine_id", event_id)


def publish_event(topic: str, event: dict) -> dict:
    try:
        event_id = str(uuid.uuid4())
        message_key = get_message_key(event, event_id)

        envelope = {
            "event_id": event_id,
            "event_type": topic,
            "event_timestamp": utc_now_iso(),
            "source": "fastapi-rest-api",
            "message_key": message_key,
            "payload": event,
        }

        producer.produce(
            topic=topic,
            key=message_key,
            value=json.dumps(envelope).encode("utf-8"),
            callback=delivery_report,
        )

        producer.poll(0)
        producer.flush(5)

        return envelope

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to publish event to Kafka topic '{topic}': {str(exc)}",
        )


class SensorReading(BaseModel):
    machine_id: str = Field(..., example="pump_01")
    sensor_id: str = Field(..., example="temperature_sensor_01")
    sensor_type: Literal[
        "temperature_sensor",
        "pressure_sensor",
        "vibration_sensor",
        "flow_sensor",
        "energy_sensor",
    ]
    value: float = Field(..., example=85.5)
    unit: str = Field(..., example="C")
    status: Literal["normal", "warning", "critical"] = "normal"


class MaintenanceEvent(BaseModel):
    machine_id: str = Field(..., example="compressor_02")
    event_category: Literal["maintenance", "inspection", "failure", "repair"]
    description: str = Field(..., example="Routine inspection completed.")
    technician: Optional[str] = Field(default=None, example="operator_01")
    severity: Literal["low", "medium", "high", "critical"] = "medium"


class OperatorEvent(BaseModel):
    machine_id: str = Field(..., example="line_03")
    operator_id: str = Field(..., example="operator_07")
    event_type: Literal[
        "alarm_acknowledged",
        "inspection_note",
        "shutdown_requested",
        "maintenance_override",
    ]
    notes: Optional[str] = Field(default=None, example="Operator acknowledged vibration alarm.")


class ActuatorCommand(BaseModel):
    machine_id: str = Field(..., example="pump_01")
    actuator_id: str = Field(..., example="cooling_fan_01")
    actuator_type: Literal[
        "alarm_siren",
        "cooling_fan",
        "emergency_shutdown",
        "pressure_relief_valve",
        "maintenance_ticket_system",
    ]
    command: Literal["activate", "deactivate", "open", "close", "create_ticket"]
    reason: str = Field(..., example="Temperature exceeded safe threshold.")


@app.get("/health")
def health_check():
    return {
        "status": "ok",
        "service": "industrial-iot-kafka-api",
        "kafka_bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "timestamp": utc_now_iso(),
    }


@app.post("/sensor-readings")
def create_sensor_reading(reading: SensorReading):
    event = publish_event("sensor-readings", reading.model_dump())
    return {
        "message": "Sensor reading accepted and published to Kafka.",
        "topic": "sensor-readings",
        "event": event,
    }


@app.post("/maintenance-events")
def create_maintenance_event(maintenance_event: MaintenanceEvent):
    event = publish_event("maintenance-events", maintenance_event.model_dump())
    return {
        "message": "Maintenance event accepted and published to Kafka.",
        "topic": "maintenance-events",
        "event": event,
    }


@app.post("/operator-events")
def create_operator_event(operator_event: OperatorEvent):
    event = publish_event("operator-events", operator_event.model_dump())
    return {
        "message": "Operator event accepted and published to Kafka.",
        "topic": "operator-events",
        "event": event,
    }


@app.post("/actuator-command")
def create_actuator_command(command: ActuatorCommand):
    event = publish_event("actuator-commands", command.model_dump())
    return {
        "message": "Actuator command accepted and published to Kafka.",
        "topic": "actuator-commands",
        "event": event,
    }
