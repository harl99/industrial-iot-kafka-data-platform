import json
import os
import random
import time
from datetime import datetime, timezone
from urllib import request, error


API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
INTERVAL_SECONDS = float(os.getenv("SIMULATION_INTERVAL_SECONDS", "2"))
EVENT_LIMIT = int(os.getenv("SIMULATION_EVENT_LIMIT", "50"))


MACHINES = [
    "pump_01",
    "pump_02",
    "compressor_01",
    "compressor_02",
    "motor_01",
    "line_01",
]


SENSOR_CONFIGS = [
    {
        "sensor_type": "temperature_sensor",
        "unit": "C",
        "normal_min": 60,
        "normal_max": 95,
        "warning_min": 96,
        "warning_max": 100,
        "critical_min": 101,
        "critical_max": 120,
    },
    {
        "sensor_type": "pressure_sensor",
        "unit": "bar",
        "normal_min": 70,
        "normal_max": 110,
        "warning_min": 111,
        "warning_max": 120,
        "critical_min": 121,
        "critical_max": 150,
    },
    {
        "sensor_type": "vibration_sensor",
        "unit": "g",
        "normal_min": 0.10,
        "normal_max": 0.60,
        "warning_min": 0.61,
        "warning_max": 0.80,
        "critical_min": 0.81,
        "critical_max": 1.20,
    },
    {
        "sensor_type": "flow_sensor",
        "unit": "L/min",
        "normal_min": 35,
        "normal_max": 80,
        "warning_min": 20,
        "warning_max": 34,
        "critical_min": 5,
        "critical_max": 19,
    },
    {
        "sensor_type": "energy_sensor",
        "unit": "kWh",
        "normal_min": 150,
        "normal_max": 420,
        "warning_min": 421,
        "warning_max": 500,
        "critical_min": 501,
        "critical_max": 750,
    },
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def choose_status() -> str:
    """
    Most industrial readings should be normal.
    A smaller percentage should be warnings or critical values.
    """
    return random.choices(
        population=["normal", "warning", "critical"],
        weights=[0.75, 0.15, 0.10],
        k=1,
    )[0]


def generate_value(sensor_config: dict, status: str) -> float:
    if status == "normal":
        value = random.uniform(sensor_config["normal_min"], sensor_config["normal_max"])
    elif status == "warning":
        value = random.uniform(sensor_config["warning_min"], sensor_config["warning_max"])
    else:
        value = random.uniform(sensor_config["critical_min"], sensor_config["critical_max"])

    if sensor_config["sensor_type"] == "vibration_sensor":
        return round(value, 3)

    return round(value, 2)


def build_sensor_event(sequence_number: int) -> dict:
    machine_id = random.choice(MACHINES)
    sensor_config = random.choice(SENSOR_CONFIGS)
    status = choose_status()
    value = generate_value(sensor_config, status)

    sensor_type = sensor_config["sensor_type"]
    sensor_id = f"{machine_id}_{sensor_type}_{random.randint(1, 3)}"

    return {
        "machine_id": machine_id,
        "sensor_id": sensor_id,
        "sensor_type": sensor_type,
        "value": value,
        "unit": sensor_config["unit"],
        "status": status,
    }


def post_json(endpoint: str, payload: dict) -> dict:
    url = f"{API_BASE_URL}{endpoint}"
    data = json.dumps(payload).encode("utf-8")

    req = request.Request(
        url=url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with request.urlopen(req, timeout=10) as response:
            response_body = response.read().decode("utf-8")
            return json.loads(response_body)

    except error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        raise RuntimeError(f"HTTP error {exc.code}: {body}") from exc

    except error.URLError as exc:
        raise RuntimeError(f"Could not connect to API at {url}: {exc}") from exc


def main():
    print("Starting industrial sensor simulator")
    print(f"API_BASE_URL={API_BASE_URL}")
    print(f"INTERVAL_SECONDS={INTERVAL_SECONDS}")
    print(f"EVENT_LIMIT={EVENT_LIMIT}")
    print("-" * 80)

    for sequence_number in range(1, EVENT_LIMIT + 1):
        event = build_sensor_event(sequence_number)

        try:
            response = post_json("/sensor-readings", event)
            kafka_event = response.get("event", {})

            print(
                f"[{sequence_number:03d}] "
                f"sent sensor_type={event['sensor_type']} "
                f"machine_id={event['machine_id']} "
                f"value={event['value']} {event['unit']} "
                f"status={event['status']} "
                f"kafka_event_id={kafka_event.get('event_id')} "
                f"message_key={kafka_event.get('message_key')}"
            )

        except Exception as exc:
            print(f"[{sequence_number:03d}] ERROR: {exc}")

        time.sleep(INTERVAL_SECONDS)

    print("-" * 80)
    print("Sensor simulation completed")


if __name__ == "__main__":
    main()
