import json
import os
import random
import time
from urllib import request, error


API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
INTERVAL_SECONDS = float(os.getenv("MAINTENANCE_INTERVAL_SECONDS", "3"))
EVENT_LIMIT = int(os.getenv("MAINTENANCE_EVENT_LIMIT", "25"))


MACHINES = [
    "pump_01",
    "pump_02",
    "compressor_01",
    "compressor_02",
    "motor_01",
    "line_01",
]


TECHNICIANS = [
    "technician_01",
    "technician_02",
    "maintenance_lead_01",
    "operator_03",
    "operator_07",
]


EVENT_TEMPLATES = [
    {
        "event_category": "inspection",
        "severity": "low",
        "description": "Routine inspection completed. No critical abnormal condition detected.",
    },
    {
        "event_category": "maintenance",
        "severity": "medium",
        "description": "Preventive maintenance performed according to scheduled plan.",
    },
    {
        "event_category": "failure",
        "severity": "high",
        "description": "Equipment failure detected. Machine requires immediate diagnostic review.",
    },
    {
        "event_category": "repair",
        "severity": "medium",
        "description": "Repair completed. Equipment returned to operational condition.",
    },
    {
        "event_category": "failure",
        "severity": "critical",
        "description": "Critical failure reported. Production line may require shutdown.",
    },
    {
        "event_category": "inspection",
        "severity": "medium",
        "description": "Inspection found early signs of wear. Follow-up maintenance recommended.",
    },
]


def build_maintenance_event(sequence_number: int) -> dict:
    machine_id = random.choice(MACHINES)
    template = random.choice(EVENT_TEMPLATES)
    technician = random.choice(TECHNICIANS)

    return {
        "machine_id": machine_id,
        "event_category": template["event_category"],
        "description": template["description"],
        "technician": technician,
        "severity": template["severity"],
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
    print("Starting industrial maintenance event producer")
    print(f"API_BASE_URL={API_BASE_URL}")
    print(f"INTERVAL_SECONDS={INTERVAL_SECONDS}")
    print(f"EVENT_LIMIT={EVENT_LIMIT}")
    print("-" * 80)

    for sequence_number in range(1, EVENT_LIMIT + 1):
        event = build_maintenance_event(sequence_number)

        try:
            response = post_json("/maintenance-events", event)
            kafka_event = response.get("event", {})

            print(
                f"[{sequence_number:03d}] "
                f"sent category={event['event_category']} "
                f"machine_id={event['machine_id']} "
                f"severity={event['severity']} "
                f"technician={event['technician']} "
                f"kafka_event_id={kafka_event.get('event_id')} "
                f"message_key={kafka_event.get('message_key')}"
            )

        except Exception as exc:
            print(f"[{sequence_number:03d}] ERROR: {exc}")

        time.sleep(INTERVAL_SECONDS)

    print("-" * 80)
    print("Maintenance event simulation completed")


if __name__ == "__main__":
    main()
