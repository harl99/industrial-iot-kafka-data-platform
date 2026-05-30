import json
import os
import random
import time
from urllib import request, error


API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
INTERVAL_SECONDS = float(os.getenv("OPERATOR_INTERVAL_SECONDS", "3"))
EVENT_LIMIT = int(os.getenv("OPERATOR_EVENT_LIMIT", "25"))


MACHINES = [
    "pump_01",
    "pump_02",
    "compressor_01",
    "compressor_02",
    "motor_01",
    "line_01",
]


OPERATORS = [
    "operator_01",
    "operator_02",
    "operator_03",
    "operator_04",
    "operator_07",
    "shift_supervisor_01",
]


EVENT_TEMPLATES = [
    {
        "event_type": "alarm_acknowledged",
        "notes": "Operator acknowledged active alarm and started field verification.",
    },
    {
        "event_type": "inspection_note",
        "notes": "Operator added inspection note after visual equipment review.",
    },
    {
        "event_type": "shutdown_requested",
        "notes": "Operator requested controlled shutdown due to abnormal operating condition.",
    },
    {
        "event_type": "maintenance_override",
        "notes": "Operator applied temporary maintenance override after supervisor approval.",
    },
    {
        "event_type": "inspection_note",
        "notes": "Operator reported unusual noise and recommended follow-up maintenance.",
    },
    {
        "event_type": "alarm_acknowledged",
        "notes": "Operator confirmed alarm was reviewed and escalation was not required.",
    },
]


def build_operator_event(sequence_number: int) -> dict:
    machine_id = random.choice(MACHINES)
    operator_id = random.choice(OPERATORS)
    template = random.choice(EVENT_TEMPLATES)

    return {
        "machine_id": machine_id,
        "operator_id": operator_id,
        "event_type": template["event_type"],
        "notes": template["notes"],
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
    print("Starting industrial operator input producer")
    print(f"API_BASE_URL={API_BASE_URL}")
    print(f"INTERVAL_SECONDS={INTERVAL_SECONDS}")
    print(f"EVENT_LIMIT={EVENT_LIMIT}")
    print("-" * 80)

    for sequence_number in range(1, EVENT_LIMIT + 1):
        event = build_operator_event(sequence_number)

        try:
            response = post_json("/operator-events", event)
            kafka_event = response.get("event", {})

            print(
                f"[{sequence_number:03d}] "
                f"sent event_type={event['event_type']} "
                f"machine_id={event['machine_id']} "
                f"operator_id={event['operator_id']} "
                f"kafka_event_id={kafka_event.get('event_id')} "
                f"message_key={kafka_event.get('message_key')}"
            )

        except Exception as exc:
            print(f"[{sequence_number:03d}] ERROR: {exc}")

        time.sleep(INTERVAL_SECONDS)

    print("-" * 80)
    print("Operator input simulation completed")


if __name__ == "__main__":
    main()
