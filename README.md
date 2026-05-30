# Industrial IoT Kafka Data Platform

A portfolio-grade industrial IoT data engineering platform built locally with FastAPI, Apache Kafka, PostgreSQL, Docker, a local Data Lake pattern, and Streamlit.

This project simulates a realistic industrial / IoT event-driven architecture where multiple sources generate events, a REST API receives JSON payloads, Kafka distributes events through topics, consumers process messages, PostgreSQL stores structured operational data, a local Data Lake stores raw and processed files, alerts are generated from sensor thresholds, actuator actions are simulated, and a Streamlit dashboard visualizes the system.

---

## Business Problem

Industrial environments generate continuous operational data from sensors, machines, maintenance systems, operator actions, alarms, and actuator commands.

A company needs to answer questions such as:

- Are machines operating within safe thresholds?
- Which sensors are reporting critical values?
- Are maintenance events being tracked?
- Are operators acknowledging alarms or requesting shutdowns?
- Can alerts be generated in near real time?
- Can actuator commands be simulated or triggered safely?
- Can events be stored for dashboards, analytics, auditing, and future machine learning?

This project demonstrates how an event-driven data engineering architecture can solve that problem.

---

## High-Level Architecture

~~~text
Sensor Simulator Producer
Maintenance Event Producer
Operator Input Producer
Manual API Requests
        ↓
FastAPI REST API
        ↓
Kafka Producer
        ↓
Kafka Topics
        ↓
Consumers
        ↓
PostgreSQL / Local Data Lake / Alerts / Actuator Simulation / Dashboard
~~~

---

## Data Flow

~~~text
1. Producers generate industrial events.
2. FastAPI receives JSON payloads through REST endpoints.
3. Pydantic validates incoming payloads.
4. FastAPI wraps payloads into a standard event envelope.
5. Kafka producer publishes events to Kafka topics.
6. Consumers subscribe to topics using consumer groups.
7. SQL consumer persists structured events into PostgreSQL.
8. Alert consumer detects threshold violations and publishes alerts.
9. Actuator consumer simulates industrial actuator actions.
10. Data Lake consumer writes raw and processed events as JSON files.
11. Streamlit dashboard reads PostgreSQL and visualizes the platform.
~~~

---

## Technologies

| Technology | Purpose |
|---|---|
| Python | Main programming language |
| FastAPI | REST API for event ingestion |
| Pydantic | JSON payload validation |
| Apache Kafka | Event streaming platform |
| Confluent Kafka Python Client | Kafka producer and consumer integration |
| PostgreSQL | Structured relational storage |
| Docker Compose | Local orchestration |
| Kafka UI | Kafka topic/message inspection |
| Streamlit | Operational dashboard |
| Plotly | Dashboard visualizations |
| Local folders | Simulated Data Lake |
| Git / GitHub | Version control and portfolio delivery |

---

## Repository Structure

~~~text
industrial-iot-kafka-data-platform/
├── app/
│   ├── Dockerfile
│   ├── __init__.py
│   └── main.py
├── producers/
│   ├── Dockerfile
│   ├── sensor_simulator.py
│   ├── maintenance_event_producer.py
│   └── operator_input_producer.py
├── consumers/
│   ├── Dockerfile
│   ├── sql_consumer.py
│   ├── alert_consumer.py
│   ├── actuator_consumer.py
│   └── data_lake_consumer.py
├── database/
│   └── init.sql
├── dashboard/
│   ├── Dockerfile
│   └── dashboard_app.py
├── data_lake/
│   ├── raw/
│   │   ├── sensor_readings/
│   │   ├── maintenance_events/
│   │   └── operator_events/
│   └── processed/
│       ├── alerts/
│       ├── actuator_commands/
│       └── processed_events/
├── docs/
├── reports/
├── tests/
├── docker-compose.yml
├── requirements.txt
├── README.md
└── .gitignore
~~~

---

## REST API Endpoints

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/health` | Health check |
| POST | `/sensor-readings` | Receive industrial sensor readings |
| POST | `/maintenance-events` | Receive maintenance, inspection, failure, and repair events |
| POST | `/operator-events` | Receive manual operator actions |
| POST | `/actuator-command` | Receive actuator commands |

---

## Kafka Topics

| Topic | Purpose |
|---|---|
| `sensor-readings` | Raw sensor telemetry |
| `maintenance-events` | Maintenance, inspection, failure, repair events |
| `operator-events` | Manual operator actions |
| `alerts` | Threshold-based alert events |
| `actuator-commands` | Commands sent to simulated actuators |
| `processed-events` | Results from actuator processing |

---

## Producers

### Sensor Simulator Producer

Generates realistic industrial sensor events for:

- `temperature_sensor`
- `pressure_sensor`
- `vibration_sensor`
- `flow_sensor`
- `energy_sensor`

The simulator sends JSON payloads to:

~~~text
POST /sensor-readings
~~~

### Maintenance Event Producer

Generates industrial maintenance events such as:

- `maintenance`
- `inspection`
- `failure`
- `repair`

The producer sends JSON payloads to:

~~~text
POST /maintenance-events
~~~

### Operator Input Producer

Generates manual operator events such as:

- `alarm_acknowledged`
- `inspection_note`
- `shutdown_requested`
- `maintenance_override`

The producer sends JSON payloads to:

~~~text
POST /operator-events
~~~

---

## Consumers

### SQL Consumer

Reads Kafka events and persists structured records into PostgreSQL.

Subscribed topics:

- `sensor-readings`
- `maintenance-events`
- `operator-events`
- `alerts`
- `actuator-commands`

Persistence flow:

~~~text
Kafka topic → SQL consumer → PostgreSQL table
~~~

The consumer commits the database transaction before committing the Kafka offset.

### Alert Consumer

Reads from:

~~~text
sensor-readings
~~~

Publishes to:

~~~text
alerts
~~~

Alert rules:

| Sensor Type | Rule | Alert Type |
|---|---|---|
| `temperature_sensor` | value > 100 | `high_temperature` |
| `pressure_sensor` | value > 120 | `high_pressure` |
| `vibration_sensor` | value > 0.8 | `high_vibration` |
| `flow_sensor` | value < 20 | `low_flow_rate` |
| `energy_sensor` | value > 500 | `high_energy_consumption` |

### Actuator Consumer

Reads from:

~~~text
actuator-commands
~~~

Publishes results to:

~~~text
processed-events
~~~

Simulated actuators:

- `alarm_siren`
- `cooling_fan`
- `emergency_shutdown`
- `pressure_relief_valve`
- `maintenance_ticket_system`

### Local Data Lake Consumer

Reads Kafka topics and writes JSON files into partitioned local folders.

Example path:

~~~text
data_lake/processed/alerts/year=2026/month=05/day=30/hour=01/
~~~

This simulates a cloud Data Lake pattern.

---

## PostgreSQL Tables

| Table | Purpose |
|---|---|
| `sensor_readings` | Stores structured sensor readings |
| `maintenance_events` | Stores maintenance, inspection, failure, and repair records |
| `operator_events` | Stores operator actions |
| `alerts` | Stores generated alert records |
| `actuator_commands` | Stores actuator command records |

Each table also stores the original `raw_event` JSON for traceability, debugging, replay, and audit purposes.

---

## Local Data Lake Layout

~~~text
data_lake/
├── raw/
│   ├── sensor_readings/
│   ├── maintenance_events/
│   └── operator_events/
└── processed/
    ├── alerts/
    ├── actuator_commands/
    └── processed_events/
~~~

Raw events are stored under `raw/`.

Derived or processed events are stored under `processed/`.

---

## Dashboard

The Streamlit dashboard connects to PostgreSQL and visualizes:

- Total sensor readings
- Total maintenance events
- Total operator events
- Total alerts
- Total actuator commands
- Event volume by category
- Alerts by type
- Machine-level status
- Latest sensor readings
- Latest alerts
- Latest actuator commands

Dashboard URL:

~~~text
http://localhost:8501
~~~

---

## Local Services

| Service | URL / Connection |
|---|---|
| FastAPI Docs | http://localhost:8000/docs |
| FastAPI Health | http://localhost:8000/health |
| Kafka UI | http://localhost:8080 |
| Streamlit Dashboard | http://localhost:8501 |
| PostgreSQL | localhost:5433 |
| Kafka external listener | localhost:9094 |
| Kafka internal Docker listener | kafka:9092 |

---

## How to Run Locally

Start core services:

~~~bash
docker compose up --build -d
~~~

Start only the dashboard:

~~~bash
docker compose up --build -d dashboard
~~~

Run sensor simulator:

~~~bash
docker compose --profile producers run --rm sensor-simulator
~~~

Run maintenance producer:

~~~bash
docker compose --profile producers run --rm maintenance-producer
~~~

Run operator producer:

~~~bash
docker compose --profile producers run --rm operator-producer
~~~

Run SQL consumer:

~~~bash
docker compose --profile consumers run --rm sql-consumer
~~~

Run alert consumer:

~~~bash
docker compose --profile consumers run --rm alert-consumer
~~~

Run actuator consumer:

~~~bash
docker compose --profile consumers run --rm actuator-consumer
~~~

Run Data Lake consumer:

~~~bash
docker compose --profile consumers run --rm data-lake-consumer
~~~

Stop services:

~~~bash
docker compose down
~~~

Stop services and delete local Docker volumes:

~~~bash
docker compose down -v
~~~

Warning: `docker compose down -v` deletes local Kafka and PostgreSQL data.

---

## API Test Examples

### Health Check

~~~bash
curl http://localhost:8000/health
~~~

### Sensor Reading

~~~bash
curl -X POST http://localhost:8000/sensor-readings \
  -H "Content-Type: application/json" \
  -d '{
    "machine_id": "pump_01",
    "sensor_id": "temperature_sensor_01",
    "sensor_type": "temperature_sensor",
    "value": 105.7,
    "unit": "C",
    "status": "critical"
  }'
~~~

### Maintenance Event

~~~bash
curl -X POST http://localhost:8000/maintenance-events \
  -H "Content-Type: application/json" \
  -d '{
    "machine_id": "compressor_02",
    "event_category": "inspection",
    "description": "Routine inspection completed.",
    "technician": "operator_01",
    "severity": "low"
  }'
~~~

### Operator Event

~~~bash
curl -X POST http://localhost:8000/operator-events \
  -H "Content-Type: application/json" \
  -d '{
    "machine_id": "line_03",
    "operator_id": "operator_07",
    "event_type": "alarm_acknowledged",
    "notes": "Operator acknowledged high temperature alarm."
  }'
~~~

### Actuator Command

~~~bash
curl -X POST http://localhost:8000/actuator-command \
  -H "Content-Type: application/json" \
  -d '{
    "machine_id": "pump_01",
    "actuator_id": "cooling_fan_01",
    "actuator_type": "cooling_fan",
    "command": "activate",
    "reason": "Temperature exceeded safe threshold."
  }'
~~~

---

## Useful Verification Commands

Check running containers:

~~~bash
docker ps
~~~

Check Kafka topic offsets:

~~~bash
docker exec -it industrial_iot_kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic sensor-readings
~~~

Check PostgreSQL table counts:

~~~bash
docker exec -it industrial_iot_postgres psql -U iot_user -d industrial_iot -P pager=off -c "
SELECT 'sensor_readings' AS table_name, COUNT(*) FROM sensor_readings
UNION ALL
SELECT 'maintenance_events' AS table_name, COUNT(*) FROM maintenance_events
UNION ALL
SELECT 'operator_events' AS table_name, COUNT(*) FROM operator_events
UNION ALL
SELECT 'alerts' AS table_name, COUNT(*) FROM alerts
UNION ALL
SELECT 'actuator_commands' AS table_name, COUNT(*) FROM actuator_commands;
"
~~~

List Data Lake JSON files:

~~~bash
find data_lake -type f -name "*.json" | head -20
~~~

---

## Kafka Concepts Demonstrated

### Broker

A Kafka broker stores and serves topic data. In this project, the `industrial_iot_kafka` container acts as the local Kafka broker.

### Topic

A Kafka topic is a logical stream of events. This project uses separate topics for sensors, maintenance, operators, alerts, actuator commands, and processed events.

### Partition

A partition is a subdivision of a topic. Partitions allow parallelism and scalability.

### Offset

An offset is the position of a message inside a partition. Offsets are tracked per partition, not globally across the topic.

### Producer

A producer publishes events to Kafka. In this project, FastAPI acts as a Kafka producer after receiving REST payloads.

### Consumer

A consumer reads events from Kafka. This project includes SQL, alert, actuator, and Data Lake consumers.

### Consumer Group

A consumer group allows multiple consumers to share processing work. Each partition is assigned to one consumer within the same group.

### Message Key

This project uses `machine_id` as the Kafka message key when available. This helps preserve event ordering per machine because all events for the same machine are routed to the same partition.

### Message Retention

Kafka can retain messages for a configured time or size. This allows consumers to replay historical events.

### Event Streaming

Event streaming means processing data continuously as events occur, instead of waiting for scheduled batch jobs.

---

## Azure Mapping

Although this project runs locally, the architecture is cloud-ready.

| Local Component | Azure Equivalent |
|---|---|
| Apache Kafka | Azure Event Hubs / Confluent Cloud on Azure |
| PostgreSQL | Azure SQL Database / Azure Database for PostgreSQL |
| Local `data_lake/` folder | Azure Blob Storage / Azure Data Lake Storage Gen2 |
| FastAPI local service | Azure App Service / Azure Container Apps |
| Python consumers | Azure Functions / Azure Container Apps / Databricks Jobs |
| Streamlit dashboard | Power BI / Azure App Service |
| Docker Compose | Azure Container Apps / AKS |
| Kafka UI | Azure monitoring tools / Confluent Control Center / Event Hubs metrics |
| Local alert consumer | Azure Stream Analytics / Azure Functions |
| Local Data Lake consumer | Event Hubs Capture / Azure Functions / Databricks |

---

## Cloud Architecture Equivalent

~~~text
Industrial Systems / IoT Devices
        ↓
Azure App Service / Azure Container Apps
        ↓
Azure Event Hubs
        ↓
Azure Functions / Azure Stream Analytics / Databricks
        ↓
Azure SQL Database / Azure Database for PostgreSQL
        ↓
Azure Data Lake Storage Gen2
        ↓
Power BI
~~~

---

## Screenshots

Recommended screenshots to add:

~~~text
reports/screenshots/fastapi_docs.png
reports/screenshots/kafka_ui_topics.png
reports/screenshots/kafka_ui_messages.png
reports/screenshots/postgres_counts.png
reports/screenshots/streamlit_dashboard.png
reports/screenshots/data_lake_files.png
~~~

Suggested GitHub section:

| Screenshot | Description |
|---|---|
| FastAPI Docs | REST API endpoints and schemas |
| Kafka UI Topics | Kafka topics created locally |
| Kafka UI Messages | Example events in Kafka |
| PostgreSQL Counts | Events persisted in SQL tables |
| Streamlit Dashboard | Operational monitoring dashboard |
| Data Lake Files | Partitioned JSON event files |

---

## Interview Talking Points

### REST API

I built a FastAPI REST API that receives industrial IoT events as JSON payloads. Each endpoint represents a different event source, such as sensors, maintenance systems, operator actions, or actuator commands.

### Pydantic Validation

I used Pydantic to validate incoming JSON payloads before publishing to Kafka. This prevents malformed events from entering the streaming pipeline.

### Kafka Producer

The FastAPI service acts as a Kafka producer. After validation, it wraps each payload in a standard event envelope and publishes it to the appropriate Kafka topic.

### Event Envelope

Each event includes:

- `event_id`
- `event_type`
- `event_timestamp`
- `source`
- `message_key`
- `payload`

This makes the pipeline easier to trace, debug, and audit.

### Kafka Message Key

I use `machine_id` as the Kafka message key when available. This keeps events from the same machine in the same partition and helps preserve ordering per machine.

### SQL Sink

The SQL consumer persists Kafka events into PostgreSQL. It stores structured fields for querying and the raw JSON event for traceability.

### Offset Commit Strategy

The SQL consumer commits the PostgreSQL transaction first and then commits the Kafka offset. This helps avoid marking a message as processed before it has been stored successfully.

### Alerting

The alert consumer reads sensor events, applies threshold rules, and publishes alert events to the `alerts` topic.

### Actuator Simulation

The actuator consumer reads actuator commands, simulates industrial actions, and publishes the result to `processed-events`.

### Data Lake Sink

The Data Lake consumer writes raw and processed Kafka events as JSON files into partitioned local folders. This simulates Azure Data Lake Storage Gen2.

### Dashboard

The Streamlit dashboard reads PostgreSQL and displays event counts, alerts, actuator commands, latest sensor readings, and machine status.

### Streaming vs Batch

This project demonstrates streaming because data is processed continuously as events arrive. Batch processing would process accumulated data at scheduled intervals.

### Azure Equivalent Architecture

The local Kafka architecture can map to Azure Event Hubs, Azure Functions, Azure Container Apps, Azure SQL Database, Azure Data Lake Storage Gen2, and Power BI.

---

## Security Notes

This project is designed to run locally and safely.

- No real credentials are used.
- No `.env` file should be committed.
- No production secrets should be stored in the repository.
- Docker Compose uses local demo credentials only.
- Runtime Data Lake JSON files are ignored by Git.
- No unnecessary `sudo` commands are required.
- The project is compatible with Mac and Apple Silicon through Docker images.

---

## Future Improvements

- Add automated integration tests
- Add anomaly detection consumer using z-score
- Add Parquet output for the local Data Lake
- Add CI/CD workflow
- Add schema registry pattern
- Add dead-letter topic for failed events
- Add retry logic and error handling
- Add Power BI version of the dashboard
- Deploy to Azure Container Apps
- Replace local Kafka with Azure Event Hubs
- Replace local Data Lake with Azure Data Lake Storage Gen2
- Add Terraform or Bicep infrastructure templates

---

## Project Status

Current version includes:

- Local Docker architecture
- FastAPI REST ingestion
- Kafka topics
- Kafka producer integration
- Sensor simulator producer
- Maintenance event producer
- Operator input producer
- SQL consumer
- PostgreSQL persistence
- Alert consumer
- Actuator consumer
- Local Data Lake consumer
- Streamlit dashboard
- Azure mapping documentation

