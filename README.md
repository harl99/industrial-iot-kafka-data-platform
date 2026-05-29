# Industrial IoT Kafka Data Platform

A portfolio-grade industrial IoT data engineering project that simulates an industrial / IoT data platform using FastAPI, Kafka, PostgreSQL, Docker, local Data Lake patterns, and Streamlit.

The goal of this project is to demonstrate a realistic event-driven data engineering architecture where multiple industrial sources generate events, a REST API receives JSON payloads, Kafka distributes events through topics, and downstream consumers process, persist, alert, and simulate actuator actions.

---

## 1. Business Problem

Industrial environments generate continuous operational data from sensors, machines, maintenance systems, and human operators.

A company may need to answer questions such as:

- Are machines operating within safe thresholds?
- Which sensors are reporting critical values?
- Are maintenance events being tracked properly?
- Can alerts be generated in near real time?
- Can actuator commands be triggered based on industrial events?
- Can operational data be stored for analytics and reporting?

This project simulates that problem using a local data engineering platform.

---

## 2. Architecture

~~~text
Sensor / Industrial System Producers
                ↓
        FastAPI REST API
                ↓
        Kafka Producer
                ↓
          Kafka Topics
                ↓
       Multiple Consumers
                ↓
PostgreSQL / Local Data Lake / Alerts / Actuators / Dashboard
~~~

Current first version:

~~~text
Client / Industrial System
        ↓
FastAPI REST API
        ↓
Kafka Producer
        ↓
Kafka Topics
        ↓
Kafka UI for verification
~~~

---

## 3. Technologies

| Technology | Purpose |
|---|---|
| Python | Main programming language |
| FastAPI | REST API for receiving industrial JSON events |
| Pydantic | Payload validation and schema enforcement |
| Apache Kafka | Event streaming platform |
| Confluent Kafka Python Client | Kafka producer and consumer integration |
| PostgreSQL | Structured relational storage |
| Docker Compose | Local orchestration |
| Kafka UI | Kafka topic and message visualization |
| Streamlit | Dashboard application |
| Local folders | Simulated Data Lake |
| GitHub | Portfolio and version control |

---

## 4. Data Flow

1. External industrial systems or simulators send JSON data to FastAPI endpoints.
2. FastAPI validates the payload using Pydantic models.
3. FastAPI wraps each payload into a standard event envelope.
4. The internal Kafka producer publishes the event to the correct Kafka topic.
5. Kafka stores the event in a topic partition.
6. Consumers will later read from Kafka topics.
7. Consumers will write data to PostgreSQL, local Data Lake folders, alerts, actuator simulations, and dashboard tables.

---

## 5. Current REST API Endpoints

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/health` | Check API status |
| POST | `/sensor-readings` | Receive sensor readings |
| POST | `/maintenance-events` | Receive maintenance, inspection, failure, or repair events |
| POST | `/operator-events` | Receive manual operator actions |
| POST | `/actuator-command` | Receive actuator commands |

---

## 6. Kafka Topics

| Topic | Purpose |
|---|---|
| `sensor-readings` | Raw sensor events |
| `maintenance-events` | Maintenance, inspection, failure, and repair events |
| `operator-events` | Manual operator actions |
| `alerts` | Alerts generated from sensor thresholds |
| `actuator-commands` | Commands sent to simulated actuators |
| `processed-events` | General processed output events |

---

## 7. Local Services

| Service | URL / Connection |
|---|---|
| FastAPI Docs | http://localhost:8000/docs |
| FastAPI Health | http://localhost:8000/health |
| Kafka UI | http://localhost:8080 |
| PostgreSQL | localhost:5433 |
| Kafka external listener | localhost:9094 |
| Kafka internal Docker listener | kafka:9092 |

---

## 8. How to Run Locally

Start the full local stack:

~~~bash
docker compose up --build
~~~

Stop the services:

~~~bash
docker compose down
~~~

Stop services and delete local Kafka/PostgreSQL volumes:

~~~bash
docker compose down -v
~~~

Warning: `docker compose down -v` deletes local Kafka and PostgreSQL data.

---

## 9. API Test Examples

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
    "description": "Routine inspection completed. No abnormal vibration detected.",
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

## 10. PostgreSQL Tables

The database initialization script creates the following tables:

| Table | Purpose |
|---|---|
| `sensor_readings` | Stores structured sensor readings |
| `maintenance_events` | Stores maintenance and inspection records |
| `operator_events` | Stores operator actions |
| `alerts` | Stores generated alerts |
| `actuator_commands` | Stores actuator commands |

---

## 11. Local Data Lake Structure

The project simulates a local Data Lake using folders:

~~~text
data_lake/
├── raw/
│   ├── sensor_readings/
│   ├── maintenance_events/
│   └── operator_events/
└── processed/
    └── alerts/
~~~

This will later be used by the Data Lake consumer to store JSON or Parquet files.

---

## 12. Kafka Concepts Explained

### Broker

A Kafka broker is a server that stores and serves Kafka topic data. In this project, the local Kafka container acts as the broker.

### Topic

A topic is a logical stream of events. For example, `sensor-readings` stores sensor events, while `maintenance-events` stores maintenance events.

### Partition

A partition is a subdivision of a Kafka topic. Partitions allow Kafka to scale because multiple consumers can process different partitions in parallel.

### Offset

An offset is the position of a message inside a Kafka partition. Consumers use offsets to track what they have already processed.

### Producer

A producer writes events to Kafka. In the current version, the FastAPI service acts as a Kafka producer.

### Consumer

A consumer reads events from Kafka. Future consumers in this project will store events in PostgreSQL, generate alerts, write to the Data Lake, simulate actuators, and feed a dashboard.

### Consumer Group

A consumer group is a group of consumers that share the processing workload for a topic. Kafka ensures that each partition is read by only one consumer within the same group.

### Message Retention

Kafka can retain messages for a configured amount of time or storage size. This allows consumers to replay historical events if needed.

### Event Streaming

Event streaming means processing data continuously as events occur. This is different from batch processing, where data is processed periodically in large chunks.

---

## 13. Azure Mapping

Even though this project runs locally, the architecture is cloud-ready.

| Local Component | Azure Equivalent |
|---|---|
| Kafka | Azure Event Hubs / Confluent Cloud on Azure |
| PostgreSQL | Azure SQL Database / Azure Database for PostgreSQL |
| Local `data_lake/` folder | Azure Blob Storage / Azure Data Lake Storage Gen2 |
| FastAPI local service | Azure App Service / Azure Container Apps |
| Consumers | Azure Functions / Azure Container Apps / Databricks Jobs |
| Streamlit dashboard | Power BI / Azure App Service |
| Docker Compose | Azure Container Apps / AKS |

---

## 14. Interview Talking Points

### REST API

I built a FastAPI REST API that receives industrial IoT events as JSON payloads. Each endpoint represents a different industrial event source, such as sensors, maintenance systems, operator input, or actuator commands.

### Pydantic Validation

I used Pydantic models to validate incoming JSON payloads before publishing them to Kafka. This prevents malformed data from entering the streaming pipeline.

### Kafka Producer

The FastAPI service acts as a Kafka producer. Once a payload is validated, it is wrapped in a standard event envelope and published to the appropriate Kafka topic.

### Kafka Topics

I separated Kafka topics by event domain. This makes the architecture easier to scale, monitor, and extend.

### Event Envelope

Each event includes metadata such as:

- `event_id`
- `event_type`
- `event_timestamp`
- `source`
- `payload`

This makes events easier to trace, audit, and process downstream.

### SQL Sink

A SQL consumer will read Kafka events and store structured records in PostgreSQL tables for querying and reporting.

### Data Lake Sink

A Data Lake consumer will write raw and processed events into local folders, simulating how files would be stored in Azure Data Lake Storage.

### Alerting

An alert consumer will detect threshold violations such as high temperature, high pressure, high vibration, low flow rate, or high energy consumption.

### Actuator Simulation

An actuator consumer will simulate industrial responses such as activating a cooling fan, opening a pressure relief valve, triggering an alarm siren, creating a maintenance ticket, or performing an emergency shutdown.

### Streaming vs Batch

This project demonstrates streaming because events are processed continuously as they arrive. Batch processing would process accumulated data at scheduled intervals.

### Azure Equivalent Architecture

This local architecture maps directly to Azure services such as Event Hubs, Azure Functions, Azure Container Apps, Azure SQL Database, Azure Data Lake Storage, Stream Analytics, and Power BI.

---

## 15. Planned Improvements

- Add sensor simulator producer with 5 industrial sensors
- Add maintenance event producer
- Add operator input producer
- Add SQL consumer
- Add alert consumer
- Add actuator consumer
- Add local Data Lake consumer
- Add Streamlit dashboard
- Add anomaly detection consumer
- Add unit tests
- Add screenshots
- Add architecture diagram
- Add Power BI / Azure architecture documentation
- Add CI/CD workflow

---

## 16. Commit Plan

Planned Git commits:

1. Initial project structure
2. Add FastAPI REST endpoints
3. Add Kafka producer
4. Add sensor simulators
5. Add SQL consumer
6. Add alert consumer
7. Add actuator consumer
8. Add local data lake consumer
9. Add dashboard
10. Add README and screenshots

---

## 17. Security Notes

This project is designed to run locally and safely.

- No real credentials are used.
- No `.env` file should be committed.
- No production secrets should be stored in the repository.
- Docker Compose uses local demo credentials only.
- The project avoids unnecessary `sudo` usage.
- Runtime data files are ignored by Git.
- The project is compatible with Mac and Apple Silicon through Docker images.

---

## 18. Project Status

Current stage:

- Project structure created
- FastAPI service created
- Kafka service created
- PostgreSQL service created
- Kafka UI configured
- Initial Kafka topics configured
- REST endpoints publish events to Kafka

Next stage:

- Add sensor simulator producer
