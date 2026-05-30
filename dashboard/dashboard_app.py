import os

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "industrial_iot")
POSTGRES_USER = os.getenv("POSTGRES_USER", "iot_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "iot_password")


st.set_page_config(
    page_title="Industrial IoT Kafka Data Platform",
    page_icon="🏭",
    layout="wide",
)


def get_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


@st.cache_data(ttl=5)
def load_query(query: str) -> pd.DataFrame:
    with get_connection() as connection:
        return pd.read_sql_query(query, connection)


def load_counts() -> pd.DataFrame:
    query = """
    SELECT 'sensor_readings' AS event_type, COUNT(*) AS total_events FROM sensor_readings
    UNION ALL
    SELECT 'maintenance_events' AS event_type, COUNT(*) AS total_events FROM maintenance_events
    UNION ALL
    SELECT 'operator_events' AS event_type, COUNT(*) AS total_events FROM operator_events
    UNION ALL
    SELECT 'alerts' AS event_type, COUNT(*) AS total_events FROM alerts
    UNION ALL
    SELECT 'actuator_commands' AS event_type, COUNT(*) AS total_events FROM actuator_commands;
    """
    return load_query(query)


def load_latest_sensor_readings() -> pd.DataFrame:
    query = """
    SELECT
        event_timestamp,
        machine_id,
        sensor_id,
        sensor_type,
        value,
        unit,
        status
    FROM sensor_readings
    ORDER BY inserted_at DESC
    LIMIT 100;
    """
    return load_query(query)


def load_alerts() -> pd.DataFrame:
    query = """
    SELECT
        event_timestamp,
        machine_id,
        sensor_type,
        alert_type,
        severity,
        observed_value,
        threshold_value
    FROM alerts
    ORDER BY inserted_at DESC
    LIMIT 100;
    """
    return load_query(query)


def load_actuator_commands() -> pd.DataFrame:
    query = """
    SELECT
        event_timestamp,
        machine_id,
        actuator_id,
        actuator_type,
        command,
        reason
    FROM actuator_commands
    ORDER BY inserted_at DESC
    LIMIT 100;
    """
    return load_query(query)


def load_machine_status() -> pd.DataFrame:
    query = """
    WITH latest_sensor AS (
        SELECT DISTINCT ON (machine_id)
            machine_id,
            event_timestamp AS latest_sensor_timestamp,
            sensor_type,
            value,
            unit,
            status
        FROM sensor_readings
        ORDER BY machine_id, inserted_at DESC
    ),
    alert_counts AS (
        SELECT
            machine_id,
            COUNT(*) AS total_alerts,
            MAX(event_timestamp) AS latest_alert_timestamp
        FROM alerts
        GROUP BY machine_id
    ),
    actuator_counts AS (
        SELECT
            machine_id,
            COUNT(*) AS total_actuator_commands,
            MAX(event_timestamp) AS latest_actuator_timestamp
        FROM actuator_commands
        GROUP BY machine_id
    )
    SELECT
        ls.machine_id,
        ls.latest_sensor_timestamp,
        ls.sensor_type AS latest_sensor_type,
        ls.value AS latest_sensor_value,
        ls.unit AS latest_sensor_unit,
        ls.status AS latest_sensor_status,
        COALESCE(ac.total_alerts, 0) AS total_alerts,
        ac.latest_alert_timestamp,
        COALESCE(act.total_actuator_commands, 0) AS total_actuator_commands,
        act.latest_actuator_timestamp
    FROM latest_sensor ls
    LEFT JOIN alert_counts ac ON ls.machine_id = ac.machine_id
    LEFT JOIN actuator_counts act ON ls.machine_id = act.machine_id
    ORDER BY total_alerts DESC, ls.machine_id;
    """
    return load_query(query)


def main():
    st.title("Industrial IoT Kafka Data Platform")
    st.caption("FastAPI → Kafka → Consumers → PostgreSQL / Data Lake / Alerts / Actuators")

    try:
        counts_df = load_counts()
        sensors_df = load_latest_sensor_readings()
        alerts_df = load_alerts()
        actuators_df = load_actuator_commands()
        machine_status_df = load_machine_status()

    except Exception as exc:
        st.error(f"Could not connect to PostgreSQL or load data: {exc}")
        st.stop()

    total_sensor_events = int(counts_df.loc[counts_df["event_type"] == "sensor_readings", "total_events"].iloc[0])
    total_maintenance_events = int(counts_df.loc[counts_df["event_type"] == "maintenance_events", "total_events"].iloc[0])
    total_operator_events = int(counts_df.loc[counts_df["event_type"] == "operator_events", "total_events"].iloc[0])
    total_alerts = int(counts_df.loc[counts_df["event_type"] == "alerts", "total_events"].iloc[0])
    total_actuator_commands = int(counts_df.loc[counts_df["event_type"] == "actuator_commands", "total_events"].iloc[0])

    col1, col2, col3, col4, col5 = st.columns(5)

    col1.metric("Sensor Readings", total_sensor_events)
    col2.metric("Maintenance Events", total_maintenance_events)
    col3.metric("Operator Events", total_operator_events)
    col4.metric("Alerts", total_alerts)
    col5.metric("Actuator Commands", total_actuator_commands)

    st.divider()

    left_col, right_col = st.columns(2)

    with left_col:
        st.subheader("Events by Type")
        fig = px.bar(
            counts_df,
            x="event_type",
            y="total_events",
            title="Event Volume by Category",
        )
        st.plotly_chart(fig, use_container_width=True)

    with right_col:
        st.subheader("Alerts by Type")
        if alerts_df.empty:
            st.info("No alerts available yet.")
        else:
            alert_counts = (
                alerts_df.groupby("alert_type")
                .size()
                .reset_index(name="total_alerts")
                .sort_values("total_alerts", ascending=False)
            )
            fig = px.bar(
                alert_counts,
                x="alert_type",
                y="total_alerts",
                title="Alerts by Rule",
            )
            st.plotly_chart(fig, use_container_width=True)

    st.divider()

    st.subheader("Machine Status")
    st.dataframe(machine_status_df, use_container_width=True)

    st.subheader("Latest Sensor Readings")
    st.dataframe(sensors_df, use_container_width=True)

    st.subheader("Latest Alerts")
    st.dataframe(alerts_df, use_container_width=True)

    st.subheader("Latest Actuator Commands")
    st.dataframe(actuators_df, use_container_width=True)


if __name__ == "__main__":
    main()
