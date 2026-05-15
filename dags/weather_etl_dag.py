"""
ETL Weather Pipeline
====================
This DAG extracts weather data from the Open-Meteo API,
transforms it, and loads it into PostgreSQL.
"""

from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

LATITUDE = 12.9716
LONGITUDE = 77.5946

POSTGRES_CONN_ID = "postgres_etl_conn"
HTTP_CONN_ID = "open_meteo_api"

# ─────────────────────────────────────────────
# DEFAULT DAG ARGUMENTS
# ─────────────────────────────────────────────

default_args = {
    "owner": "data_team",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

# ─────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────

with DAG(
    dag_id="weather_etl_pipeline",
    description="ETL Weather Pipeline using Airflow",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["etl", "weather", "postgres"],
) as dag:

    # ─────────────────────────────────────────
    # TASK 1 — EXTRACT
    # ─────────────────────────────────────────

    @task()
    def extract_weather_data():

        http_hook = HttpHook(
            http_conn_id=HTTP_CONN_ID,
            method="GET"
        )

        endpoint = (
            f"/v1/forecast"
            f"?latitude={LATITUDE}"
            f"&longitude={LONGITUDE}"
            f"&current_weather=true"
            f"&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"
        )

        response = http_hook.run(endpoint)

        if response.status_code == 200:
            data = response.json()

            print("✅ Weather data extracted successfully")

            return data

        raise Exception(
            f"❌ API request failed with status: {response.status_code}"
        )

    # ─────────────────────────────────────────
    # TASK 2 — TRANSFORM
    # ─────────────────────────────────────────

    @task()
    def transform_weather_data(raw_data: dict):

        current = raw_data.get("current_weather", {})
        hourly = raw_data.get("hourly", {})

        current_record = {
            "record_type": "current",
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "temperature_c": current.get("temperature"),
            "wind_speed_kmh": current.get("windspeed"),
            "wind_direction_deg": current.get("winddirection"),
            "weather_code": current.get("weathercode"),
            "timestamp": current.get("time"),
        }

        records = [current_record]

        times = hourly.get("time", [])[:5]
        temps = hourly.get("temperature_2m", [])[:5]
        wind_speeds = hourly.get("wind_speed_10m", [])[:5]

        for i in range(len(times)):

            records.append({
                "record_type": "hourly_forecast",
                "latitude": LATITUDE,
                "longitude": LONGITUDE,
                "temperature_c": temps[i] if i < len(temps) else None,
                "wind_speed_kmh": wind_speeds[i] if i < len(wind_speeds) else None,
                "wind_direction_deg": None,
                "weather_code": None,
                "timestamp": times[i],
            })

        print(f"✅ Transformed {len(records)} records")

        return records

    # ─────────────────────────────────────────
    # TASK 3 — LOAD
    # ─────────────────────────────────────────

    @task()
    def load_weather_data(transformed_records: list):

        pg_hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                id SERIAL PRIMARY KEY,
                record_type VARCHAR(30),
                latitude FLOAT,
                longitude FLOAT,
                temperature_c FLOAT,
                wind_speed_kmh FLOAT,
                wind_direction_deg FLOAT,
                weather_code INT,
                timestamp VARCHAR(50),
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        insert_query = """
            INSERT INTO weather_data (
                record_type,
                latitude,
                longitude,
                temperature_c,
                wind_speed_kmh,
                wind_direction_deg,
                weather_code,
                timestamp
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """

        for record in transformed_records:

            cursor.execute(
                insert_query,
                (
                    record["record_type"],
                    record["latitude"],
                    record["longitude"],
                    record["temperature_c"],
                    record["wind_speed_kmh"],
                    record["wind_direction_deg"],
                    record["weather_code"],
                    record["timestamp"],
                )
            )

        conn.commit()

        cursor.close()
        conn.close()

        print(
            f"✅ Loaded {len(transformed_records)} records into PostgreSQL"
        )

    # ─────────────────────────────────────────
    # DAG EXECUTION FLOW
    # ─────────────────────────────────────────

    raw_weather_data = extract_weather_data()

    transformed_weather_data = transform_weather_data(
        raw_weather_data
    )

    load_weather_data(transformed_weather_data)