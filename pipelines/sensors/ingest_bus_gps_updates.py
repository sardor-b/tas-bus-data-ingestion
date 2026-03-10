import os
import asyncio

from dagster import job
from datetime import datetime
from zoneinfo import ZoneInfo
from dagster import sensor, SensorEvaluationContext, SkipReason
from src.api_loader.bus_gps_updates import ingest_bus_gps_updates

source = os.environ["SOURCE"]
source_url = os.environ["SOURCE_BUS_GPS_UPDATES"]
host = os.environ.get("PSQL_HOST", "172.29.172.1")
port = os.environ.get("PSQL_PORT", "5432")
database = os.environ.get("PSQL_DATABASE", "main")
user = os.environ["PSQL_USER"]
password = os.environ["PSQL_PASSWORD"]

@job
def gps_ingestion_job():
    pass

@sensor(job=gps_ingestion_job, minimum_interval_seconds=8)
def gps_streaming_sensor(context: SensorEvaluationContext):
    """
    This sensor acts as a persistent 'daemon' process.
    """

    tz = ZoneInfo("Etc/GMT-5")
    current_time = datetime.now(tz)
    current_hour = current_time.hour

    # Skip the dead hour
    if 1 <= current_hour < 3:
        return SkipReason("Outside operating hours (1-3 AM GMT+5)")

    asyncio.run(
        ingest_bus_gps_updates(
            source=source,
            source_url=source_url,
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    )
    return None