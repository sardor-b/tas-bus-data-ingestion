import os
import asyncio

from dagster import job
from dagster import sensor, SensorEvaluationContext
from src.api_loader.bus_gps_updates import ingest_bus_gps_updates

source = os.environ["SOURCE"]
source_url = os.environ["SOURCE_BUS_ROUTES"]
host = os.environ.get("PSQL_HOST", "172.29.172.1")
port = os.environ.get("PSQL_PORT", "5432")
database = os.environ.get("PSQL_DATABASE", "main")
user = os.environ["PSQL_USER"]
password = os.environ["PSQL_PASSWORD"]

@job
def gps_ingestion_job():
    # If you just want the sensor to do the work,
    # this job can even be empty or contain a 'pass' op.
    pass

@sensor(job=gps_ingestion_job, minimum_interval_seconds=5)
def gps_streaming_sensor(context: SensorEvaluationContext):
    """
    This sensor acts as a persistent 'daemon' process.
    """
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