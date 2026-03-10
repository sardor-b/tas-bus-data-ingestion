import os
import asyncio

from dagster import sensor, SensorEvaluationContext, RunRequest, repository
from src.api_loader.bus_gps_updates import ingest_bus_gps_updates

source = os.environ["SOURCE"]
source_url = os.environ["SOURCE_BUS_ROUTES"]
host = os.environ.get("PSQL_HOST", "172.29.172.1")
port = os.environ.get("PSQL_PORT", "5432")
database = os.environ.get("PSQL_DATABASE", "main")
user = os.environ["PSQL_USER"]
password = os.environ["PSQL_PASSWORD"]

@sensor(job_name="my_gps_ingestion_job")
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