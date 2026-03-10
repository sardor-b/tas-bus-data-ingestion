import os
import asyncio
import logging

from datetime import datetime
from src.api_loader.bus_stations import ingest_bus_stations
from dagster import op, job, OpExecutionContext, ScheduleDefinition

logger = logging.getLogger(__name__)

source = os.environ["SOURCE"]
source_url = os.environ["SOURCE_BUS_STATIONS"]
host = os.environ.get("PSQL_HOST", "172.29.172.1")
port = os.environ.get("PSQL_PORT", "5432")
database = os.environ.get("PSQL_DATABASE", "main")
user = os.environ["PSQL_USER"]
password = os.environ["PSQL_PASSWORD"]


@op
def op_ingest_bus_stations(context: OpExecutionContext) -> datetime:
    """
    Fetches bus stations from the configured API source
    and inserts them into the PostgreSQL staging table.

    Returns:
        datetime: Execution timestamp to be used by all downstream ops
    """
    execution_time = datetime.now()
    context.log.info(f"Starting bus station ingestion from: {source_url}")
    context.log.info(f"Execution timestamp: {execution_time}")

    asyncio.run(
        ingest_bus_stations(
            source=source,
            source_url=source_url,
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    )

    context.log.info("Bus station ingestion completed successfully.")

    return execution_time


@job
def bus_stations_job():
    op_ingest_bus_stations()


# Runs every day at 02:00 AM GMT+5 (= 21:00 UTC)
bus_routes_schedule = ScheduleDefinition(
    job=bus_stations_job,
    cron_schedule="0 21 * * *",
    name="bus_routes_daily_2am_gmt5",
    execution_timezone="UTC",
)