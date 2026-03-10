import os
import asyncio
import logging

from datetime import datetime
from src.api_loader.bus_stations import ingest_bus_stations
from src.dds_loader.load_bus_stations import load_stations, load_route_path
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


@op
def op_load_routes_path(context: OpExecutionContext, execution_time: datetime):
    """
    Loads route ID and name mappings into dds.routes_path table.

    Args:
        execution_time: Timestamp from the ingestion op
    """
    context.log.info(f"Starting routes table load for timestamp: {execution_time}")

    asyncio.run(
        load_route_path(
            dt=execution_time,
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    )

    context.log.info("routes path load completed successfully.")


@op
def op_load_stations(context: OpExecutionContext, execution_time: datetime):
    """
    Loads route ID and name mappings into dds.stations table.

    Args:
        execution_time: Timestamp from the ingestion op
    """
    context.log.info(f"Starting stations table load for timestamp: {execution_time}")

    asyncio.run(
        load_stations(
            dt=execution_time,
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    )

    context.log.info("stations load completed successfully.")


@job
def bus_stations_job():
    """
    ETL job that:
    1. Ingests bus stations from API to staging
    2. Loads dds.stations, dds.routes_path

    All transformation steps use the same execution timestamp for data consistency.
    """
    # First, ingest raw data from API and get execution timestamp
    execution_time = op_ingest_bus_stations()

    op_load_routes_path(execution_time)
    op_load_stations(execution_time)


# Runs every day at 03:00 AM GMT+5 (= 21:00 UTC)
bus_routes_schedule = ScheduleDefinition(
    job=bus_stations_job,
    cron_schedule="0 22 * * *",
    name="bus_stations_daily_3am_gmt5",
    execution_timezone="UTC",
)