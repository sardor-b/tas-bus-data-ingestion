import os
import asyncio
import logging

from datetime import datetime
from src.api_loader.bus_routes import ingest_bus_routes
from src.dds_loader.load_bus_routes import (
    load_route_id_name,
    load_route_origin_destination,
    load_route_fleet,
    load_route_start_end_time
)
from dagster import op, job, OpExecutionContext, ScheduleDefinition

logger = logging.getLogger(__name__)

source = os.environ["SOURCE"]
source_url = os.environ["SOURCE_BUS_ROUTES"]
host = os.environ.get("PSQL_HOST", "172.29.172.1")
port = os.environ.get("PSQL_PORT", "5432")
database = os.environ.get("PSQL_DATABASE", "main")
user = os.environ["PSQL_USER"]
password = os.environ["PSQL_PASSWORD"]


@op
def op_ingest_bus_routes(context: OpExecutionContext) -> datetime:
    """
    Fetches bus routes from the configured API source
    and inserts them into the PostgreSQL staging table.

    Returns:
        datetime: Execution timestamp to be used by all downstream ops
    """
    execution_time = datetime.now()
    context.log.info(f"Starting bus route ingestion from: {source_url}")
    context.log.info(f"Execution timestamp: {execution_time}")

    asyncio.run(
        ingest_bus_routes(
            source=source,
            source_url=source_url,
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    )

    context.log.info("Bus route ingestion completed successfully.")
    return execution_time


@op
def op_load_route_id_name(context: OpExecutionContext, execution_time: datetime):
    """
    Loads route ID and name mappings into dds.route_id_name table.

    Args:
        execution_time: Timestamp from the ingestion op
    """
    context.log.info(f"Starting route_id_name table load for timestamp: {execution_time}")

    asyncio.run(
        load_route_id_name(
            dt=execution_time,
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    )

    context.log.info("route_id_name load completed successfully.")


@op
def op_load_route_origin_destination(context: OpExecutionContext, execution_time: datetime):
    """
    Loads route origin and destination data into dds.route_origin_destination table.

    Args:
        execution_time: Timestamp from the ingestion op
    """
    context.log.info(f"Starting route_origin_destination table load for timestamp: {execution_time}")

    asyncio.run(
        load_route_origin_destination(
            dt=execution_time,
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    )

    context.log.info("route_origin_destination load completed successfully.")


@op
def op_load_route_fleet(context: OpExecutionContext, execution_time: datetime):
    """
    Loads route fleet information into dds.route_fleet table.

    Args:
        execution_time: Timestamp from the ingestion op
    """
    context.log.info(f"Starting route_fleet table load for timestamp: {execution_time}")

    asyncio.run(
        load_route_fleet(
            dt=execution_time,
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    )

    context.log.info("route_fleet load completed successfully.")


@op
def op_load_route_start_end_time(context: OpExecutionContext, execution_time: datetime):
    """
    Loads route start and end times into dds.route_start_end_time table.

    Args:
        execution_time: Timestamp from the ingestion op
    """
    context.log.info(f"Starting route_start_end_time table load for timestamp: {execution_time}")

    asyncio.run(
        load_route_start_end_time(
            dt=execution_time,
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    )

    context.log.info("route_start_end_time load completed successfully.")


@job
def bus_routes_job():
    """
    ETL job that:
    1. Ingests bus routes from API to staging
    2. Loads route_id_name mappings
    3. Loads route origin/destination data
    4. Loads route fleet information
    5. Loads route start/end times

    All transformation steps use the same execution timestamp for data consistency.
    """
    # First, ingest raw data from API and get execution timestamp
    execution_time = op_ingest_bus_routes()

    execution_time_after_ingestion = op_load_route_id_name(execution_time)

    op_load_route_origin_destination(execution_time_after_ingestion)
    op_load_route_fleet(execution_time_after_ingestion)
    op_load_route_start_end_time(execution_time_after_ingestion)


# Runs every day at 02:00 AM GMT+5 (= 21:00 UTC)
bus_routes_schedule = ScheduleDefinition(
    job=bus_routes_job,
    cron_schedule="0 21 * * *",
    name="bus_routes_daily_2am_gmt5",
    execution_timezone="UTC",
)