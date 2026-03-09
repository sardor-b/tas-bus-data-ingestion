import os
import asyncio
import logging

from src.api_loader.bus_routes import ingest_bus_routes
from dagster import op, job, OpExecutionContext, ScheduleDefinition

logger = logging.getLogger(__name__)


@op
def run(context: OpExecutionContext):
    """
    Fetches bus routes from the configured API source
    and inserts them into the PostgreSQL staging table.
    """

    source = os.environ["SOURCE"]
    source_url = os.environ["SOURCE_BUS_ROUTES"]
    host = os.environ.get("PSQL_HOST", "172.29.172.1")
    port = os.environ.get("PSQL_PORT", "5432")
    database = os.environ.get("PSQL_DATABASE", "main")
    user = os.environ["PSQL_USER"]
    password = os.environ["PSQL_PASSWORD"]

    context.log.info(f"Starting bus route ingestion from: {source_url}")

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


@job
def bus_routes_job():
    run()


# Runs every day at 02:00 AM GMT+5 (= 21:00 UTC)
bus_routes_schedule = ScheduleDefinition(
    job=bus_routes_job,
    cron_schedule="0 21 * * *",
    name="bus_routes_daily_2am_gmt5",
    execution_timezone="UTC",
)