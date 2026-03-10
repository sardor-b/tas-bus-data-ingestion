import os
import asyncio
import logging

from datetime import datetime
from src.dds_loader.load_bus import load_bus
from dagster import op, job, OpExecutionContext, ScheduleDefinition

logger = logging.getLogger(__name__)

source = os.environ["SOURCE"]
host = os.environ.get("PSQL_HOST", "172.29.172.1")
port = os.environ.get("PSQL_PORT", "5432")
database = os.environ.get("PSQL_DATABASE", "main")
user = os.environ["PSQL_USER"]
password = os.environ["PSQL_PASSWORD"]


@op
def op_load_bus(context: OpExecutionContext):
    context.log.info(f"Starting bus table load")

    asyncio.run(
        load_bus(
            dt=datetime.now(),
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    )

    context.log.info("bus load completed successfully.")


@job
def bus_job():
    op_load_bus()


bus_schedule = ScheduleDefinition(
    job=bus_job,
    cron_schedule="0 20 * * *",
    name="load_bus_info_at_1AM_GMT5",
    execution_timezone="UTC",
)