import os
import asyncio
import logging

from datetime import datetime
from dagster import op, job, OpExecutionContext, ScheduleDefinition
from src.dds_loader.load_bus_movement import load_bus_movement, truncate_stg_bus_gps_updates

logger = logging.getLogger(__name__)

source = os.environ["SOURCE"]
host = os.environ.get("PSQL_HOST", "172.29.172.1")
port = os.environ.get("PSQL_PORT", "5432")
database = os.environ.get("PSQL_DATABASE", "main")
user = os.environ["PSQL_USER"]
password = os.environ["PSQL_PASSWORD"]


@op
def op_calc_bus_movement(context: OpExecutionContext):
    context.log.info(f"Starting bus_movement table load")

    asyncio.run(
        load_bus_movement(
            dt=datetime.now(),
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    )

    context.log.info("bus load completed successfully.")


@op
def op_truncate_stg_bus_gps_updates(context: OpExecutionContext, start_after: bool):
    context.log.info(f"Starting stg.bus_gps_updates table truncate")

    asyncio.run(
        truncate_stg_bus_gps_updates(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    )

    context.log.info("stg.bus_gps_updates truncate completed successfully.")


@job
def bus_movement_job():
    op_calc_bus_movement() >> op_truncate_stg_bus_gps_updates()


bus_movement_schedule = ScheduleDefinition(
    job=bus_movement_job,
    cron_schedule="0 21 * * *",
    name="calc_bus_movement_at_2AM_GMT5",
    execution_timezone="UTC",
)