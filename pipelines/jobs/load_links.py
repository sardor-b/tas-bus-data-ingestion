import os
import asyncio
import logging

from dagster import op, job, OpExecutionContext, ScheduleDefinition
from src.dds_loader.load_links import (
    l_route_bus,
    l_route_station,
    truncate_stg_bus_updates
)

os.makedirs("logs", exist_ok=True)
logger = logging.getLogger(__name__)

source = os.environ["SOURCE"]
host = os.environ.get("PSQL_HOST", "172.29.172.1")
port = os.environ.get("PSQL_PORT", "5432")
database = os.environ.get("PSQL_DATABASE", "main")
user = os.environ["PSQL_USER"]
password = os.environ["PSQL_PASSWORD"]


@op
def load_l_route_bus(context: OpExecutionContext):
    asyncio.run(
        l_route_bus(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    )


@op
def load_l_route_station(context: OpExecutionContext):
    asyncio.run(
        l_route_station(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    )


@op
def op_truncate_stg_bus_gps_updates(context: OpExecutionContext, wait):
    # asyncio.run(
    #     truncate_stg_bus_updates(
    #         host=host,
    #         port=port,
    #         database=database,
    #         user=user,
    #         password=password
    #     )
    # )
    logger.info("TRUNCATED")


@job
def load_links_job():
    action_1 = load_l_route_bus(), load_l_route_station()

    op_truncate_stg_bus_gps_updates(action_1)


links_load_schedule = ScheduleDefinition(
    job=load_links_job,
    cron_schedule="30 2 * * *",  # 02:30 AM
    name="load_links_daily_local",
    execution_timezone="Asia/Tashkent", # GMT+5 timezone
)