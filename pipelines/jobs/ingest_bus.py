import os
import asyncio
import logging

from dagster import op, job, OpExecutionContext, ScheduleDefinition
from src.dds_loader.load_bus import (
    h_bus,
    s_bus_garage_number,
    s_bus_license_plate,
    s_bus_model,
    s_bus_movement
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
def load_h_bus(context: OpExecutionContext):
    asyncio.run(
        h_bus(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    )

@op
def load_s_bus_garage_number(context: OpExecutionContext, wait):
    asyncio.run(
        s_bus_garage_number(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    )

@op
def load_s_bus_license_plate(context: OpExecutionContext, wait):
    asyncio.run(
        s_bus_license_plate(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    )

@op
def load_s_bus_model(context: OpExecutionContext, wait):
    asyncio.run(
        s_bus_model(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    )

@op
def load_s_bus_movement(context: OpExecutionContext, wait):
    asyncio.run(
        s_bus_movement(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    )

@job
def ingest_bus_job():
    action_1 = load_h_bus

    load_s_bus_license_plate(wait=action_1)
    load_s_bus_model(wait=action_1)
    load_s_bus_movement(wait=action_1)


bus_schedule = ScheduleDefinition(
    job=ingest_bus_job,
    cron_schedule="0 21 * * *",
    name="ingest buses daily @ 2AM UTC +5",
    execution_timezone="UTC",
)