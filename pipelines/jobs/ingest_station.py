import os
import asyncio
import logging
from asyncio import wait_for

from src.api_loader.bus_stations import ingest_bus_stations
from dagster import op, job, OpExecutionContext, ScheduleDefinition

from src.dds_loader.load_station import (
    h_station,
    s_station_location,
    s_station_name
)

os.makedirs("logs", exist_ok=True)
logger = logging.getLogger(__name__)

source = os.environ["SOURCE"]
source_url = os.environ["SOURCE_BUS_STATIONS"]
host = os.environ.get("PSQL_HOST", "172.29.172.1")
port = os.environ.get("PSQL_PORT", "5432")
database = os.environ.get("PSQL_DATABASE", "main")
user = os.environ["PSQL_USER"]
password = os.environ["PSQL_PASSWORD"]


@op
def op_ingest_station(context: OpExecutionContext):
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


@op
def load_h_station(context: OpExecutionContext, wait_for):
    asyncio.run(
        h_station(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    )

@op
def load_s_station_location(context: OpExecutionContext, wait_for):
    asyncio.run(
        s_station_location(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    )

@op
def load_s_station_name(context: OpExecutionContext, wait_for_for):
    asyncio.run(
        s_station_name(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    )


@job
def ingest_station_job():
    action_1 = op_ingest_station()

    action_2 = h_station(wait_for=action_1)

    s_station_name(wait_for=action_2)
    s_station_location(wait_for=action_2)


# Runs every day at 01:00 AM GMT+5 (= 20:00 UTC)
bus_stations_schedule = ScheduleDefinition(
    job=ingest_station_job,
    cron_schedule="0 20 * * *",
    name="ingest stations daily @ 1AM UTC +5",
    execution_timezone="UTC",
)