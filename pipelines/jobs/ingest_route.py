import os
import asyncio
import logging

from src.api_loader.bus_routes import ingest_bus_routes
from dagster import op, job, OpExecutionContext, ScheduleDefinition

from src.dds_loader.load_route import (
    h_route,
    s_route_name,
    s_route_schedule,
    s_route_path_destination,
    s_route_path_origin
)

os.makedirs("logs", exist_ok=True)
logger = logging.getLogger(__name__)

source = os.environ["SOURCE"]
source_url = os.environ["SOURCE_BUS_ROUTES"]
host = os.environ.get("PSQL_HOST", "172.29.172.1")
port = os.environ.get("PSQL_PORT", "5432")
database = os.environ.get("PSQL_DATABASE", "main")
user = os.environ["PSQL_USER"]
password = os.environ["PSQL_PASSWORD"]


@op
def op_ingest_route(context: OpExecutionContext):
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


@op
def load_h_route(context: OpExecutionContext, wait):
    asyncio.run(
        h_route(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    )

@op
def load_s_route_name(context: OpExecutionContext, wait):
    asyncio.run(
        s_route_name(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    )

@op
def load_s_route_schedule(context: OpExecutionContext, wait):
    asyncio.run(
        s_route_schedule(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    )

@op
def load_s_route_path_destination(context: OpExecutionContext, wait):
    asyncio.run(
        s_route_path_destination(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    )

@op
def load_s_route_path_origin(context: OpExecutionContext, wait):
    asyncio.run(
        s_route_path_origin(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    )


@job
def ingest_route_job():
    action_1 = op_ingest_route()

    action_2 = load_h_route(wait=action_1)

    load_s_route_name(wait=action_2)
    load_s_route_schedule(wait=action_2)
    load_s_route_path_destination(wait=action_2)
    load_s_route_path_origin(wait=action_2)


bus_routes_schedule = ScheduleDefinition(
    job=ingest_route_job,
    cron_schedule="0 1 * * *",  # 01:00 AM
    name="ingest_routes_daily_local",
    execution_timezone="Asia/Tashkent", # GMT+5 timezone
)