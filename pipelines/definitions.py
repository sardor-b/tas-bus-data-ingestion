from dagster import Definitions
from pipelines.jobs.ingest_bus_routes import bus_routes_job, bus_routes_schedule

defs = Definitions(
    jobs=[bus_routes_job],
    schedules=[bus_routes_schedule],
)