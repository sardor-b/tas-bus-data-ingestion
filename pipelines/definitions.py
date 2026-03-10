from dagster import Definitions
from pipelines.jobs.ingest_bus_routes import bus_routes_job, bus_routes_schedule
from pipelines.jobs.ingest_bus_stations import bus_stations_job, bus_stations_schedule

defs = Definitions(
    jobs=[
        bus_routes_job,
        bus_stations_job
    ],
    schedules=[
        bus_routes_schedule,
        bus_stations_job
    ]
)