from dagster import Definitions
from pipelines.jobs.ingest_bus import ingest_bus_job, bus_schedule
from pipelines.jobs.ingest_route import ingest_route_job, bus_routes_schedule
from pipelines.jobs.ingest_station import ingest_station_job, bus_stations_schedule

defs = Definitions(
    jobs=[
        ingest_bus_job,
        ingest_route_job,
        ingest_station_job,
    ],
    schedules=[
        bus_schedule,
        bus_routes_schedule,
        bus_stations_schedule,
    ]
)