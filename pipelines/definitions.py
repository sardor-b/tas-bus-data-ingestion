from dagster import Definitions
from pipelines.jobs.ingest_bus_routes import bus_routes_job, bus_routes_schedule
from pipelines.jobs.ingest_bus_stations import bus_stations_job, bus_stations_schedule
from pipelines.sensors.ingest_bus_gps_updates import gps_streaming_sensor, gps_ingestion_job

defs = Definitions(
    jobs=[
        bus_routes_job,
        bus_stations_job,
        gps_ingestion_job
    ],
    sensors=[
        gps_streaming_sensor
    ],
    schedules=[
        bus_routes_schedule,
        bus_stations_schedule
    ]
)