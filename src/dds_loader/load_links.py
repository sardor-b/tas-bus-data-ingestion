import os
import logging

from src.connectors import AsyncPostgresConnector

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/links.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


async def l_route_station(
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START dds.l_route_station load")

    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    before = await db.fetch('SELECT count(hk_route_station) FROM dds.l_route_station;')

    await db.execute(
        query="""
            INSERT INTO dds.l_route_station (hk_route_station, hk_route, hk_station, load_dt, load_source)
            WITH src AS (
                SELECT
                    object_value #>> '{route, id}' as route_id,
                    jsonb_array_elements((object_value #>> '{gpsStations}')::jsonb) as v
                FROM stg.bus_stations
                WHERE (object_value #>> '{route, id}')::int != 0
                UNION ALL
                SELECT
                    object_value #>> '{route, id}' as route_id,
                    jsonb_array_elements((object_value #>> '{gpsSecondStations}')::jsonb) as v
                FROM stg.bus_stations
                WHERE (object_value #>> '{route, id}')::int != 0
            ),
            route_stations AS (
                SELECT DISTINCT
                    route_id,
                    (v ->> 'stationId') as station_id
                FROM src
            )
            SELECT
                md5(
                    concat_ws(
                     '||',
                    route_id,
                    station_id
                    )
                ) as hk_route_station,
            
                md5(route_id) as hk_route,
                md5(station_id) as hk_station,
            
                now() as load_dt,
                'stg.bus_stations' as load_source
            FROM route_stations
            ON CONFLICT (hk_route_station)
                DO NOTHING
            ;
        """,
        params={}
    )

    after = await db.fetch('SELECT count(hk_route_station) FROM dds.l_route_station;')

    logger.info(f"END dds.l_route_station load. Loaded {after[0]['count'] - before[0]['count']} records.")


async def l_route_bus(
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START dds.l_route_bus load")

    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    before = await db.fetch('SELECT count(hk_route_bus) FROM dds.l_route_bus;')

    await db.execute(
        query="""
            insert into dds.l_route_bus
            with src as (
                select
                    jsonb_array_elements(object_value) as v
                from stg.bus_gps_updates
            ),
            route_bus_combinations as (
                select
                    v #>> '{bus, routeId}' as route_id,
                    v #>> '{bus, id}' as bus_id
                from src
            ),
            route_bus as (
                select distinct
                    route_id,
                    bus_id
                from route_bus_combinations
            )
            select
                md5(
                    concat_ws(
                     '||',
                    route_id,
                    bus_id
                    )
                ) as hk_route_bus,
            
                md5(route_id) as hk_route,
                md5(bus_id) as hk_bus,
            
                now() as load_dt,
                'stg.bus_gps_updates' as load_source
            from route_bus
            on conflict (hk_route_bus)
                do nothing
            ;
        """,
        params={}
    )

    after = await db.fetch('SELECT count(hk_route_bus) FROM dds.l_route_bus;')

    logger.info(f"END dds.l_route_bus load. Loaded {after[0]['count'] - before[0]['count']} records.")


async def truncate_stg_bus_updates(
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START stg.bus_gps_updates truncate")

    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    await db.execute(
        query="TRUNCATE stg.bus_gps_updates;",
        params={}
    )

    logger.info("END stg.bus_gps_updates truncate")
