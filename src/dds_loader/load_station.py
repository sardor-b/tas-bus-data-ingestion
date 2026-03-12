import os
import logging

from src.connectors import AsyncPostgresConnector

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/station.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


async def h_station(
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START dds.h_station load")

    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    before = await db.fetch('SELECT count(hk_station) FROM dds.h_station;')

    await db.execute(
        query="""
            INSERT INTO dds.h_station (hk_station, station_id, load_dt, load_source)
            WITH src AS (
                SELECT jsonb_array_elements((object_value #>> '{gpsStations}')::jsonb) as v
                FROM stg.bus_stations
                UNION
                SELECT jsonb_array_elements((object_value #>> '{gpsSecondStations}')::jsonb) as v
                FROM stg.bus_stations
            ),
            stations AS (
                SELECT DISTINCT ON ((v ->> 'stationId')::int)
                    (v ->> 'stationId')::int as station_id
                FROM src
            )
            SELECT
                md5(station_id::text) as hk_station,
            
                station_id,
            
                NOW() AS load_dt,
                'stg.bus_stations' AS load_source
            FROM stations
            ON CONFLICT (hk_station)
                DO NOTHING
            ;
        """,
        params={}
    )

    after = await db.fetch('SELECT count(hk_station) FROM dds.h_station;')

    logger.info(f"END dds.h_station load. Loaded {after[0]['count'] - before[0]['count']} records.")


async def s_station_location(
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START dds.s_station_location load")

    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    before = await db.fetch('SELECT count(hk_station) FROM dds.s_station_location;')

    await db.execute(
        query="""
            INSERT INTO dds.s_station_location (hk_station, location, load_dt, load_source, hash_diff)
            WITH src AS (
                SELECT jsonb_array_elements((object_value #>> '{gpsStations}')::jsonb) as v
                FROM stg.bus_stations
                UNION
                SELECT jsonb_array_elements((object_value #>> '{gpsSecondStations}')::jsonb) as v
                FROM stg.bus_stations
            ),
            latest AS (
                SELECT
                    hk_station,
                    hash_diff
                FROM dds.s_station_location
                ORDER BY hk_station
            ),
            stations AS (
                SELECT DISTINCT ON ((v ->> 'stationId')::int)
                    (v ->> 'stationId')::text as station_id,
                    ST_SetSRID(ST_MakePoint((v ->> 'lx')::float, (v ->> 'ly')::float), 4326) as location
                FROM src
            )
            SELECT
                md5(station_id) as hk_station,
            
                location,
            
                NOW() AS load_dt,
                'stg.bus_stations' AS load_source,
                MD5(
                    CONCAT_WS(
                        '||',
                        station_id,
                        st_asbinary(location)
                    )
                )
            FROM stations
            LEFT JOIN latest l ON l.hk_station = md5(station_id)
            WHERE
                MD5(
                    CONCAT_WS(
                        '||',
                        station_id,
                        st_asbinary(location)
                    )
                ) IS DISTINCT FROM l.hash_diff
            ;
        """,
        params={}
    )

    after = await db.fetch('SELECT count(hk_station) FROM dds.s_station_location;')

    logger.info(f"END dds.s_station_location load. Loaded {after[0]['count'] - before[0]['count']} records.")


async def s_station_name(
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START dds.s_station_name load")

    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    before = await db.fetch('SELECT count(hk_station) FROM dds.s_station_name;')

    await db.execute(
        query="""
            INSERT INTO dds.s_station_name (hk_station, name_uz, name_ru, load_dt, load_source, hash_diff)
            WITH src AS (
                SELECT jsonb_array_elements((object_value #>> '{gpsStations}')::jsonb) as v
                FROM stg.bus_stations
                UNION
                SELECT jsonb_array_elements((object_value #>> '{gpsSecondStations}')::jsonb) as v
                FROM stg.bus_stations
            ),
            latest AS (
                SELECT
                    hk_station,
                    hash_diff
                FROM dds.s_station_name
                ORDER BY hk_station
            ),
            stations AS (
                SELECT DISTINCT ON ((v ->> 'stationId')::int)
                    (v ->> 'stationId')::text as station_id,
                    v ->> 'uzName' as name_uz,
                    v ->> 'name' as name_ru
                FROM src
            )
            SELECT
                md5(station_id) as hk_station,
            
                name_uz,
                name_ru,
            
                NOW() AS load_dt,
                'stg.bus_stations' AS load_source,
                MD5(
                    CONCAT_WS(
                        '||',
                        station_id,
                        name_uz,
                        name_ru
                    )
                )
            FROM stations
            LEFT JOIN latest l ON l.hk_station = md5(station_id)
            WHERE
                MD5(
                    CONCAT_WS(
                       '||',
                        station_id,
                        name_uz,
                        name_ru
                    )
                ) IS DISTINCT FROM l.hash_diff
            ;
        """,
        params={}
    )

    after = await db.fetch('SELECT count(hk_station) FROM dds.s_station_name;')

    logger.info(f"END dds.s_station_name load. Loaded {after[0]['count'] - before[0]['count']} records.")
