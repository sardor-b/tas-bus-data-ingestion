import os
import logging

from datetime import datetime
from src.connectors import AsyncPostgresConnector

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/bus_stations.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


async def load_route_path(
    dt: datetime,
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START dds.routes_path load")

    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    before = await db.fetch('SELECT count(route_id) FROM dds.routes_path;')

    await db.execute(
        query="""
            INSERT INTO dds.routes_path (route_id, origin_path, destination_path)
            WITH new_geometries AS (
                SELECT
                    rin.id as route_id,
                    (SELECT ST_SetSRID(ST_MakeLine(ST_MakePoint((p.val->>'ly')::float, (p.val->>'lx')::float) ORDER BY ord), 4326)
                     FROM jsonb_array_elements(t.origin_raw) WITH ORDINALITY AS p(val, ord)) AS origin_path,
                    (SELECT ST_SetSRID(ST_MakeLine(ST_MakePoint((p.val->>'ly')::float, (p.val->>'lx')::float) ORDER BY ord), 4326)
                     FROM jsonb_array_elements(t.dest_raw) WITH ORDINALITY AS p(val, ord)) AS destination_path
                FROM (
                    SELECT DISTINCT ON ((object_value #>> '{route, id}')::int)
                        (object_value #>> '{route, id}')::int as route_id,
                        (object_value -> 'coordsOne')::jsonb as origin_raw,
                        (object_value -> 'coordsTwo')::jsonb as dest_raw
                    FROM stg.bus_stations
                    WHERE update_dt::date = (%(date)s)::date
                      AND (object_value #>> '{route, id}')::int != 0
                ) t
                INNER JOIN dds.routes rin ON t.route_id = rin.route_id
            )
            SELECT
                ng.route_id,
                ng.origin_path,
                ng.destination_path
            FROM new_geometries ng
            LEFT JOIN LATERAL (
                SELECT
                    route_id,
                    origin_path,
                    destination_path
                FROM dds.routes_path
                WHERE route_id = ng.route_id
                ORDER BY create_dt DESC
                LIMIT 1
            ) rf ON TRUE
            WHERE
                rf.route_id IS NULL  -- No previous record exists
                OR NOT ST_Equals(ng.origin_path, rf.origin_path)     -- Spatial comparison
                OR NOT ST_Equals(ng.destination_path, rf.destination_path); -- Spatial comparison
        """,
        params={
            'date': dt
        }
    )

    after = await db.fetch('SELECT count(route_id) FROM dds.routes_path;')

    logger.info(f"END dds.routes_path load. Loaded {after[0]['count'] - before[0]['count']} records.")


async def load_stations(
    dt: datetime,
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START dds.stations load")

    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    before = await db.fetch('SELECT count(station_id) FROM dds.stations;')

    await db.execute(
        query="""
            INSERT INTO dds.stations (station_id, station_name_uz, station_name_ru, location, direction)
            WITH raw_data AS (
                -- Unnest both arrays into a single set
                SELECT jsonb_array_elements((object_value #>> '{gpsStations}')::jsonb) as v 
                FROM stg.bus_stations
                WHERE update_dt::date = (%(date)s)::date 
                UNION ALL
                SELECT jsonb_array_elements((object_value #>> '{gpsSecondStations}')::jsonb) as v 
                FROM stg.bus_stations
                WHERE update_dt::date = (%(date)s)::date 
            ),
            new_stations AS (
                -- Deduplicate and extract fields
                SELECT DISTINCT ON ((v ->> 'stationId')::int)
                    (v ->> 'stationId')::int as station_id,
                    v ->> 'uzName' as station_name_uz,
                    v ->> 'name' as station_name_ru,
                    ST_SetSRID(ST_MakePoint((v ->> 'lx')::float, (v ->> 'ly')::float), 4326) as location,
                    case
                        when (v ->> 'direction')::int = 1 then 'origin'
                        when (v ->> 'direction')::int = 0 then 'destination'
                    end as direction
                FROM raw_data
            )
            SELECT
                ns.station_id,
                ns.station_name_uz,
                ns.station_name_ru,
                ns.location,
                ns.direction
            FROM new_stations ns
            LEFT JOIN LATERAL (
                -- Get the most recent version of this station from the target table
                SELECT station_id, station_name_uz, station_name_ru, location, direction
                FROM dds.stations
                WHERE station_id = ns.station_id
                ORDER BY create_dt DESC -- Assuming you are tracking history
                LIMIT 1
            ) s ON TRUE
            WHERE
                s.station_id IS NULL -- New record
                OR s.station_name_uz IS DISTINCT FROM ns.station_name_uz
                OR s.station_name_ru IS DISTINCT FROM ns.station_name_ru
                OR NOT ST_Equals(ns.location, s.location)
                OR s.direction IS DISTINCT FROM ns.direction
            ;
        """,
        params={
            'date': dt
        }
    )

    after = await db.fetch('SELECT count(station_id) FROM dds.stations;')

    logger.info(f"END dds.stations load. Loaded {after[0]['count'] - before[0]['count']} records.")