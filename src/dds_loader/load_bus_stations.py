import logging

from datetime import date, datetime
from src.connectors import AsyncPostgresConnector

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/bus_routes.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


async def load_route_coordinates(
    dt: datetime,
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START dds.route_coordinates load")

    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    before = await db.fetch('SELECT count(route_id) FROM dds.route_coordinates;')

    await db.execute(
        query="""
            INSERT INTO dds.route_coordinates (route_id, origin_path, destination_path)
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
                INNER JOIN dds.route_id_name rin ON t.route_id = rin.route_id
            )
            SELECT
                ng.route_id,
                ng.origin_path,
                ng.destination_path
            FROM new_geometries ng
            LEFT JOIN LATERAL (
                SELECT origin_path, destination_path
                FROM dds.route_coordinates
                WHERE route_id = ng.route_id
                ORDER BY create_dt DESC
                LIMIT 1
            ) rf ON TRUE
            WHERE
                rf.origin_path IS NULL  -- No previous record exists
                OR NOT ST_Equals(ng.origin_path, rf.origin_path)     -- Spatial comparison
                OR NOT ST_Equals(ng.destination_path, rf.destination_path); -- Spatial comparison
            ;
        """,
        params={
            'date': dt
        }
    )

    after = await db.fetch('SELECT count(route_id) FROM dds.route_coordinates;')

    logger.info(f"END dds.route_coordinates load. Loaded {after[0]['count'] - before[0]['count']} records.")


async def load_station_id_name(
    dt: datetime,
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START dds.station_id_name load")

    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    before = await db.fetch('SELECT count(station_id) FROM dds.station_id_name;')

    await db.execute(
        query="""

        """,
        params={
            'date': dt
        }
    )

    after = await db.fetch('SELECT count(station_id) FROM dds.station_id_name;')

    logger.info(f"END dds.station_id_name load. Loaded {after[0]['count'] - before[0]['count']} records.")


# TODO: remove
# if __name__ == '__main__':
#     import os
#     import asyncio
#     import datetime
#     from dotenv import load_dotenv
#     load_dotenv(dotenv_path="../../.env")
#
#     HOST = '172.29.172.1'
#     PORT = 5432
#     DATABASE = 'main'
#     USER = os.getenv('PSQL_USER')
#     PASSWORD = os.getenv('PSQL_PASSWORD')
#
#     asyncio.run(
#         load_route_coordinates(dt=datetime.datetime.now(), host=HOST, port=PORT, database=DATABASE, user=USER,
#                           password=PASSWORD)
#     )
