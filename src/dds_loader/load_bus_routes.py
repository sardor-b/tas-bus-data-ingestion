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


async def load_routes(
    dt: datetime,
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START dds.route_id_name load")

    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    before = await db.fetch('SELECT count(route_id) FROM dds.routes;')

    await db.execute(
        query="""
            INSERT INTO dds.routes (
                route_id, route_name, bus_type, fleet_size, start_time,
                end_time, origin_name_uz, destination_name_uz,
                origin_name_ru, destination_name_ru, create_dt
            )
            SELECT
                (t.value ->> 'id')::int AS route_id,
                t.value ->> 'name' AS route_name,
                COALESCE(t.value ->> 'busType', 'None') AS bus_type,
                (t.value ->> 'busSize')::int AS fleet_size,
                (t.value ->> 'startTime')::time AS start_time,
                (t.value ->> 'endTime')::time AS end_time,
                t.value ->> 'uzNameA' AS origin_name_uz,
                t.value ->> 'uzNameB' AS destination_name_uz, -- Fixed order
                t.value ->> 'nameA' AS origin_name_ru,       -- Fixed order
                t.value ->> 'nameB' AS destination_name_ru,   -- Fixed order
                NOW() AS create_dt
            FROM
                (
                select
                    jsonb_array_elements(v.object_value) as value
                from (
                    select
                        object_value
                    from stg.bus_routes
                    where update_dt::date = (%(date)s)::date
                    order by update_dt desc
                    limit 1
                     ) v
                 ) as t
            LEFT JOIN LATERAL (
                SELECT *
                FROM dds.routes
                WHERE route_id = (t.value ->> 'id')::int
                ORDER BY create_dt DESC
                LIMIT 1
            ) rf ON TRUE
            WHERE
                (t.value ->> 'id')::int != 0
                AND (
                    rf.route_id IS NULL -- Record doesn't exist (New)
                    OR t.value ->> 'name' IS DISTINCT FROM rf.route_name
                    OR COALESCE(t.value ->> 'busType', 'None') IS DISTINCT FROM rf.bus_type
                    OR (t.value ->> 'busSize')::int IS DISTINCT FROM rf.fleet_size
                    OR (t.value ->> 'startTime')::time IS DISTINCT FROM rf.start_time
                    OR (t.value ->> 'endTime')::time IS DISTINCT FROM rf.end_time
                    OR t.value ->> 'uzNameA' IS DISTINCT FROM rf.origin_name_uz
                    OR t.value ->> 'uzNameB' IS DISTINCT FROM rf.destination_name_uz
                    OR t.value ->> 'nameA' IS DISTINCT FROM rf.origin_name_ru
                    OR t.value ->> 'nameB' IS DISTINCT FROM rf.destination_name_ru
                );
        """,
        params={
            'date': dt
        }
    )

    after = await db.fetch('SELECT count(route_id) FROM dds.routes;')

    logger.info(f"END dds.routes load. Loaded {after[0]['count'] - before[0]['count']} records.")


# TODO: remove
# if __name__ == '__main__':
#     import os
#     import asyncio
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
#         load_route_id_name(dt=datetime.now(), host=HOST, port=PORT, database=DATABASE, user=USER,
#                           password=PASSWORD)
#     )
