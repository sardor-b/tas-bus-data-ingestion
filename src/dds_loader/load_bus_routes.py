import datetime
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


async def load_route_id_name(
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

    before = await db.fetch('SELECT count(route_id) FROM dds.route_id_name;')

    await db.execute(
        query="""
            insert into dds.route_id_name (route_id, route_name)
            select
                (t.value ->> 'id')::int as route_id,
                t.value ->> 'name' as route_name
            from
                (
                    select
                        jsonb_array_elements(object_value) as value
                    from stg.bus_routes
                    where
                        update_dt::date = (%(date)s)::date
                    order by update_dt desc
                    limit 1
                 ) as t
            where
                (t.value ->> 'id')::int != 0
            on conflict (route_id)
                do update set 
                    update_dt = now(),
                    route_name = excluded.route_name
            ;
        """,
        params={
            'date': dt
        }
    )

    after = await db.fetch('SELECT count(route_id) FROM dds.route_id_name;')

    logger.info(f"END dds.route_id_name load. Loaded {after[0]['count'] - before[0]['count']} records.")


async def load_route_origin_destination(
    dt: datetime,
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START dds.route_origin_destination load")

    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    before = await db.fetch('SELECT count(route_id) FROM dds.route_origin_destination;')

    await db.execute(
        query="""
            insert into dds.route_origin_destination (route_id, origin_name_uz, origin_name_ru, destination_name_uz, destination_name_ru, create_dt, update_dt)
            select
                rin.id as route_id,
                t.value ->> 'uzNameA' as origin_name_uz,
                t.value ->> 'nameA' as origin_name_ru,
                t.value ->> 'uzNameB' as destination_name_uz,
                t.value ->> 'nameB' as destination_name_ru,
                NOW() AS create_dt,
                NOW() AS update_dt
            from
                (
                    select
                        jsonb_array_elements(object_value) as value
                    from stg.bus_routes
                    where
                        update_dt::date = (%(date)s)::date
                    order by update_dt desc
                 ) as t
            inner join dds.route_id_name rin
                on rin.route_id = (t.value ->> 'id')::int
            left join lateral (
                select
                    route_id,
                    origin_name_uz,
                    origin_name_ru,
                    destination_name_uz,
                    destination_name_ru
                from dds.route_origin_destination
                where route_id = rin.id
                order by rin.create_dt desc
                limit 1
            ) rf on true
            where
                (t.value ->> 'id')::int != 0
                and (
                    rf.route_id IS NULL  -- New route
                    or (t.value ->> 'uzNameA') != rf.origin_name_uz
                    or (t.value ->> 'nameA') != rf.origin_name_ru
                    or (t.value ->> 'uzNameB') != rf.destination_name_uz
                    or (t.value ->> 'nameB') != rf.destination_name_ru
                )
            ;
        """,
        params={
            'date': dt
        }
    )

    after = await db.fetch('SELECT count(route_id) FROM dds.route_origin_destination;')

    logger.info(f"END dds.route_origin_destination load. Loaded {after[0]['count'] - before[0]['count']} records.")


async def load_route_fleet(
    dt: datetime,
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START dds.route_fleet load")

    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    before = await db.fetch('SELECT count(route_id) FROM dds.route_fleet;')

    await db.execute(
        query="""
            INSERT INTO dds.route_fleet (route_id, bus_type, fleet_size, create_dt, update_dt)
            SELECT
                rin.id AS route_id,
                COALESCE(t.value ->> 'busType', 'None') AS bus_type,
                (t.value ->> 'busSize')::int AS fleet_size,
                NOW() AS create_dt,
                NOW() AS update_dt
            FROM (
                SELECT jsonb_array_elements(object_value) AS value
                FROM stg.bus_routes
                WHERE update_dt::date = (%(date)s)::date
            ) AS t
            INNER JOIN dds.route_id_name rin
                ON rin.route_id = (t.value ->> 'id')::int
            LEFT JOIN LATERAL (
                SELECT
                    route_id,
                    bus_type,
                    fleet_size
                FROM dds.route_fleet
                WHERE route_id = rin.id
                ORDER BY create_dt DESC
                LIMIT 1
            ) rf ON true
            WHERE
                (t.value ->> 'id')::int != 0
                AND (
                    rf.route_id IS NULL -- Check if no previous record exists
                    OR COALESCE(t.value ->> 'busType', 'None') != rf.bus_type
                    OR (t.value ->> 'busSize')::int != rf.fleet_size
                );
            ;
        """,
        params={
            'date': dt
        }
    )

    after = await db.fetch('SELECT count(route_id) FROM dds.route_fleet;')

    logger.info(f"END dds.route_fleet load. Loaded {after[0]['count'] - before[0]['count']} records.")


async def load_route_start_end_time(
    dt: datetime,
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START dds.route_start_end_time load")

    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    before = await db.fetch('SELECT count(route_id) FROM dds.route_start_end_time;')

    await db.execute(
        query="""
            INSERT INTO dds.route_start_end_time (route_id, start_time, end_time, create_dt, update_dt)
            SELECT
                rin.id AS route_id,
                (t.value ->> 'startTime')::time as start_time,
                (t.value ->> 'endTime')::time as end_time,
                NOW() AS create_dt,
                NOW() AS update_dt
            FROM (
                SELECT jsonb_array_elements(object_value) AS value
                FROM stg.bus_routes
                WHERE update_dt::date = (%(date)s)::date
            ) AS t
            INNER JOIN dds.route_id_name rin
                ON rin.route_id = (t.value ->> 'id')::int
            LEFT JOIN LATERAL (
                SELECT
                    route_id,
                    start_time,
                    end_time
                FROM dds.route_start_end_time
                WHERE route_id = rin.id
                ORDER BY create_dt DESC
                LIMIT 1
            ) rf ON true
            WHERE
                (t.value ->> 'id')::int != 0
                AND (
                    rf.route_id IS NULL -- Check if no previous record exists
                    OR (t.value ->> 'startTime')::time != rf.start_time
                    OR (t.value ->> 'endTime')::time != rf.end_time
                )
            ;
        """,
        params={
            'date': dt
        }
    )

    after = await db.fetch('SELECT count(route_id) FROM dds.route_start_end_time;')

    logger.info(f"END dds.route_start_end_time load. Loaded {after[0]['count'] - before[0]['count']} records.")


# TODO: remove
if __name__ == '__main__':
    import os
    import asyncio
    from dotenv import load_dotenv
    load_dotenv(dotenv_path="../../.env")

    HOST = '172.29.172.1'
    PORT = 5432
    DATABASE = 'main'
    USER = os.getenv('PSQL_USER')
    PASSWORD = os.getenv('PSQL_PASSWORD')

    asyncio.run(
        load_route_id_name(dt=datetime.now(), host=HOST, port=PORT, database=DATABASE, user=USER,
                          password=PASSWORD)
    )
