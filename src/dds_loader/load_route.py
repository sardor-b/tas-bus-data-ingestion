import os
import logging

from src.connectors import AsyncPostgresConnector

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/route.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


async def h_route(
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START dds.h_route load")

    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    before = await db.fetch('SELECT count(hk_route) FROM dds.h_route;')

    await db.execute(
        query="""
            INSERT INTO dds.h_route(hk_route, route_id, load_dt, load_source)
            SELECT
                MD5((src.value ->> 'id')::TEXT) AS hk_route,
            
                (src.value ->> 'id')::INT AS route_id,
            
                NOW() AS load_dt,
                'stg.bus_routes' AS load_source
            FROM (
                SELECT
                    JSONB_ARRAY_ELEMENTS(v.object_value) AS value
                FROM
                    (
                    SELECT
                        object_value
                    FROM stg.bus_routes
                    ORDER BY update_dt DESC
                    LIMIT 1
                    ) AS v
            ) AS src
            WHERE (src.value ->> 'id')::INT != 0
            ON CONFLICT (hk_route)
                DO NOTHING
            ;
        """,
        params={}
    )

    after = await db.fetch('SELECT count(hk_route) FROM dds.h_route;')

    logger.info(f"END dds.h_route load. Loaded {after[0]['count'] - before[0]['count']} records.")


async def s_route_schedule(
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START dds.s_route_schedule load")

    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    before = await db.fetch('SELECT count(hk_route) FROM dds.s_route_schedule;')

    await db.execute(
        query="""
            INSERT INTO dds.s_route_schedule (hk_route, start_time, end_time, load_dt, load_source, hash_diff)
            SELECT
                MD5((src.value ->> 'id')::TEXT) AS hk_route,
            
                (src.value ->> 'startTime')::TIME AS start_time,
                (src.value ->> 'endTime')::TIME   AS end_time,
            
                NOW() AS load_dt,
                'stg.bus_routes' AS load_source,
                MD5(
                    CONCAT_WS(
                        '||',
                        (src.value ->> 'startTime')::TIME::TEXT,
                        (src.value ->> 'endTime')::TIME::TEXT
                    )
                ) AS hash_diff
            FROM (
                SELECT
                    JSONB_ARRAY_ELEMENTS(v.object_value) AS value
                FROM (
                    SELECT object_value
                    FROM stg.bus_routes
                    ORDER BY update_dt DESC
                    LIMIT 1
                ) AS v
            ) AS src
            LEFT JOIN (
                SELECT DISTINCT ON (hk_route)
                    hk_route,
                    hash_diff
                FROM dds.s_route_schedule
                ORDER BY hk_route, load_dt DESC
            ) latest ON latest.hk_route = MD5((src.value ->> 'id')::TEXT)
            WHERE
                (src.value ->> 'id')::INT != 0
                AND MD5(
                    CONCAT_WS(
                        '||',
                        (src.value ->> 'startTime')::TIME::TEXT,
                        (src.value ->> 'endTime')::TIME::TEXT
                    )
                ) IS DISTINCT FROM latest.hash_diff
            ;
        """,
        params={}
    )

    after = await db.fetch('SELECT count(hk_route) FROM dds.s_route_schedule;')

    logger.info(f"END dds.s_route_schedule load. Loaded {after[0]['count'] - before[0]['count']} records.")


async def s_route_name(
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START dds.s_route_name load")

    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    before = await db.fetch('SELECT count(hk_route) FROM dds.s_route_name;')

    await db.execute(
        query="""
            INSERT INTO dds.s_route_name (hk_route, origin_name_uz, origin_name_ru, destination_name_uz, destination_name_ru, load_dt, load_source, hash_diff)
            SELECT
                MD5((src.value ->> 'id')::TEXT) AS hk_route,
            
                (src.value ->> 'uzNameA') AS origin_name_uz,
                (src.value ->> 'nameA')   AS origin_name_ru,
                (src.value ->> 'uzNameB') AS destination_name_uz,
                (src.value ->> 'nameB')   AS destination_name_ru,
            
                NOW() AS load_dt,
                'stg.bus_routes' AS load_source,
                MD5(
                    CONCAT_WS(
                        '||',
                        (src.value ->> 'uzNameA'),
                        (src.value ->> 'nameA'),
                        (src.value ->> 'uzNameB'),
                        (src.value ->> 'nameB')
                    )
                ) AS hash_diff
            FROM (
                SELECT
                    JSONB_ARRAY_ELEMENTS(v.object_value) AS value
                FROM (
                    SELECT object_value
                    FROM stg.bus_routes
                    ORDER BY update_dt DESC
                    LIMIT 1
                ) AS v
            ) AS src
            LEFT JOIN (
                SELECT DISTINCT ON (hk_route)
                    hk_route,
                    hash_diff
                FROM dds.s_route_name
                ORDER BY hk_route, load_dt DESC
            ) latest ON latest.hk_route = MD5((src.value ->> 'id')::TEXT)
            WHERE
                (src.value ->> 'id')::INT != 0
                AND MD5(
                    CONCAT_WS(
                        '||',
                        (src.value ->> 'uzNameA'),
                        (src.value ->> 'nameA'),
                        (src.value ->> 'uzNameB'),
                        (src.value ->> 'nameB')
                    )
                ) IS DISTINCT FROM latest.hash_diff
            ;
        """,
        params={}
    )

    after = await db.fetch('SELECT count(hk_route) FROM dds.s_route_name;')

    logger.info(f"END dds.s_route_name load. Loaded {after[0]['count'] - before[0]['count']} records.")


async def s_route_path_origin(
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START dds.s_route_path_origin load")

    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    before = await db.fetch('SELECT count(hk_route) FROM dds.s_route_path_origin;')

    await db.execute(
        query="""
            INSERT INTO dds.s_route_path_origin (hk_route, path, load_dt, load_source, hash_diff)
            WITH src AS (
                SELECT DISTINCT ON ((object_value #>> '{route, id}')::INT)
                    (object_value #>> '{route, id}')::INT AS route_id,
                    (object_value -> 'coordsTwo')::JSONB   AS path
                FROM stg.bus_stations
                WHERE (object_value #>> '{route, id}')::INT != 0
                ORDER BY (object_value #>> '{route, id}')::INT, update_dt DESC
            ),
            
            src_with_geo AS (
                SELECT
                    route_id,
                    MD5(route_id::TEXT) AS hk_route,
                    ST_SetSRID(
                        ST_MakeLine(
                            ST_MakePoint((p.val->>'ly')::FLOAT, (p.val->>'lx')::FLOAT)
                            ORDER BY p.ord
                        ),
                        4326
                    ) AS path
                FROM src
                CROSS JOIN LATERAL (
                    SELECT val, ord
                    FROM JSONB_ARRAY_ELEMENTS(src.path) WITH ORDINALITY AS p(val, ord)
                ) p
                GROUP BY route_id
            ),
            
            src_hashed AS (
                SELECT
                    hk_route,
                    path,
                    MD5(ST_AsText(path)) AS hash_diff
                FROM src_with_geo
            ),
            
            latest AS (
                SELECT DISTINCT ON (hk_route)
                    hk_route,
                    hash_diff
                FROM dds.s_route_path_origin
                ORDER BY hk_route, load_dt DESC
            )
            
            SELECT
                s.hk_route,
                s.path,
                NOW()              AS load_dt,
                'stg.bus_stations' AS load_source,
                s.hash_diff
            FROM src_hashed s
            LEFT JOIN latest l ON l.hk_route = s.hk_route
            WHERE s.hash_diff IS DISTINCT FROM l.hash_diff
            ;
        """,
        params={}
    )

    after = await db.fetch('SELECT count(hk_route) FROM dds.s_route_path_origin;')

    logger.info(f"END dds.s_route_path_origin load. Loaded {after[0]['count'] - before[0]['count']} records.")


async def s_route_path_destination(
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START dds.s_route_path_destination load")

    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    before = await db.fetch('SELECT count(hk_route) FROM dds.s_route_path_destination;')

    await db.execute(
        query="""
            INSERT INTO dds.s_route_path_destination (hk_route, path, load_dt, load_source, hash_diff)
            WITH src AS (
                SELECT DISTINCT ON ((object_value #>> '{route, id}')::INT)
                    (object_value #>> '{route, id}')::INT AS route_id,
                    (object_value -> 'coordsOne')::JSONB   AS path
                FROM stg.bus_stations
                WHERE (object_value #>> '{route, id}')::INT != 0
                ORDER BY (object_value #>> '{route, id}')::INT, update_dt DESC
            ),
            
            src_with_geo AS (
                SELECT
                    route_id,
                    MD5(route_id::TEXT) AS hk_route,
                    ST_SetSRID(
                        ST_MakeLine(
                            ST_MakePoint((p.val->>'ly')::FLOAT, (p.val->>'lx')::FLOAT)
                            ORDER BY p.ord
                        ),
                        4326
                    ) AS path
                FROM src
                CROSS JOIN LATERAL (
                    SELECT val, ord
                    FROM JSONB_ARRAY_ELEMENTS(src.path) WITH ORDINALITY AS p(val, ord)
                ) p
                GROUP BY route_id
            ),
            
            src_hashed AS (
                SELECT
                    hk_route,
                    path,
                    MD5(ST_AsText(path)) AS hash_diff
                FROM src_with_geo
            ),
            
            latest AS (
                SELECT DISTINCT ON (hk_route)
                    hk_route,
                    hash_diff
                FROM dds.s_route_path_destination
                ORDER BY hk_route, load_dt DESC
            )
            
            SELECT
                s.hk_route,
                s.path,
                NOW()              AS load_dt,
                'stg.bus_stations' AS load_source,
                s.hash_diff
            FROM src_hashed s
            LEFT JOIN latest l ON l.hk_route = s.hk_route
            WHERE s.hash_diff IS DISTINCT FROM l.hash_diff
            ;
        """,
        params={}
    )

    after = await db.fetch('SELECT count(hk_route) FROM dds.s_route_path_destination;')

    logger.info(f"END dds.s_route_path_destination load. Loaded {after[0]['count'] - before[0]['count']} records.")
