import os
import logging

from src.connectors import AsyncPostgresConnector

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/bus.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


async def h_bus(
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START dds.h_bus load")

    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    before = await db.fetch('SELECT count(hk_bus) FROM dds.h_bus;')

    await db.execute(
        query="""
            INSERT INTO dds.h_bus (hk_bus, bus_id, load_dt, load_source)
            SELECT
                md5(src.bus_hash_id) as hk_bus,
            
                bus_hash_id,
            
                now() as load_dt,
                'stg.bus_gps_updates' as load_source
            FROM (
                SELECT DISTINCT ON (v #>> '{bus, id}')
                    v #>> '{bus, id}' AS bus_hash_id
                FROM
                    stg.bus_gps_updates,
                LATERAL jsonb_array_elements(object_value) AS v
            ) src
            ON CONFLICT (hk_bus)
                DO NOTHING
            ;
        """,
        params={}
    )

    after = await db.fetch('SELECT count(hk_bus) FROM dds.h_bus;')

    logger.info(f"END dds.h_bus load. Loaded {after[0]['count'] - before[0]['count']} records.")


async def s_bus_model(
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START dds.s_bus_model load")

    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    before = await db.fetch('SELECT count(hk_bus) FROM dds.s_bus_model;')

    await db.execute(
        query="""
            INSERT INTO dds.s_bus_model (hk_bus, model, load_dt, load_source, hash_diff)
            WITH latest AS (
                SELECT
                    hk_bus,
                    hash_diff
                FROM dds.s_bus_model
                ORDER BY hk_bus
            )
            SELECT
                md5(src.bus_hash_id) as hk_bus,
            
                src.bus_model,
            
                NOW() AS load_dt,
                'stg.bus_gps_updates' as load_source,
                md5(
                    concat_ws(
                        '||',
                        src.bus_hash_id,
                        src.bus_model
                    )
                ) AS hash_diff
            FROM (
                SELECT DISTINCT ON (v #>> '{bus, id}')
                    v #>> '{bus, id}' AS bus_hash_id,
                    v #>> '{bus, innerType}' AS bus_model
                FROM
                    stg.bus_gps_updates,
                LATERAL jsonb_array_elements(object_value) AS v
            ) src
            LEFT JOIN latest l on l.hk_bus = md5(src.bus_hash_id)
            WHERE
                md5(
                    concat_ws(
                        '||',
                        src.bus_hash_id,
                        src.bus_model
                    )
                ) IS DISTINCT FROM l.hash_diff
            ;
        """,
        params={}
    )

    after = await db.fetch('SELECT count(hk_bus) FROM dds.s_bus_model;')

    logger.info(f"END dds.s_bus_model load. Loaded {after[0]['count'] - before[0]['count']} records.")


async def s_bus_license_plate(
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START dds.s_bus_license_plate load")

    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    before = await db.fetch('SELECT count(hk_bus) FROM dds.s_bus_license_plate;')

    await db.execute(
        query="""
            INSERT INTO dds.s_bus_license_plate (hk_bus, license_plate, load_dt, load_source, hash_diff)
            WITH latest AS (
                SELECT
                    hk_bus,
                    hash_diff
                FROM dds.s_bus_license_plate
                ORDER BY hk_bus
            )
            SELECT
                md5(src.bus_hash_id) as hk_bus,
            
                src.license_plate,
            
                NOW() AS load_dt,
                'stg.bus_gps_updates' as load_source,
                md5(
                    concat_ws(
                        '||',
                        src.bus_hash_id,
                        src.license_plate
                    )
                ) AS hash_diff
            FROM (
                SELECT DISTINCT ON (v #>> '{bus, id}')
                    v #>> '{bus, id}' AS bus_hash_id,
                    v #>> '{bus, gos}' AS license_plate
                FROM
                    stg.bus_gps_updates,
                LATERAL jsonb_array_elements(object_value) AS v
            ) src
            LEFT JOIN latest l on l.hk_bus = md5(src.bus_hash_id)
            WHERE
                md5(
                    concat_ws(
                        '||',
                        src.bus_hash_id,
                        src.license_plate
                    )
                ) IS DISTINCT FROM l.hash_diff
            ;
        """,
        params={}
    )

    after = await db.fetch('SELECT count(hk_bus) FROM dds.s_bus_license_plate;')

    logger.info(f"END dds.s_bus_license_plate load. Loaded {after[0]['count'] - before[0]['count']} records.")


async def s_bus_garage_number(
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START dds.s_bus_garage_number load")

    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    before = await db.fetch('SELECT count(hk_bus) FROM dds.s_bus_garage_number;')

    await db.execute(
        query="""
            INSERT INTO dds.s_bus_garage_number (hk_bus, garage_number, load_dt, load_source, hash_diff)
            WITH latest AS (
                SELECT
                    hk_bus,
                    hash_diff
                FROM dds.s_bus_garage_number
                ORDER BY hk_bus
            )
            SELECT
                md5(src.bus_hash_id) as hk_bus,
            
                src.garage_number,
            
                NOW() AS load_dt,
                'stg.bus_gps_updates' as load_source,
                md5(
                    concat_ws(
                        '||',
                        src.bus_hash_id,
                        src.garage_number
                    )
                ) AS hash_diff
            FROM (
                SELECT DISTINCT ON (v #>> '{bus, id}')
                    v #>> '{bus, id}' AS bus_hash_id,
                    v #>> '{bus, gar}' AS garage_number
                FROM
                    stg.bus_gps_updates,
                LATERAL jsonb_array_elements(object_value) AS v
            ) src
            LEFT JOIN latest l on l.hk_bus = md5(src.bus_hash_id)
            WHERE
                md5(
                    concat_ws(
                        '||',
                        src.bus_hash_id,
                        src.garage_number
                    )
                ) IS DISTINCT FROM l.hash_diff
            ;
        """,
        params={}
    )

    after = await db.fetch('SELECT count(hk_bus) FROM dds.s_bus_garage_number;')

    logger.info(f"END dds.s_bus_garage_number load. Loaded {after[0]['count'] - before[0]['count']} records.")


async def s_bus_movement(
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START dds.s_bus_movement load")

    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    before = await db.fetch('SELECT count(hk_bus) FROM dds.s_bus_movement;')

    await db.execute(
        query="""
            insert into dds.s_bus_movement (hk_bus, location, speed, course, status, direction, ping_dt, load_dt, load_source, hash_diff)
            with bus_gps_updates as (
                select
                    update_dt,
                    jsonb_array_elements(object_value) as v
                from stg.bus_gps_updates
            ),
            latest AS (
                SELECT
                    hk_bus,
                    hash_diff
                FROM dds.s_bus_garage_number
                ORDER BY hk_bus
            ),
            bus_movement as (
                select
                    v #>> '{bus, id}' as bus_id,
                    st_setsrid(
                        st_makepoint(
                            (v #>> '{bus, ly}')::float,
                            (v #>> '{bus, lx}')::float
                        ), 4326
                    ) as location, -- location ping
                    (v #>> '{bus, speed}') as speed,
                    (v #>> '{course}') as course,
                    v #>> '{status}' as status,
                    case
                        when (v #>> '{qDirection}')::int = 1 then 'origin'
                        when (v #>> '{qDirection}')::int = 0 then 'destination'
                        when (v #>> '{qDirection}')::int = -1 then 'None'
                    end as direction,
                    update_dt as ping_dt
                from bus_gps_updates
            )
            select
                md5(bus_id) as hk_bus,
            
                location,
                speed::numeric(5,1),
                course::int,
                status,
                direction,
                ping_dt,
            
                now() as load_dt,
                'stg.bus_gps_updates' as load_source,
                md5(
                    concat_ws(
                        '||',
                        st_asbinary(location),
                        speed,
                        course,
                        status,
                        direction,
                        ping_dt
                    )
                ) as hash_diff
            from bus_movement
            left join latest l on l.hk_bus = md5(bus_id)
            where
                md5(
                    concat_ws(
                        '||',
                        st_asbinary(location),
                        speed,
                        course,
                        status,
                        direction,
                        ping_dt
                    )
                ) is distinct from l.hash_diff
            ;
        """,
        params={}
    )

    after = await db.fetch('SELECT count(hk_bus) FROM dds.s_bus_movement;')

    logger.info(f"END dds.s_bus_movement load. Loaded {after[0]['count'] - before[0]['count']} records.")
