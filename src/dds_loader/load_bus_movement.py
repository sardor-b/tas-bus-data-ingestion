import os
import logging

from datetime import datetime
from src.connectors import AsyncPostgresConnector

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/bus_movement.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


async def load_bus_movement(
    dt: datetime,
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START dds.bus_movement load")

    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    before = await db.fetch('SELECT count(movement_id) FROM dds.bus_movement;')

    await db.execute(
        query="""
            insert into dds.bus_movement (bus_hash_id, route_id, location, speed, course, status, direction, ping_dt)
            with bus_gps_updates as (
                select
                    update_dt,
                    jsonb_array_elements(object_value) as v
                from stg.bus_gps_updates
            )
            select
                v #>> '{bus, id}' as bus_hash_id,
                (v #>> '{bus, routeId}')::int as route_id,
                st_setsrid(
                    st_makepoint(
                        (v #>> '{bus, ly}')::float,
                        (v #>> '{bus, lx}')::float
                    ), 4326
                ) as location, -- location ping
                (v #>> '{bus, speed}')::numeric(5,1) as speed,
                (v #>> '{course}')::int as course, -- degree to which bus is headed
                v #>> '{status}' as status, -- NOTINROUTE,INPARK,LOSTTIME,LATENCY,ONLINE
                case
                    when (v #>> '{qDirection}')::int = 1 then 'origin'
                    when (v #>> '{qDirection}')::int = 0 then 'destination'
                    when (v #>> '{qDirection}')::int = -1 then 'None'
                end as direction,
                update_dt
            from bus_gps_updates
            ;
        """,
        params={}
    )

    after = await db.fetch('SELECT count(movement_id) FROM dds.bus_movement;')

    logger.info(f"END dds.bus_movement load. Loaded {after[0]['count'] - before[0]['count']} records.")


async def truncate_stg_bus_gps_updates(
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

    await db.execute("TRUNCATE stg.bus_gps_updates;", params={})

    logger.info("END stg.bus_gps_updates truncate")
