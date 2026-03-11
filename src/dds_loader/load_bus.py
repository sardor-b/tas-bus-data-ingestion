import os
import logging

from datetime import datetime
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


async def load_bus(
    dt: datetime,
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START dds.bus load")

    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    before = await db.fetch('SELECT count(bus_id) FROM dds.bus;')

    await db.execute(
        query="""
            INSERT INTO dds.bus (
                bus_hash_id,
                bus_id,
                garage_number,
                license_plate,
                bus_type,
                create_dt
            )
            SELECT
                t.bus_hash_id,
                t.bus_id,
                t.garage_number,
                t.license_plate,
                t.bus_type,
                NOW() AS create_dt
            FROM (
                -- Get the latest state of each bus from the staging batch for the day
                SELECT DISTINCT ON (v #>> '{bus, id}')
                    v #>> '{bus, id}' AS bus_hash_id,
                    (v #>> '{bus, busId}')::int AS bus_id,
                    v #>> '{bus, gar}' AS garage_number,
                    v #>> '{bus, gos}' AS license_plate,
                    v #>> '{bus, innerType}' AS bus_type
                FROM
                    stg.bus_gps_updates,
                    LATERAL jsonb_array_elements(object_value) AS v
                WHERE
                    update_dt::date = (%(date)s)::date
                ORDER BY
                    v #>> '{bus, id}'
            ) t
            LEFT JOIN LATERAL (
                -- Find the most recent existing record for this bus_hash_id
                SELECT *
                FROM dds.bus
                WHERE bus_hash_id = t.bus_hash_id
                ORDER BY create_dt DESC
                LIMIT 1
            ) last_version ON TRUE
            WHERE
                last_version.bus_hash_id IS NULL -- Record is brand new
                OR t.bus_id IS DISTINCT FROM last_version.bus_id
                OR t.garage_number IS DISTINCT FROM last_version.garage_number
                OR t.license_plate IS DISTINCT FROM last_version.license_plate
                OR t.bus_type IS DISTINCT FROM last_version.bus_type
        """,
        params={
            'date': dt
        }
    )

    after = await db.fetch('SELECT count(bus_id) FROM dds.bus;')

    logger.info(f"END dds.bus load. Loaded {after[0]['count'] - before[0]['count']} records.")