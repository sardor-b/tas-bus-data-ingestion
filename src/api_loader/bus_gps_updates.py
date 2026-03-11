import os
import json
import logging
import aiohttp
import asyncio
import psycopg
import platform
from dotenv import load_dotenv
from psycopg.rows import dict_row

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/bus_gps_updates.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

SEMAPHORE_LIMIT = 200

async def get_request(semaphore: asyncio.Semaphore, session: aiohttp.ClientSession, url: str, headers: dict):
    async with semaphore:
        async with session.get(url=url, headers=headers) as response:
            if response.status == 200:
                return await response.json()
            return None


async def ingest_bus_gps_updates(
    source: str,
    source_url: str,
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START bus gps update ingestion")

    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
    conninfo = f"host={host} port={port} dbname={database} user={user} password={password}"

    # Single connection reused for both read and write
    async with await psycopg.AsyncConnection.connect(conninfo) as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("SELECT DISTINCT route_id FROM dds.routes")
            bus_routes = await cur.fetchall()

        headers = {
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate'
        }

        request_urls = [f"{source_url}{idx['route_id']}" for idx in bus_routes]

        connector = aiohttp.TCPConnector(limit=SEMAPHORE_LIMIT)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [
                get_request(semaphore, session, url, headers)
                for url in request_urls
            ]
            responses = await asyncio.gather(*tasks)

        # COPY is much faster than executemany for bulk inserts
        valid_responses = [obj for obj in responses if obj is not None]
        if valid_responses:
            async with conn.cursor() as cur:
                async with cur.copy(
                    "COPY stg.bus_gps_updates (source, object_value) FROM STDIN"
                ) as copy:
                    for obj in valid_responses:
                        await copy.write_row((source, json.dumps(obj)))

    logger.info("END bus gps update ingestion")


if __name__ == '__main__':
    system = platform.system()
    if system == 'Linux':
        load_dotenv(dotenv_path=".env")
    else:
        load_dotenv(dotenv_path='../../.env')

    SOURCE = os.getenv('SOURCE')
    SOURCE_URL = os.getenv('SOURCE_BUS_GPS_UPDATES')

    HOST = os.getenv('PSQL_HOST', '172.29.172.1')
    PORT = 5432
    DATABASE = os.getenv('PSQL_DB')
    USER = os.getenv('PSQL_USER')
    PASSWORD = os.getenv('PSQL_PASSWORD')

    asyncio.run(
        ingest_bus_gps_updates(
            source=SOURCE,
            source_url=SOURCE_URL,
            host=HOST, port=PORT,
            database=DATABASE,
            user=USER,
            password=PASSWORD
        )
    )