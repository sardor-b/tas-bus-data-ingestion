import os
import json
import logging
import aiohttp
import asyncio

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

SEMAPHORE_LIMIT = 100

async def get_request(semaphore: asyncio.Semaphore, session: aiohttp.ClientSession, url: str, headers: dict):
    async with semaphore:
        async with session.get(url=url, headers=headers) as response:
            if response.status == 200:
                return await response.json()
            else:
                return None


async def ingest_bus_stations(
    source: str,
    source_url: str,
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START bus station ingestion")

    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    bus_routes = await db.fetch(
        query="SELECT DISTINCT route_id FROM dds.h_route;"
    )

    headers = {
        'Accept': 'application/json',
        'Accept-Encoding': 'gzip, deflate'
    }

    request_urls = [f"{source_url}{idx['route_id']}" for idx in bus_routes]

    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in request_urls:
            tasks.append(
                get_request(
                    semaphore=semaphore,
                    session=session,
                    url=url,
                    headers=headers
                )
            )

        response = await asyncio.gather(*tasks)

    insert_params = [
        {
            'source': source,
            'object_value': json.dumps(obj)
        } for obj in response
    ]

    await db.executemany(
        query="""
            INSERT INTO stg.bus_stations (source, object_value)
            VALUES (%(source)s, %(object_value)s);
        """,
        params_seq=insert_params
    )

    logger.info("END bus station ingestion")