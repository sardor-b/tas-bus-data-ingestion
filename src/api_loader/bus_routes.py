import json
import logging
import aiohttp
import asyncio

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

SEMAPHORE_LIMIT = 100

async def get_request(semaphore: asyncio.Semaphore, session: aiohttp.ClientSession, url: str, headers: dict):
    async with semaphore:
        async with session.get(url=url, headers=headers) as response:
            if response.status == 200:
                return await response.json()
            else:
                return None


async def ingest_bus_routes(
    source: str,
    source_url: str,
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
):
    logger.info("START bus route ingestion")

    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    headers = {
        'Accept': 'application/json',
        'Accept-Encoding': 'gzip, deflate'
    }

    async with aiohttp.ClientSession() as session:
        response = await get_request(
            semaphore=semaphore,
            session=session,
            url=source_url,
            headers=headers
        )

    await db.execute(
        query="""
            INSERT INTO stg.bus_routes (source, object_value)
            VALUES (%(source)s, %(object_value)s);
        """,
        params={
            'source': source,
            'object_value': json.dumps(response)
        }
    )

    logger.info("END bus route ingestion")


# TODO: remove
# if __name__ == '__main__':
#     import os
#     from dotenv import load_dotenv
#     load_dotenv(dotenv_path="../../.env")
#
#     SOURCE = os.getenv('SOURCE')
#     SOURCE_URL = os.getenv('SOURCE_BUS_ROUTES')
#
#     HOST = '172.29.172.1'
#     PORT = 5432
#     DATABASE = 'main'
#     USER = os.getenv('PSQL_USER')
#     PASSWORD = os.getenv('PSQL_PASSWORD')
#
#     asyncio.run(
#         run(source=SOURCE, source_url=SOURCE_URL, host=HOST, port=PORT, database=DATABASE, user=USER,
#                           password=PASSWORD)
#     )
