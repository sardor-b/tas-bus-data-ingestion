import psycopg
from psycopg.rows import dict_row, DictRow
from contextlib import asynccontextmanager, contextmanager
from typing import AsyncIterator, Iterator, Any


class AsyncPostgresConnector:
    def __init__(self, host: str,  port: int, db: str, user: str, password: str):
        self.conninfo = f"host={host} port={port} dbname={db} user={user} password={password}"

    @asynccontextmanager
    async def get_connection(self) -> AsyncIterator[psycopg.AsyncConnection]:
        async with await psycopg.AsyncConnection.connect(self.conninfo) as conn:
            yield conn

    @asynccontextmanager
    async def get_cursor(self) -> AsyncIterator[psycopg.AsyncCursor[DictRow]]:
        async with self.get_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                yield cur

    async def fetch(self, query: str, params: tuple = None) -> list[DictRow]:
        async with self.get_cursor() as cur:
            await cur.execute(query, params)
            return await cur.fetchall()

    async def fetchmany(self, query: str, params_seq: Any) -> list[DictRow]:
        async with self.get_cursor() as cur:
            await cur.executemany(query, params_seq, returning=True)

            results = []
            while True:
                row = await cur.fetchone()
                if row:
                    results.append(row)
                if not cur.nextset():
                    break

            return results

    async def execute(self, query: str, params: Any) -> list[DictRow]:
        async with self.get_cursor() as cur:
            await cur.execute(query, params)

    async def executemany(self, query: str, params_seq: Any) -> list[DictRow]:
        async with self.get_cursor() as cur:
            await cur.executemany(query, params_seq)


class PostgresConnector:
    def __init__(self, host: str,  port: int, db: str, user: str, password: str):
        self.conninfo = f"host={host} port={port} dbname={db} user={user} password={password}"

    @contextmanager
    def get_connection(self) -> Iterator[psycopg.Connection]:
        with psycopg.connect(self.conninfo) as conn:
            yield conn

    @contextmanager
    def get_cursor(self) -> Iterator[psycopg.Cursor[DictRow]]:
        with self.get_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                yield cur

    def fetch(self, query: str, params: tuple = None) -> list[DictRow]:
        with self.get_cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def executemany(self, query: str, params_seq: list[tuple]) -> None:
        with self.get_cursor() as cur:
            cur.executemany(query, params_seq)