from contextlib import asynccontextmanager
from contextvars import ContextVar
from functools import lru_cache
from typing import AsyncGenerator
from psycopg import AsyncConnection, AsyncCursor
from psycopg_pool import ConnectionPool, AsyncConnectionPool
from psycopg.rows import dict_row

from transparentepstein.core.config import settings

class RecordNotFoundError(Exception):
    pass

_apool: AsyncConnectionPool | None = None
_conn: ContextVar[AsyncConnection | None] = ContextVar("db_conn", default=None)

@asynccontextmanager
async def transaction():
    conn = _conn.get()
    
    # there's an existing outer connection
    if conn is not None:
        # adds a SAVEPOINT instead of a new transaction
        async with conn.transaction():
            yield conn
        return
    
    pool = await apool()
    
    # new connection
    async with pool.connection() as conn:
        token = _conn.set(conn)
        try:
            # BEGIN... COMMIT/ROLLBACK
            async with conn.transaction():      
                yield conn
        finally:
            _conn.reset(token)
          
@asynccontextmanager  
async def _cursor(row_factory = None) -> AsyncGenerator[AsyncCursor, None]:
    factory = row_factory or dict_row
    conn = _conn.get()

    if conn is not None:
        async with conn.cursor(row_factory=factory) as cur:
            yield cur
        return
    
    pool = await apool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=factory) as cur:
            yield cur
        

async def apool() -> AsyncConnectionPool:
    global _apool
    if _apool is None:
        _apool = AsyncConnectionPool(
            str(settings.database_url),
            min_size=1,
            max_size=10,
            reconnect_timeout=10,
            open=False, # open explicitly after, so it can be awaited
            kwargs={
                "row_factory": dict_row, 
                "autocommit": False,
            },
        )
        await _apool.open(wait=True, timeout=10)
    return _apool

async def close_apool() -> None:
    global _apool
    if _apool is not None:
        await _apool.close()
        _apool = None
        
async def select_many(
    query: str, 
    inputs: list | tuple,
    row_factory = None,
) -> list:
    async with _cursor(row_factory=row_factory) as cur:
        await cur.execute(query, inputs)
        result = await cur.fetchall()
    return result
    
async def select_one(
    query: str, 
    inputs: list,
    row_factory = None,
    raise_on_not_found: bool = True,
):
    async with _cursor(row_factory=row_factory) as cur:
        await cur.execute(query, inputs)
        result = await cur.fetchone()
        if raise_on_not_found and result is None:
            raise RecordNotFoundError(f"record not found for {query} with {inputs}")
    return result

async def insert(
    query: str,
    inputs: list,
    row_factory = None,
    returning: bool = False,
):
    async with _cursor(row_factory=row_factory) as cur:
        await cur.execute(query, inputs)
        if returning:
            return await cur.fetchone()
        
async def update(
    query: str,
    inputs: list,
):
    async with _cursor() as cur:
        await cur.execute(query, inputs)
            
async def delete(
    query: str,
    inputs: list,
):
    async with _cursor() as cur:
        await cur.execute(query, inputs)
        
@lru_cache(maxsize=1)
def pool() -> ConnectionPool:
    return ConnectionPool(
        str(settings.database_url),
        min_size=1,
        max_size=10,
        open=True,
    )

