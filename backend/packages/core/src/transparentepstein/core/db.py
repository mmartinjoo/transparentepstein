from functools import lru_cache
from psycopg_pool import ConnectionPool, AsyncConnectionPool
from psycopg.rows import dict_row

from transparentepstein.core.config import settings

class RecordNotFoundError(Exception):
    pass

_apool: AsyncConnectionPool | None = None

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
    inputs: list,
    row_factory = None,
) -> list:
    if row_factory is None:
        row_factory = dict_row
        
    pool = await apool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=row_factory) as cur:
            await cur.execute(query, inputs)
            result = await cur.fetchall()
    return result

async def select_one(
    query: str, 
    inputs: list,
    row_factory = None,
    raise_on_not_found: bool = True,
):
    if row_factory is None:
        row_factory = dict_row
            
    pool = await apool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=row_factory) as cur:
            await cur.execute(query, inputs)
            result = await cur.fetchone()
            if raise_on_not_found and result is None:
                raise RecordNotFoundError(f"record not found for {query} with {inputs}")
    return result
        
@lru_cache(maxsize=1)
def pool() -> ConnectionPool:
    return ConnectionPool(
        str(settings.database_url),
        min_size=1,
        max_size=10,
        open=True,
    )

