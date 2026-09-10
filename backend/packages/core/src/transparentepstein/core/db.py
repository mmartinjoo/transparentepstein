from functools import lru_cache
from psycopg_pool import ConnectionPool, AsyncConnectionPool
from psycopg.rows import dict_row

from transparentepstein.core.config import settings

_apool: AsyncConnectionPool | None = None

async def apool() -> AsyncConnectionPool:
    global _apool
    if _apool is None:
        _apool = AsyncConnectionPool(
            str(settings.database_url),
            min_size=1,
            max_size=10,
            open=False, # open explicitly after, so it can be awaited
            kwargs={
                "row_factory": dict_row, 
                "autocommit": False,
            },
        )
        await _apool.open()
    return _apool

async def close_apool() -> None:
    global _apool
    if _apool is not None:
        await _apool.close()
        _apool = None
        
async def select(
    query: str, 
    inputs: list,
    fetchall: bool = True,
    row_factory = None,
):
    if row_factory is None:
        row_factory = dict_row
        
    pool = await apool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=row_factory) as cur:
            await cur.execute(query, inputs)
            if fetchall:
                result = await cur.fetchall()
            else:
                result = await cur.fetchone()
    return result
        
@lru_cache(maxsize=1)
def pool() -> ConnectionPool:
    return ConnectionPool(
        str(settings.database_url),
        min_size=1,
        max_size=10,
        open=True,
    )

