from functools import lru_cache
from psycopg_pool import ConnectionPool

from transparentepstein.core.config import settings

@lru_cache(maxsize=1)
def pool() -> ConnectionPool:
    return ConnectionPool(
        str(settings.database_url),
        min_size=1,
        max_size=10,
        open=True,
    )
