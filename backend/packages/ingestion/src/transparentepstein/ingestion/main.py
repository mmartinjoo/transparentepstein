from pprint import pprint
import asyncio

from transparentepstein.ingestion import scraper
from transparentepstein.core.db import close_apool

async def async_main():
    try:
        data_sets = await scraper.discover(2)
        pprint(data_sets)
    finally:
        await close_apool()
    
# UV workaround: https://github.com/astral-sh/uv/issues/18931
def main() -> None:
    asyncio.run(async_main())