from pprint import pprint
import asyncio

from transparentepstein.ingestion import scraper

async def async_main():
    data_sets = await scraper.discover(4)
    pprint(data_sets)
    
# UV workaround: https://github.com/astral-sh/uv/issues/18931
def main() -> None:
    asyncio.run(async_main())