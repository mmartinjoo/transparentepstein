from pprint import pprint
import asyncio

from transparentepstein.ingestion import scraper
from transparentepstein.core.db import close_apool
from transparentepstein.core.logging import setup_logging

async def async_main():
    try:
        setup_logging()
        await scraper.discover(1)
    finally:
        await close_apool()
    
# UV workaround: https://github.com/astral-sh/uv/issues/18931
def main() -> None:
    asyncio.run(async_main())