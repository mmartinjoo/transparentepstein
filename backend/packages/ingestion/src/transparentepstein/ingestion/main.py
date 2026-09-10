from pprint import pprint
import asyncio

from transparentepstein.ingestion import scraper, stages, status, selectors
from transparentepstein.core.db import close_apool
from transparentepstein.core.logging import setup_logging

async def async_main():
    try:
        setup_logging()
        # await stages.discover_stage()
        pipeline = await selectors.find_ingestion_pipeline(id=52)
        status.guard_transition(
            ingestion_pipeline=pipeline,
            to_status=status.IngestionPipelineStatus.CHUNKING,
        )
    finally:
        await close_apool()
    
# UV workaround: https://github.com/astral-sh/uv/issues/18931
def main() -> None:
    asyncio.run(async_main())