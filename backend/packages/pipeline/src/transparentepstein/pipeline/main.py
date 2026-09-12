from pprint import pprint
import asyncio

from transparentepstein.core.db import close_apool
from transparentepstein.core.logging import setup_logging
from transparentepstein.pipeline import stages

async def async_main():
    try:
        setup_logging()
        # await stages.discover_stage()
        # await stages.fetch_stage()
        # await stages.transition_to_load_stage()
        # await stages.load_stage()
        # await stages.transition_to_chunk_stage()
        await stages.chunk_stage()
        # await stages.move_to_classification_stage()
        # await stages.classification_stage()
    finally:
        await close_apool()
    
# UV workaround: https://github.com/astral-sh/uv/issues/18931
def main() -> None:
    asyncio.run(async_main())