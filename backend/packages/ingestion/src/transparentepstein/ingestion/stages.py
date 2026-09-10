import logging

from transparentepstein.core import db
from transparentepstein.ingestion import selectors, scraper, services

logger = logging.getLogger(__name__)

class NothingToDiscoverError(Exception):
    pass

async def discover_stage():
    data_set = await selectors.find_data_set(data_set_id=1)
    next_page = data_set.processed_until_page + 1
    if next_page >= data_set.max_pages:
        logger.info(f"data set {data_set.id} has been fully processed")
        return
    
    urls = await scraper.discover(data_set=data_set, page=next_page)
    if len(urls) == 0:
        raise NothingToDiscoverError(f"zero URLs discovered for {data_set.name} at page {next_page}")
    
    await services.add_urls_to_ingestion_pipeline(
        urls=urls,
        data_set_id=data_set.id,
    )
            
    logger.info(f"discovered {len(urls)} URLs on page {next_page}")