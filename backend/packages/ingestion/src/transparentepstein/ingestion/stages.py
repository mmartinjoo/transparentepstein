import logging
from pprint import pprint

from transparentepstein.ingestion import selectors, scraper, queue, tasks

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
    
    await queue.enqueue_urls(
        urls=urls,
        data_set_id=data_set.id,
    )
    
    logger.info(f"discovered {len(urls)} URLs on page {next_page}")
    
async def fetch_stage():
    items = await queue.dequeue_for_fetch()
    logger.info(f"fetching {len(items)} URLs")
    batch_size = len(items) // 5
    for i in range(5):
        start = i * batch_size
        batch = items[start:start + batch_size]
        ids = [item.id for item in batch]
        res = tasks.fetch.delay(ids)
        values: list[dict] = res.get()
        fetch_results: list[tasks.FetchResult] = [tasks.FetchResult(**v) for v in values]
        pprint(fetch_results)