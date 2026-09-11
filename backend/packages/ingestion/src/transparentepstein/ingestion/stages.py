import logging
from pprint import pprint

from transparentepstein.ingestion import selectors, scraper, queue, tasks, services

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
    results: list[tasks.FetchResult] = []
    
    for i in range(5):
        start = i * batch_size
        batch = items[start:start + batch_size]        
        
        await queue.mark_fetching(items=batch)
        
        task_result = tasks.fetch.delay([item.id for item in batch])
        
        fetch_results: list[tasks.FetchResult] = [tasks.FetchResult(**v) for v in task_result.get()]
        for r in fetch_results:
            results.append(r)
        
    for res in results:
        if res.ok:
            # TODO: transaction
            item = await queue.find_item(id=res.item_id)
            document = await services.create_document(
                url=res.url,
                s3_key=res.s3_key,
                data_set_id=item.data_set_id,
            )
            await queue.mark_fetched(
                item=item, 
                document=document,
            )
            logger.info(f"document {document.id} fetched from {document.url}")
        else:
            await queue.mark_fetch_failed(item_id=res.item_id, error=res.error)
            logger.info(f"fetch failed at {res.url}, error: {res.error}")