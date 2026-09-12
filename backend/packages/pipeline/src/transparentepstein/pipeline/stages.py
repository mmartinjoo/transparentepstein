import asyncio
import logging
from pprint import pprint

from transparentepstein.classification.classifier.base import ClassificationLabel
from transparentepstein.ingestion import selectors, scraper, queue, tasks, services
from transparentepstein.classification import services as classification_services
from transparentepstein.core import storage
from transparentepstein.classification.tasks import classify as classify_task
from transparentepstein.classification.tasks import ClassificationResult

logger = logging.getLogger(__name__)

class NothingToDiscoverError(Exception):
    pass

async def discover_stage():
    data_set = await selectors.find_data_set(data_set_id=13)
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
    
    await queue.mark_fetching(items=items)
    
    task_results: list[dict] = []
    for item in items:
        task_result = tasks.fetch.delay(url=item.url, item_id=item.id, data_set_id=item.data_set_id)
        task_results.append(task_result.get())
        
    results = [tasks.FetchResult(**r) for r in task_results]
    for res in results:
        if res.ok:
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
            logger.error(f"fetch failed at {res.url}, error: {res.error}")
        
async def move_to_load_stage():
    items = await queue.dequeue_for_waiting_for_load()
    for item in items:
        document = await selectors.find_document(id=item.document_id)

        exists = await asyncio.to_thread(storage.exists, key=document.s3_key)
        if not exists:
            await queue.mark_fetch_failed(item=item, error=f"S3 object does not exist at {document.s3_key}")
            logger.error(f"item {item.id} marked as failed: S3 object does not exist")
            continue
            
        empty = await asyncio.to_thread(storage.empty, key=document.s3_key)
        if empty:
            await queue.mark_fetch_failed(item=item, error=f"S3 object exists but empty at {document.s3_key}")
            logger.error(f"item {item.id} marked as failed: S3 object is empty")
            continue

        await queue.mark_waiting_for_load(item=item)
        logger.info(f"item {item.id} marked as waiting for load")
            
async def load_stage():
    items = await queue.dequeue_for_load()
    batch_size = len(items) // 5
    results: list[tasks.LoadResult] = []
    
    for i in range(5):
        start = i * batch_size
        batch = items[start:start + batch_size]
        
        await queue.mark_loading(items=batch)
        
        context = {}
        for item in batch:
            context[item.id] = {
                "item_id": item.id,
                "document_id": item.document_id,
            }
        
        task_result = tasks.load.delay(context)
                
        load_results: list[tasks.LoadResult] = [tasks.LoadResult(**v) for v in task_result.get()]
        for r in load_results:
            results.append(r)
    
    for res in results:
        if res.ok:
            if res.content is None:
                await queue.mark_load_failed(item_id=res.item_id, error="empty content")
                logger.info(f"load failed for {res.document_id}, error: empty content")
                continue
            
            item = await queue.find_item(id=res.item_id)
            await services.update_document_content(document_id=res.document_id, content=res.content)
            await queue.mark_loaded(
                item=item, 
            )
            logger.info(f"document {res.document_id} loaded")
        else:
            await queue.mark_load_failed(item_id=res.item_id, error=res.error)
            logger.info(f"load failed for {res.document_id}, error: {res.error}")
            
async def move_to_chunk_stage():
    items = await queue.dequeue_for_waiting_for_chunk()
    for item in items:
        document = await selectors.find_document(id=item.document_id)

        if document.content is None or len(document.content) == 0:
            await queue.mark_load_failed(item=item, error=f"content is empty for {document.id}")
            logger.error(f"item {item.id} marked as failed: content is empty")
            continue
            
        await queue.mark_waiting_for_chunk(item=item)
        logger.info(f"item {item.id} marked as waiting for chunk")            

async def chunk_stage():
    items = await queue.dequeue_for_chunk()
    batch_size = len(items) // 5
    results: list[tasks.ChunkResult] = []
    
    for i in range(5):
        start = i * batch_size
        batch = items[start:start + batch_size]
        
        await queue.mark_chunking(items=batch)
        
        context = {}
        for item in batch:
            context[item.id] = {
                "item_id": item.id,
                "document_id": item.document_id,
            }
        
        task_result = tasks.chunk.delay(context)
                
        load_results: list[tasks.ChunkResult] = [tasks.ChunkResult(**v) for v in task_result.get()]
        for r in load_results:
            results.append(r)
    
    for res in results:
        if res.ok:
            if len(res.chunks) == 0:
                await queue.mark_chunk_failed(item_id=res.item_id, error="empty chunks")
                logger.info(f"chunk failed for {res.document_id}, error: empty chunks")
                continue
            
            item = await queue.find_item(id=res.item_id)
            await services.create_document_chunks(document_id=res.document_id, chunks=res.chunks)
            await queue.mark_chunked(
                item=item,
            )
            logger.info(f"document {res.document_id} chunked")
        else:
            await queue.mark_chunk_failed(item_id=res.item_id, error=res.error)
            logger.info(f"chunk failed for {res.document_id}, error: {res.error}")
            
async def move_to_classification_stage():
    items = await queue.dequeue_for_waiting_for_classification()
    for item in items:
        chunk_count = await selectors.count_document_chunks_by_document(document_id=item.document_id)
        if chunk_count is None or chunk_count == 0:
            await queue.mark_chunk_failed(item=item, error="no chunks were created")
            continue
        
        await queue.mark_waiting_for_classification(item=item)
        logger.info(f"item {item.id} marked as waiting for classification")            
            
async def classification_stage():
    items = await queue.dequeue_for_classification()
    batch_size = len(items) // 5
    results: list[ClassificationResult] = []
    
    for i in range(5):
        start = i * batch_size
        batch = items[start:start + batch_size]
        
        await queue.mark_classifying(items=batch)
        
        context = {}
        for item in batch:
            context[item.id] = {
                "document_id": item.document_id,
                "document_content": await selectors.fetch_document_content(id=item.document_id),
            }
        
        task_result = classify_task.delay(context)
                
        load_results: list[ClassificationResult] = [ClassificationResult(**v) for v in task_result.get()]
        for r in load_results:
            results.append(r)
    
    for res in results:
        if res.ok:
            if res.label is None:
                await queue.mark_classification_failed(item_id=res.item_id, error="empty label")
                logger.info(f"classification failed for {res.document_id}, error: empty label")
                continue
            
            label = ClassificationLabel[res.label]
            
            item = await queue.find_item(id=res.item_id)
            await classification_services.update_document_main_classification_label(document_id=res.document_id, label=label)
            await queue.mark_classified(
                item=item,
            )
            logger.info(f"document {res.document_id} classified as {res.label}")
        else:
            await queue.mark_classification_failed(item_id=res.item_id, error=res.error)
            logger.info(f"classification failed for {res.document_id}, error: {res.error}")