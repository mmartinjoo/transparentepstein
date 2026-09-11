import asyncio
from dataclasses import dataclass, asdict, field
import logging
import aiohttp
import traceback

from transparentepstein.core import storage, celery
from transparentepstein.ingestion import queue, selectors, services
from transparentepstein.ingestion.models import Document

logger = logging.getLogger(__name__)

HEADERS = {
    "Accept": "text/html",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
    "Sec-ch-ua": '"Not=A?Brand";v="99", "Google Chrome";v="151", "Chromium";v="151"',
    "Sec-ch-ua-mobile": "?0",
    "Sec-ch-ua-platform": '"macOS"',
    "Sec-fetch-dest": "document",
    "Sec-fetch-mode": "navigate",
    "Sec-fetch-site": "none",
    "Sec-fetch-user": "?1",
    "Upgrade-insecure-requests": "1",
    "Cookie": "justiceGovAgeVerified=true",
}

MAX_CONCURRENT_FETCHES = 8

@dataclass
class FetchResult():
    item_id: int
    url: str
    ok: bool
    s3_key: str | None = None
    error: str | None = None
    
@dataclass
class LoadResult():
    document_id: int
    item_id: int
    ok: bool
    content: str | None = None    
    error: str | None = None
    
@dataclass
class ChunkResult():
    document_id: int
    item_id: int
    ok: bool
    chunks: list[str] = field(default_factory=lambda: [])
    error: str | None = None

@celery.app.task
def fetch(item_ids: list[int]):
    return asyncio.run(async_fetch(item_ids))

@celery.app.task
def load(item_ids: list[int]):
    return asyncio.run(async_load(item_ids))

@celery.app.task
def chunk(item_ids: list[int]):
    return asyncio.run(async_chunk(item_ids))

async def async_fetch(item_ids: list[int]) -> list[FetchResult]:
    items = await queue.find_items(item_ids=item_ids)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_FETCHES)
    
    # No total timeout since files can be large. Only fail if can't connect or see no data for 60s
    timeout = aiohttp.ClientTimeout(total=None, connect=10, sock_read=60)
    
    async with aiohttp.ClientSession(headers=HEADERS, timeout=timeout) as session:
        results: list[FetchResult] = await asyncio.gather(
            *[fetch_one(session, semaphore, item) for item in items]
        )
        return results
    
async def fetch_one(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    item: queue.Item,
) -> dict:
    async with semaphore:
        try:
            parts = item.url.split("/")
            filename = parts[-1]            
            async with session.get(item.url) as response:
                if response.status != 200:
                    return asdict(FetchResult(
                        item_id=item.id,
                        url=item.url,
                        ok=False,
                        error=f"HTTP error: {response.status}"
                    ))

                data = await response.read()
                
            data_set = await selectors.find_data_set(data_set_id=item.data_set_id)
            key = await asyncio.to_thread(storage.put_file, data_set.name, filename, data)
            logger.info(f"saved {len(data)} bytes to \"{key}\"")
            return asdict(FetchResult(
                item_id=item.id,
                url=item.url,
                ok=True,
                s3_key=key,
            ))
            
        except Exception as exc:
            logger.info(f"fetch failed for {item.url} with error: {exc}")
            return asdict(FetchResult(
                item_id=item.id,
                url=item.url,
                ok=False,
                error=traceback.format_exc(exc)
            ))
            
async def async_load(item_ids: list[int]) -> list[dict]:
    coros = []
    for id in item_ids:
        item = await queue.find_item(id=id)
        document = await selectors.find_document(id=item.document_id)
        coros.append(load_one(document, item.id))
        
    return await asyncio.gather(*coros)

async def load_one(document: Document, item_id: int) -> dict:
    try:
        content = await services.load_document_content(document=document)
        return asdict(LoadResult(
            document_id=document.id,
            item_id=item_id,
            content=content,
            ok=True,
        ))
    except Exception as exc:
        return asdict(LoadResult(
            document_id=document.id,
            item_id=item_id,
            content=None,
            ok=False,
            error=traceback.format_exc(exc),
        ))
        
async def async_chunk(item_ids: list[int]) -> list[dict]:
    coros = []
    for id in item_ids:
        item = await queue.find_item(id=id)
        document = await selectors.find_document(id=item.document_id)
        coros.append(chunk_one(document, item.id))
        
    return await asyncio.gather(*coros)

async def chunk_one(document: Document, item_id: int) -> dict:
    try:
        if document.content is None or len(document.content) == 0:
            raise ValueError(f"content is None for document {document.id}")
        
        chunks = await asyncio.to_thread(services.chunk_text, text=document.content)
        return asdict(ChunkResult(
            document_id=document.id,
            item_id=item_id,
            chunks=chunks,
            ok=True,
        ))
    except Exception as exc:
        return asdict(ChunkResult(
            document_id=document.id,
            item_id=item_id,
            chunks=[],
            ok=False,
            error=traceback.format_exc(exc),
        ))
        
