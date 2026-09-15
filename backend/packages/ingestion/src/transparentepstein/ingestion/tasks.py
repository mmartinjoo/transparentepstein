import asyncio
from dataclasses import dataclass, asdict, field
import logging
from pprint import pprint

from transparentepstein.core import storage, celery
from transparentepstein.ingestion import selectors, services, scraper
from transparentepstein.ingestion.models import Document, DataSet

logger = logging.getLogger(__name__)

MAX_CONCURRENT_FETCHES = 8

@dataclass
class DiscoverResponse():
    data_set_id: int
    ok: bool
    urls: list[str] | None = None
    error: str | None = None

@dataclass
class FetchResponse():
    document_id: int
    url: str
    ok: bool
    s3_key: str | None = None
    error: str | None = None
    
@dataclass
class LoadResponse():
    document_id: int
    ok: bool
    content: str | None = None    
    error: str | None = None
    
@dataclass
class ChunkResponse():
    document_id: int
    ok: bool
    chunks: list[str] = field(default_factory=lambda: [])
    error: str | None = None
    
@celery.app.task
def discover(data_set: dict):
    data_set_mapped = DataSet(**data_set)
    return asyncio.run(async_discover(data_set_mapped))

@celery.app.task
def fetch(documents: list[dict]):
    docs = [Document(**d) for d in documents]
    return asyncio.run(async_fetch(docs))

@celery.app.task
def load(documents: list[dict]):
    docs = [Document(**d) for d in documents]
    return asyncio.run(async_load(docs))

@celery.app.task
def chunk(documents: list[dict]):
    docs = [Document(**d) for d in documents]
    return asyncio.run(async_chunk(docs))

async def async_discover(data_set: DataSet):
    try:
        urls = await scraper.discover(
            data_set=data_set,
            page=data_set.processed_until_page + 1,
        )
        
        return asdict(DiscoverResponse(
            data_set_id=data_set.id,
            urls=urls,
            ok=True,
        ))
    except Exception as exc:
        logger.error(f"discover failed for data set {data_set.id}: {repr(exc)}")
        return asdict(DiscoverResponse(
            data_set_id=data_set.id,
            ok=False,
            error=f"discover failed: {repr(exc)}",
        ))

async def async_fetch(documents: list[Document]) -> list[dict]:
    semaphore = asyncio.Semaphore(value=MAX_CONCURRENT_FETCHES)
    coros = [fetch_one(d, semaphore) for d in documents]
    return await asyncio.gather(*coros)
        
async def fetch_one(document: Document, semaphore: asyncio.Semaphore) -> dict:
    async with semaphore:
        try:
            data = await scraper.fetch(document.url)
            assert len(data) > 0
            
            parts = document.url.split("/")
            assert len(parts) >= 2
            
            filename = parts[-1]
            assert filename.find(".") != -1            
                
            data_set = await selectors.find_data_set(data_set_id=document.data_set_id)
            key = await asyncio.to_thread(storage.put_file, data_set.name, filename, data)
            logger.info(f"saved {len(data)} bytes to \"{key}\"")
            return asdict(FetchResponse(
                document_id=document.id,
                url=document.url,
                ok=True,
                s3_key=key,
            ))
        except Exception as exc:
            logger.error(f"fetch failed for {document.url} with error: {exc}")
            return asdict(FetchResponse(
                document_id=document.id,
                url=document.url,
                ok=False,
                error=f"fetch failed: {repr(exc)}",
            ))
            
async def async_load(documents: list[Document]) -> list[dict]:
    coros = []
    for document in documents:
        coros.append(load_one(document))
        
    return await asyncio.gather(*coros)

async def load_one(document: Document) -> dict:
    try:
        content = await services.load_document_content(s3_key=document.s3_key)
        return asdict(LoadResponse(
            document_id=document.id,
            content=content,
            ok=True,
        ))
    except Exception as exc:
        logger.error(f"load failed for {document.url} with error: {exc}")
        return asdict(LoadResponse(
            document_id=document.id,
            content=None,
            ok=False,
            error=f"load failed: {repr(exc)}",
        ))
        
async def async_chunk(documents: list[Document]) -> list[dict]:
    coros = []
    for document in documents:
        coros.append(chunk_one(document))
        
    return await asyncio.gather(*coros)

async def chunk_one(document: Document) -> dict:
    try:
        if document.content is None or len(document.content) == 0:
            raise ValueError(f"content is None for document {document.id}")
        
        chunks = await asyncio.to_thread(services.chunk_text, text=document.content)
        return asdict(ChunkResponse(
            document_id=document.id,
            chunks=chunks,
            ok=True,
        ))
    except Exception as exc:
        logger.error(f"chunk failed for {document.url} with error: {exc}")
        return asdict(ChunkResponse(
            document_id=document.id,
            chunks=[],
            ok=False,
            error=f"chunk failed: {repr(exc)}",
        ))
        
