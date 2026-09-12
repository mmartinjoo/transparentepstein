import asyncio
from dataclasses import dataclass, asdict, field
import logging
from pprint import pprint
import aiohttp
import traceback

from transparentepstein.core import storage, celery
from transparentepstein.ingestion import selectors, services
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
def fetch(document: dict):
    doc = Document(**document)
    return asyncio.run(async_fetch(doc))

@celery.app.task
def load(documents: list[dict]):
    docs = [Document(**d) for d in documents]
    return asyncio.run(async_load(docs))

@celery.app.task
def chunk(documents: list[dict]):
    docs = [Document(**d) for d in documents]
    return asyncio.run(async_chunk(docs))

async def async_fetch(document: Document) -> dict:
    # No total timeout since files can be large. Only fail if can't connect or see no data for 60s
    timeout = aiohttp.ClientTimeout(total=None, connect=10, sock_read=60)

    async with aiohttp.ClientSession(headers=HEADERS, timeout=timeout) as session:    
        try:
            parts = document.url.split("/")
            filename = parts[-1]            
            async with session.get(document.url) as response:
                if response.status != 200:
                    return asdict(FetchResponse(
                        document_id=document.id,
                        url=document.url,
                        ok=False,
                        error=f"HTTP error: {response.status}"
                    ))

                data = await response.read()
                
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
            logger.info(f"fetch failed for {document.url} with error: {exc}")
            return asdict(FetchResponse(
                document_id=document.id,
                url=document.url,
                ok=False,
                error=traceback.format_exc(exc)
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
        return asdict(LoadResponse(
            document_id=document.id,
            content=None,
            ok=False,
            error=traceback.format_exc(exc),
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
        return asdict(ChunkResponse(
            document_id=document.id,
            chunks=[],
            ok=False,
            error=traceback.format_exc(exc),
        ))
        
