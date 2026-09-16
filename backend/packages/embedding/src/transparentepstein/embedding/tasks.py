import asyncio
from dataclasses import asdict, dataclass
import logging
from typing import TypedDict

from fastembed import TextEmbedding

from transparentepstein.core import celery, qdrant, db

logger = logging.getLogger(__name__)
model = TextEmbedding("BAAI/bge-base-en-v1.5") # 768-dim

class ChunkEntry(TypedDict):
    id: int
    content: str

@dataclass
class EmbedRequest():
    document_id: int
    chunks: list[ChunkEntry]
    
@dataclass
class EmbedResponse():
    document_id: int
    ok: bool
    error: str | None = None

@celery.app.task
def embed(request: dict):
    request_mapped = EmbedRequest(**request)
    return run_task(async_embed(request_mapped))

def run_task(coro):
    async def _run():
        await db.apool()
        try:
            return await coro
        finally:
            await db.close_apool()
    return asyncio.run(_run())

async def async_embed(request: EmbedRequest):
    try:        
        texts = []
        document_chunk_ids = []
        for chunk_entry in request.chunks:
            texts.append(chunk_entry["content"])
            document_chunk_ids.append(chunk_entry["id"])
            
        vectors = await asyncio.to_thread(model.embed, texts)
        await asyncio.to_thread(
            qdrant.upsert,
            collection_name="document_chunks",
            vectors=vectors,
            document_id=request.document_id,
            document_chunk_ids=document_chunk_ids
        )
        
        return asdict(EmbedResponse(
            document_id=request.document_id,
            ok=True,
        ))
    except Exception as exc:
        logger.error(f"document {request.document_id} failed while embedding: {exc}")
        return asdict(EmbedResponse(
            document_id=request.document_id,
            ok=False,
            error=repr(exc),
        ))