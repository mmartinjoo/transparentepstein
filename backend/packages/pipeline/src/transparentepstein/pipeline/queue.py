from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from psycopg.rows import class_row

from transparentepstein.core import db
from transparentepstein.pipeline.models import Document

MAX_ATTEMPTS = 10
FETCH_LIMIT = 100
LOAD_LIMIT = 200
CHUNK_LIMIT = 500
CLASSIFICATION_LIMIT=100

@dataclass
class DocumentPipeline():
    id: int
    url: str
    data_set_id: int
    fetched: bool
    loaded: bool
    chunked: bool
    classified: bool
    embedded: bool
    status: str
    error: str
    attempts: int
    next_attempt_at: datetime
    document_id: int
    created_at: datetime
    updated_at: datetime
    finished_at: datetime

async def enqueue(document: Document):
    await db.insert(
        query="""
            insert into ops.document_queue(document_id)
            values(%s)   
        """,
        inputs=[document.id],
    )

async def enqueue_urls(urls: list[str], data_set_id: int):
    assert data_set_id is not None
        
    pool = await db.apool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            for url in urls:
                await cur.execute("""
                    insert into ops.ingestion_queue(data_set_id, url, status)
                    values(%s, %s, %s)              
                """, [data_set_id, url, Status.WAITING_FOR_FETCH.name])
                
            await cur.execute("""
                update ops.data_sets
                set processed_until_page = processed_until_page + 1
                where id = %s     
            """, [data_set_id])
            
            await conn.commit()
            
async def dequeue_for_fetch() -> list[DocumentPipeline]:
    pool = await db.apool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=class_row(DocumentPipeline)) as cur:
            await cur.execute("""
                select *
                from ops.ingestion_queue
                where fetched = false
                and status in (%s, %s)
                and attempts <= %s
                and next_attempt_at <= now()                
                order by created_at asc
                limit %s
                for update skip locked
            """, [
                Status.WAITING_FOR_FETCH.name,
                Status.FETCH_FAILED.name,
                MAX_ATTEMPTS,
                FETCH_LIMIT,
            ])
            return await cur.fetchall()
        
async def dequeue_for_waiting_for_load() -> list[DocumentPipeline]:
    pool = await db.apool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=class_row(DocumentPipeline)) as cur:
            await cur.execute("""
                select *
                from ops.ingestion_queue
                where fetched = true
                and status = %s
                and attempts <= %s
                and next_attempt_at <= now()                
                order by created_at asc
                for update skip locked
            """, [
                Status.FETCHED.name,
                MAX_ATTEMPTS,
            ])
            return await cur.fetchall()
        
async def dequeue_for_load() -> list[DocumentPipeline]:
    pool = await db.apool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=class_row(DocumentPipeline)) as cur:
            await cur.execute("""
                select *
                from ops.ingestion_queue
                where fetched = true
                and loaded = false
                and status in (%s, %s)
                and attempts <= %s
                and next_attempt_at <= now()                
                order by created_at asc
                limit %s
                for update skip locked
            """, [
                Status.WAITING_FOR_LOAD.name,
                Status.LOAD_FAILED.name,
                MAX_ATTEMPTS,
                LOAD_LIMIT,
            ])
            return await cur.fetchall()
        
async def dequeue_for_waiting_for_chunk() -> list[DocumentPipeline]:
    pool = await db.apool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=class_row(DocumentPipeline)) as cur:
            await cur.execute("""
                select *
                from ops.ingestion_queue
                where fetched = true
                and loaded = true
                and status = %s
                and attempts <= %s
                and next_attempt_at <= now()                
                order by created_at asc
                for update skip locked
            """, [
                Status.LOADED.name,
                MAX_ATTEMPTS,
            ])
            return await cur.fetchall()
        
async def dequeue_for_chunk() -> list[DocumentPipeline]:
    pool = await db.apool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=class_row(DocumentPipeline)) as cur:
            await cur.execute("""
                select *
                from ops.ingestion_queue
                where fetched = true
                and loaded = true
                and chunked = false
                and status in (%s, %s)
                and attempts <= %s
                and next_attempt_at <= now()                
                order by created_at asc
                limit %s
                for update skip locked
            """, [
                Status.WAITING_FOR_CHUNK.name,
                Status.CHUNK_FAILED.name,
                MAX_ATTEMPTS,
                CHUNK_LIMIT,
            ])
            return await cur.fetchall()
        
async def dequeue_for_waiting_for_classification() -> list[DocumentPipeline]:
    pool = await db.apool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=class_row(DocumentPipeline)) as cur:
            await cur.execute("""
                select *
                from ops.ingestion_queue
                where fetched = true
                and loaded = true
                and chunked = true
                and classified = false
                and status = %s
                and attempts <= %s
                and next_attempt_at <= now()                
                order by created_at asc
                for update skip locked
            """, [
                Status.CHUNKED.name,
                MAX_ATTEMPTS,
            ])
            return await cur.fetchall()
        
async def dequeue_for_classification() -> list[DocumentPipeline]:
    pool = await db.apool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=class_row(DocumentPipeline)) as cur:
            await cur.execute("""
                select *
                from ops.ingestion_queue
                where fetched = true
                and loaded = true
                and chunked = true
                and classified = false
                and status in (%s, %s)
                and attempts <= %s
                and next_attempt_at <= now()                
                order by created_at asc
                limit %s
                for update skip locked
            """, [
                Status.WAITING_FOR_CLASSIFICATION.name,
                Status.CLASSIFICATION_FAILED.name,
                MAX_ATTEMPTS,
                CLASSIFICATION_LIMIT,
            ])
            return await cur.fetchall()
        
async def find_items(item_ids: list[int]) -> list[DocumentPipeline]:
    in_clause = ','.join(['%s'] * len(item_ids))
    return await db.select_many(
        query=f"""
            select *
            from ops.ingestion_queue                   
            where id in ({in_clause})
        """,
        inputs=tuple(item_ids),
        row_factory=class_row(DocumentPipeline),
    )
    
async def find_item(id: int) -> DocumentPipeline:
    return await db.select_one(
        query=f"""
            select *
            from ops.ingestion_queue                   
            where id = %s
        """,
        inputs=[id],
        row_factory=class_row(DocumentPipeline),
    )
    
async def mark_fetching(
    items: list[DocumentPipeline]
):
    assert len(items) != 0
    
    for item in items:
        guard_transition(item=item, to_status=Status.FETCHING)
    
    in_clause = ','.join(['%s'] * len(items))
    await db.update(
        query=f"""
            update ops.ingestion_queue
            set
                status = %s,
                attempts = attempts + 1,
                updated_at = now()
            where id in ({in_clause})
        """,
        inputs=[
            Status.FETCHING.name,
            *[item.id for item in items],
        ]
    )
    
async def mark_fetched(
    item: DocumentPipeline, 
    document: Document,
):
    assert document is not None
    
    guard_transition(item=item, to_status=Status.FETCHED)
    
    await db.update(
        query=f"""
            update ops.ingestion_queue
            set
                status = %s,
                fetched = true,
                document_id = %s,
                updated_at = now()
            where id = %s
        """,
        inputs=[
            Status.FETCHED.name,
            document.id,
            item.id,
        ]
    )

async def mark_fetch_failed(item: DocumentPipeline, error: str):
    guard_transition(item=item, to_status=Status.FETCH_FAILED)
    
    await db.update(
        query=f"""
            update ops.ingestion_queue
            set
                status = %s,
                error = %s,
                next_attempt_at = now() + interval '1 hour',
                fetched = false,
                updated_at = now()
            where id = %s
        """,
        inputs=[
            Status.FETCH_FAILED.name,
            error,
            item.id,
        ]
    )
    
async def mark_waiting_for_load(
    item: DocumentPipeline
):
    assert item is not None
    
    guard_transition(item=item, to_status=Status.WAITING_FOR_LOAD)
    
    await db.update(
        query=f"""
            update ops.ingestion_queue
            set
                status = %s,
                fetched = true,
                updated_at = now()
            where id = %s
        """,
        inputs=[
            Status.WAITING_FOR_LOAD.name,
            item.id,
        ]
    )
    
async def mark_loading(
    items: list[DocumentPipeline]
):
    assert len(items) != 0
    
    for item in items:
        guard_transition(item=item, to_status=Status.LOADING)
    
    in_clause = ','.join(['%s'] * len(items))
    await db.update(
        query=f"""
            update ops.ingestion_queue
            set
                status = %s,
                attempts = attempts + 1,
                updated_at = now()
            where id in ({in_clause})
        """,
        inputs=[
            Status.LOADING.name,
            *[item.id for item in items],
        ]
    )
    
async def mark_loaded(
    item: DocumentPipeline, 
):
    assert item is not None
    
    guard_transition(item=item, to_status=Status.LOADED)
    
    await db.update(
        query=f"""
            update ops.ingestion_queue
            set
                status = %s,
                loaded = true,
                updated_at = now()
            where id = %s
        """,
        inputs=[
            Status.LOADED.name,
            item.id,
        ]
    )
    
async def mark_load_failed(item: DocumentPipeline, error: str):
    guard_transition(item=item, to_status=Status.LOAD_FAILED)
    
    await db.update(
        query=f"""
            update ops.ingestion_queue
            set
                status = %s,
                error = %s,
                next_attempt_at = now() + interval '1 hour',
                loaded = false,
                updated_at = now()
            where id = %s
        """,
        inputs=[
            Status.LOAD_FAILED.name,
            error,
            item.id,
        ]
    )
    
async def mark_waiting_for_chunk(
    item: DocumentPipeline
):
    assert item is not None
    
    guard_transition(item=item, to_status=Status.WAITING_FOR_CHUNK)
    
    await db.update(
        query=f"""
            update ops.ingestion_queue
            set
                status = %s,
                loaded = true,
                updated_at = now()
            where id = %s
        """,
        inputs=[
            Status.WAITING_FOR_CHUNK.name,
            item.id,
        ]
    )
    
async def mark_chunking(
    items: list[DocumentPipeline]
):
    assert len(items) != 0
    
    for item in items:
        guard_transition(item=item, to_status=Status.CHUNKING)
    
    in_clause = ','.join(['%s'] * len(items))
    await db.update(
        query=f"""
            update ops.ingestion_queue
            set
                status = %s,
                attempts = attempts + 1,
                updated_at = now()
            where id in ({in_clause})
        """,
        inputs=[
            Status.CHUNKING.name,
            *[item.id for item in items],
        ]
    )
    
async def mark_chunk_failed(item: DocumentPipeline, error: str):
    guard_transition(item=item, to_status=Status.CHUNK_FAILED)
    
    await db.update(
        query=f"""
            update ops.ingestion_queue
            set
                status = %s,
                error = %s,
                next_attempt_at = now() + interval '1 hour',
                chunked = false,
                updated_at = now()
            where id = %s
        """,
        inputs=[
            Status.CHUNK_FAILED.name,
            error,
            item.id,
        ]
    )
    
async def mark_chunked(
    item: DocumentPipeline, 
):
    assert item is not None
    
    guard_transition(item=item, to_status=Status.CHUNKED)
    
    await db.update(
        query=f"""
            update ops.ingestion_queue
            set
                status = %s,
                chunked = true,
                updated_at = now()
            where id = %s
        """,
        inputs=[
            Status.CHUNKED.name,
            item.id,
        ]
    )
    
async def mark_waiting_for_classification(
    item: DocumentPipeline
):
    assert item is not None
    
    guard_transition(item=item, to_status=Status.WAITING_FOR_CLASSIFICATION)
    
    await db.update(
        query=f"""
            update ops.ingestion_queue
            set
                status = %s,
                chunked = true,
                updated_at = now()
            where id = %s
        """,
        inputs=[
            Status.WAITING_FOR_CLASSIFICATION.name,
            item.id,
        ]
    )
    
async def mark_classifying(
    items: list[DocumentPipeline]
):
    assert len(items) != 0
    
    for item in items:
        guard_transition(item=item, to_status=Status.CLASSIFYING)
    
    in_clause = ','.join(['%s'] * len(items))
    await db.update(
        query=f"""
            update ops.ingestion_queue
            set
                status = %s,
                attempts = attempts + 1,
                updated_at = now()
            where id in ({in_clause})
        """,
        inputs=[
            Status.CLASSIFYING.name,
            *[item.id for item in items],
        ]
    )
    
async def mark_classification_failed(item: DocumentPipeline, error: str):
    guard_transition(item=item, to_status=Status.CLASSIFICATION_FAILED)
    
    await db.update(
        query=f"""
            update ops.ingestion_queue
            set
                status = %s,
                error = %s,
                next_attempt_at = now() + interval '1 hour',
                classified = false,
                updated_at = now()
            where id = %s
        """,
        inputs=[
            Status.CLASSIFICATION_FAILED.name,
            error,
            item.id,
        ]
    )
    
async def mark_classified(
    item: DocumentPipeline, 
):
    assert item is not None
    
    guard_transition(item=item, to_status=Status.CLASSIFIED)
    
    await db.update(
        query=f"""
            update ops.ingestion_queue
            set
                status = %s,
                classified = true,
                updated_at = now()
            where id = %s
        """,
        inputs=[
            Status.CLASSIFIED.name,
            item.id,
        ]
    )
    
class Status(Enum):
    WAITING_FOR_FETCH = "waiting_for_fetch"
    FETCHING = "fetching"
    FETCHED = "fetched"
    FETCH_FAILED = "fetch_failed"
    
    WAITING_FOR_LOAD = "waiting_for_load"
    LOADING = "loading"
    LOADED = "loaded"
    LOAD_FAILED = "load_failed"
    
    WAITING_FOR_CHUNK = "waiting_for_chunk"
    CHUNKING = "chunking"
    CHUNKED = "chunked"
    CHUNK_FAILED = "chunk_failed"
    
    WAITING_FOR_CLASSIFICATION = "waiting_for_classification"
    CLASSIFYING = "classifying"
    CLASSIFIED = "classified"
    CLASSIFICATION_FAILED = "classification_failed"
    
    WAITING_FOR_EMBEDDING = "waiting_for_embedding"
    EMBEDDING = "embedding"
    EMBEDDED = "embedded"
    
    FINISHED = "finished"
    
class StatusTransitionError(Exception):
    pass

def guard_transition(
    item: DocumentPipeline,
    to_status: Status, 
):
    allowed: dict[Status, dict] = {
        Status.WAITING_FOR_FETCH: {
            "to_status": Status.FETCHING,
            "criteria": lambda ip: ip.fetched == False,
        },
        Status.FETCHING: {
            "to_status": Status.FETCH_FAILED,
            "criteria": lambda ip: ip.fetched == False,
        },
        Status.FETCH_FAILED: {
            "to_status": Status.FETCHING,
            "criteria": lambda ip: ip.fetched == False,
        },
        Status.FETCHING: {
            "to_status": Status.FETCHED,
            "criteria": lambda ip: ip.fetched == False,
        },
        Status.FETCHED: {
            "to_status": Status.FETCH_FAILED,
            "criteria": lambda: True,
        },        
        Status.FETCHED: {
            "to_status": Status.WAITING_FOR_LOAD,
            "criteria": lambda ip: ip.fetched == True and ip.chunked == False and ip.document_id is not None,
        },
        
        Status.WAITING_FOR_LOAD: {
            "to_status": Status.LOADING,
            "criteria": lambda ip: ip.fetched == True and ip.loaded == False,
        },
        Status.LOADING: {
            "to_status": Status.LOAD_FAILED,
            "criteria": lambda ip: ip.fetched == True and ip.loaded == False,
        },
        Status.LOAD_FAILED: {
            "to_status": Status.LOADING,
            "criteria": lambda ip: ip.fetched == True and ip.loaded == False,
        },
        Status.LOADING: {
            "to_status": Status.LOADED,
            "criteria": lambda ip: ip.fetched == True and ip.loaded == False,
        },      
        Status.LOADED: {
            "to_status": Status.LOAD_FAILED,
            "criteria": lambda ip: ip.fetched == True,
        },      
        Status.LOADED: {
            "to_status": Status.WAITING_FOR_CHUNK,
            "criteria": lambda ip: ip.fetched == True and ip.loaded == True and ip.chunked == False and ip.document_id is not None,
        },
        
        Status.WAITING_FOR_CHUNK: {
            "to_status": Status.CHUNKING,
            "criteria": lambda ip: ip.fetched == True and ip.chunked == False,
        },
        Status.CHUNKING: {
            "to_status": Status.CHUNK_FAILED,
            "criteria": lambda ip: ip.fetched == True and ip.chunked == False,
        },
        Status.CHUNK_FAILED: {
            "to_status": Status.CHUNKING,
            "criteria": lambda ip: ip.fetched == True and ip.chunked == False,
        },
        Status.CHUNKING: {
            "to_status": Status.CHUNKED,
            "criteria": lambda ip: ip.fetched == True and ip.chunked == False,
        },
        Status.CHUNKED: {
            "to_status": Status.WAITING_FOR_CLASSIFICATION,
            "criteria": lambda ip: ip.fetched == True and ip.chunked == True and ip.classified == False,
        },
        
        Status.WAITING_FOR_CLASSIFICATION: {
            "to_status": Status.CLASSIFYING,
            "criteria": lambda ip: ip.fetched == True and ip.chunked == True and ip.classified == False,
        },
        Status.CLASSIFYING: {
            "to_status": Status.CLASSIFICATION_FAILED,
            "criteria": lambda ip: ip.fetched == True and ip.chunked == True and ip.classified == False,
        },
        Status.CLASSIFICATION_FAILED: {
            "to_status": Status.CLASSIFYING,
            "criteria": lambda ip: ip.fetched == True and ip.chunked == True and ip.classified == False,
        },
        Status.CLASSIFYING: {
            "to_status": Status.CLASSIFIED,
            "criteria": lambda ip: ip.fetched == True and ip.chunked == True and ip.classified == False,
        },
        Status.CLASSIFIED: {
            "to_status": Status.WAITING_FOR_EMBEDDING,
            "criteria": lambda ip: ip.fetched == True and ip.chunked == True and ip.classified == True and ip.embedded == False,
        },
        Status.CLASSIFIED: {
            "to_status": Status.WAITING_FOR_EMBEDDING,
            "criteria": lambda ip: ip.fetched == True and ip.chunked == True and ip.classified == True and ip.embedded == False,
        },
        
        Status.WAITING_FOR_EMBEDDING: {
            "to_status": Status.EMBEDDING,
            "criteria": lambda ip: ip.fetched == True and ip.chunked == True and ip.classified == True and ip.embedded == False,
        },
        Status.EMBEDDING: {
            "to_status": Status.EMBEDDED,
            "criteria": lambda ip: ip.fetched == True and ip.chunked == True and ip.classified == True and ip.embedded == False,
        },
        
        Status.EMBEDDED: {
            "to_status": Status.FINISHED,
            "criteria": lambda ip: ip.fetched == True and ip.chunked == True and ip.classified == True and ip.embedded == True,
        },
    }
    
    from_status = Status[item.status]
    transition = allowed[from_status]
    if to_status != transition["to_status"]:
        raise StatusTransitionError(f"transition from {item.status} to {to_status} is not allowed")
    
    if transition["criteria"](item) == False:
        raise StatusTransitionError(f"transition criteria failed from {item.status} to {to_status}. pipeline: {item}")
       