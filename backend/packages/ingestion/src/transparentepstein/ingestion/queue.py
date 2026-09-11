from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from psycopg.rows import class_row

from transparentepstein.core import db

MAX_ATTEMPTS = 10
FETCH_LIMIT = 20

@dataclass
class Item():
    id: int
    url: str
    data_set_id: int
    fetched: bool
    chunked: bool
    classified: bool
    embedded: bool
    status: str
    error: str
    attempts: int
    next_attempt_at: datetime
    created_at: datetime
    updated_at: datetime
    finished_at: datetime

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
            
async def dequeue_for_fetch() -> list[Item]:
    pool = await db.apool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=class_row(Item)) as cur:
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
        
async def find_items(item_ids: list[int]) -> list[Item]:
    in_clause = ','.join(['%s'] * len(item_ids))
    return await db.select_many(
        query=f"""
            select *
            from ops.ingestion_queue                   
            where id in ({in_clause})
        """,
        inputs=tuple(item_ids),
        row_factory=class_row(Item),
    )
    
class Status(Enum):
    WAITING_FOR_FETCH = "waiting_for_fetch"
    FETCHING = "fetching"
    FETCHED = "fetched"
    FETCH_FAILED = "fetch_failed"
    
    WAITING_FOR_CHUNK = "waiting_for_chunk"
    CHUNKING = "chunking"
    CHUNKED = "chunked"
    
    WAITING_FOR_CLASSIFICATION = "waiting_for_classification"
    CLASSIFYING = "classifying"
    CLASSIFIED = "classified"
    
    WAITING_FOR_EMBEDDING = "waiting_for_embedding"
    EMBEDDING = "embedding"
    EMBEDDED = "embedded"
    
    FINISHED = "finished"
    
class StatusTransitionError(Exception):
    pass

def guard_transition(
    item: Item,
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
            "to_status": Status.WAITING_FOR_CHUNK,
            "criteria": lambda ip: ip.fetched == True and ip.chunked == False,
        },
        
        Status.WAITING_FOR_CHUNK: {
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
            "to_status": Status.CLASSIFIED,
            "criteria": lambda ip: ip.fetched == True and ip.chunked == True and ip.classified == False,
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
       