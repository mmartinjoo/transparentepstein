from psycopg.rows import class_row

from transparentepstein.core import db
from transparentepstein.pipeline.document_pipeline import Stage, StageStatus
from transparentepstein.pipeline.models import Document

MAX_ATTEMPTS = 15

class EmptyQueueError(Exception):
    pass

async def enqueue(document: Document):
    await db.insert(
        query="""
            insert into ops.document_queue(document_id)
            values(%s)   
        """,
        inputs=[document.id],
    )
    
async def claim(stage: Stage, n: int = 100) -> list[Document]:
    documents = await db.select_many(
        query="""
            select documents.*
            from ops.document_queue as queue
            join ops.document_pipeline as pipeline on pipeline.document_id = queue.document_id
            join ops.documents as documents on documents.id = queue.document_id
            where pipeline.stage = %s
            and pipeline.stage_status in (%s, %s)
            and queue.attempts < %s
            and queue.next_attempt_at <= now()
            or (
                pipeline.stage = %s
                and pipeline.stage_status = %s
                and queue.attempts < %s
                and queue.next_attempt_at <= now()
                and queue.claimed_until <= now()
            )
            order by queue.queued_at desc
            limit %s
            for update of queue skip locked
        """,
        inputs=[
            stage.name,
            StageStatus.PENDING.name,
            StageStatus.FAILED.name,
            MAX_ATTEMPTS,
            stage.name,
            StageStatus.IN_PROGRESS,
            MAX_ATTEMPTS,
            n,
        ],
        row_factory=class_row(Document),
    )
    
    if len(documents) == 0:
        raise EmptyQueueError()
    
    in_clause = ','.join(['%s'] * len(documents))
    await db.update(
        query=f"""
            update ops.document_queue
            set
                claimed_at = now(),
                claimed_until = now() + interval '30 minutes',
                attempts = attempts + 1,
                next_attempt_at = now()
            where document_id in ({in_clause})
        """,
        inputs=[d.id for d in documents],
    )
    
    return documents
    
async def dequeue(document_id: int):
    await db.delete(
        query="""
            delete from ops.document_queue
            where document_id = %s
        """,
        inputs=[
            document_id
        ],
    )