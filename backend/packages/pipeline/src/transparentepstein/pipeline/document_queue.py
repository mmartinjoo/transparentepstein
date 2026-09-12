from psycopg.rows import class_row

from transparentepstein.core import db
from transparentepstein.pipeline.document_pipeline import Stage, StageStatus
from transparentepstein.pipeline.models import Document

MAX_ATTEMPTS = 15

async def enqueue(document: Document):
    await db.insert(
        query="""
            insert into ops.document_queue(document_id)
            values(%s)   
        """,
        inputs=[document.id],
    )
    
async def dequeue(stage: Stage) -> list[Document]:
    return await db.select_many(
        query="""
            select documents.*
            from ops.document_queue as queue
            join ops.document_pipeline as pipeline on pipeline.document_id = queue.document_id
            join ops.documents as documents on documents.id = queue.document_id
            where pipeline.stage = %s
            and pipeline.stage_status in (%s, %s)
            and queue.attempts < %s
            and queue.next_attempt_at <= now()
            order by queue.queued_at desc
            limit 100
            for update skip locked
        """,
        inputs=[
            stage.name,
            StageStatus.PENDING.name,
            StageStatus.FAILED.name,
            MAX_ATTEMPTS,
        ],
        row_factory=class_row(Document),
    )

async def remove(document_id: int):
    await db.delete(
        query="""
            delete from ops.document_queue
            where document_id = %s
        """,
        inputs=[
            document_id
        ],
    )