from enum import Enum, auto

from transparentepstein.core import db

from transparentepstein.pipeline.models import Document


class Stage(Enum):
    FETCH = auto()
    LOAD = auto()
    CHUNK = auto()
    CLASSIFY = auto()
    EMBEDDING = auto()
    
class StageStatus(Enum):
    PENDING = auto()
    IN_PROGRESS = auto()
    DONE = auto()
    FAILED = auto()
    
async def initialize(document: Document):
    await db.insert(
        query="""
            insert into ops.document_pipeline(document_id, stage, stage_status)
            values(%s, %s, %s)
        """,
        inputs=[
            document.id,
            Stage.FETCH.name,
            StageStatus.PENDING.name,
        ]
    )
    
async def mark_many(document_ids: list[int], stage_status: StageStatus):
    in_clause = ','.join(['%s'] * len(document_ids))
    await db.update(
        query=f"""
            update ops.document_pipeline
            set
                stage_status = %s,
                updated_at = now()
            where document_id in ({in_clause})
        """,
        inputs=[
            stage_status.name,
            *[id for id in document_ids]
        ],
    )
    
async def mark_one(document_id: int, stage_status: StageStatus, error: str | None = None):
    await db.update(
        query=f"""
            update ops.document_pipeline
            set
                stage_status = %s,
                updated_at = now(),
                error = %s
            where document_id = %s
        """,
        inputs=[
            stage_status.name,
            error,
            document_id,
        ],
    )