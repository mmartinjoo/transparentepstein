from enum import Enum, auto
from psycopg.rows import class_row

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
    
class StageTransitionError(Exception):
    pass
    
async def transition_to_next_stage(document_id: int, current_stage: Stage):
    transitions = {
        Stage.FETCH: Stage.LOAD,
        Stage.LOAD: Stage.CHUNK,
        Stage.CHUNK: Stage.CLASSIFY,
        Stage.CLASSIFY: Stage.EMBEDDING,
    }
    
    try:
        next_stage = transitions[current_stage]
    except KeyError as exc:
        raise StageTransitionError(f"no next stage found for {current_stage}")
    
    await db.update(
        query="""
            update ops.document_pipeline
            set 
                stage = %s,
                stage_status = %s,
                updated_at = now()
            where document_id = %s
        """,
        inputs=[
            next_stage.name,
            StageStatus.PENDING.name,
            document_id,
        ],
    )
    
async def fetch(stage: Stage, stage_status: StageStatus) -> list[Document]:
    return await db.select_many(
        query="""
            select documents.*
            from ops.document_pipeline as pipeline
            join ops.documents as documents on documents.id = pipeline.document_id
            where pipeline.stage = %s
            and pipeline.stage_status = %s
        """,
        inputs=[
            stage.name,
            stage_status.name,
        ],
        row_factory=class_row(Document),
    )