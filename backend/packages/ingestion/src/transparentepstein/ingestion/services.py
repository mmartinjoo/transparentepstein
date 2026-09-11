from psycopg.rows import class_row

from transparentepstein.core import db
from transparentepstein.ingestion.models import Document

async def create_document(url: str, s3_key: str, data_set_id: int) -> Document:
    return await db.insert(
        query="""
            insert into ops.documents(url, s3_key, data_set_id)
            values(%s, %s, %s)
            returning *
        """,
        inputs=[
            url,
            s3_key,
            data_set_id,
        ],
        row_factory=class_row(Document),
    )