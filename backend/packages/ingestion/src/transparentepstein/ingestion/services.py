import asyncio
from pprint import pprint
import pymupdf

from psycopg.rows import class_row

from transparentepstein.core import db, storage
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

async def load_document_content(document: Document) -> str:
    data = await asyncio.to_thread(storage.get_file, document.s3_key)
    doc = await asyncio.to_thread(pymupdf.open, stream=data, filetype="pdf")
    content = await asyncio.to_thread(_load_pages, doc=doc)
    return content

async def update_document_content(document_id: int, content: str):
    await db.update(
        query="""
            update ops.documents
            set 
                content = %s,
                updated_at = now()
            where id = %s
        """,
        inputs=[
            content,
            document_id,
        ],
    )

def _load_pages(doc) -> str:
    content: str = ""
    for page in doc:
        text = page.get_text()
        content += "\n" + text
    return content