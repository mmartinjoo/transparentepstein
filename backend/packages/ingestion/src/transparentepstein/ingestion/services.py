import asyncio
from typing import TypeAlias
from pprint import pprint
import pymupdf
from psycopg.rows import class_row

from transparentepstein.classification.classifier.base import ClassificationLabel
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
        returning=True,
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
    
async def update_document_main_classification_label(document_id: int, label: ClassificationLabel):
    await db.update(
        query="""
            update ops.documents
            set 
                main_classification_label = %s,
                updated_at = now()
            where id = %s
        """,
        inputs=[
            label.name,
            document_id,
        ],
    )
    
async def create_document_chunks(document_id: int, chunks: list[str]):
    assert len(chunks) != 0
    
    for idx, chunk in enumerate(chunks):
        await db.insert(
            query="""
                insert into ops.document_chunks(document_id, position, content, created_at)
                values(%s, %s, %s, now())
            """,
            inputs=[
                document_id,
                idx,
                chunk,
            ]
        )

def _load_pages(doc) -> str:
    content: str = ""
    for page in doc:
        text = page.get_text()
        content += "\n" + text
    return content

TokenCount: TypeAlias = int

def chunk_text(text: str, size: TokenCount = 512, overlap: TokenCount = 50) -> list[str]:
    # 3.84 letter = 1 token
    token_to_letter = 3.84
    
    size_in_char = int(size * token_to_letter)
    overlap_in_char = int(overlap * token_to_letter)
    step_in_char = int(size_in_char - overlap_in_char)
    
    return [
        text[i:i + size_in_char]
        for i in range(0, len(text), step_in_char)
    ]