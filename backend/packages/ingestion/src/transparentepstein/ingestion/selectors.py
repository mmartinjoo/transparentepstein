from psycopg.rows import class_row
from transparentepstein.ingestion.models import DataSet, Document, DocumentChunk
from transparentepstein.core import db

async def find_data_set(data_set_id: int) -> DataSet:
    assert data_set_id is not None
    
    return await db.select_one(
        query="""
            select *
            from ops.data_sets                   
            where id = %s
            limit 1
        """,
        inputs=[data_set_id],
        row_factory=class_row(DataSet),
    )
    
async def find_document(id: int) -> Document:
    assert id is not None
        
    return await db.select_one(
        query="""
            select *
            from ops.documents                   
            where id = %s
            limit 1
        """,
        inputs=[id],
        row_factory=class_row(Document),
    )
    
async def fetch_document_content(id: int) -> str:
    assert id is not None
        
    res = await db.select_one(
        query="""
            select content
            from ops.documents                   
            where id = %s
            limit 1
        """,
        inputs=[id],
    )
    return res["content"]
    
async def count_document_chunks_by_document(document_id: int) -> int:
    assert document_id is not None
        
    res = await db.select_one(
        query="""
            select count(*)
            from ops.document_chunks                  
            where document_id = %s
        """,
        inputs=[document_id],
    )
    return int(res["count"])