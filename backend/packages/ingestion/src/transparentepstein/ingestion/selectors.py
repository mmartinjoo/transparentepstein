from psycopg.rows import class_row
from transparentepstein.ingestion.models import DataSet, Document
from transparentepstein.core import db
from transparentepstein.ingestion.queue import Item

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