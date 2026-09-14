from transparentepstein.core import db

async def update_data_set_processed_until(data_set_id: int):
    await db.update(
        query="""
            update ops.data_sets
            set processed_until_page = processed_until_page + 1
            where id = %s 
        """,
        inputs=[data_set_id],
    )
    
async def update_document_s3_key(document_id: int, s3_key: str):
    await db.update(
        query="""
            update ops.documents
            set s3_key = %s
            where id = %s 
        """,
        inputs=[s3_key, document_id],
    )