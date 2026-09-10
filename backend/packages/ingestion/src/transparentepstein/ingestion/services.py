from transparentepstein.core import db

async def add_urls_to_ingestion_pipeline(urls: list[str], data_set_id: int):
    assert data_set_id is not None
    
    pool = await db.apool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            for url in urls:
                await cur.execute("""
                    insert into ops.ingestion_pipeline(data_set_id, url)
                    values(%s, %s)              
                """, [data_set_id, url])
                
            await cur.execute("""
                update ops.data_sets
                set processed_until_page = processed_until_page + 1
                where id = %s     
            """, [data_set_id])
            
            await conn.commit()