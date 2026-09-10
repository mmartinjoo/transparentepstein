from transparentepstein.core.db import select
from transparentepstein.ingestion.models import DataSet
from psycopg.rows import class_row

async def discover(data_set_id: int) -> list[DataSet]:
    return await select(
        query="""
            select *
            from ops.data_sets                   
            where id = %s
            limit 1
        """,
        inputs=[data_set_id],
        fetchall=False,
        row_factory=class_row(DataSet),
    )