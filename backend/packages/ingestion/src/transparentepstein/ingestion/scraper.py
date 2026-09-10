from transparentepstein.core.db import select_many, select_one
from transparentepstein.ingestion.models import DataSet
from psycopg.rows import class_row

async def discover(data_set_id: int) -> list[DataSet]:
    return await select_one(
        query="""
            select *
            from ops.data_sets                   
            where id = %s
            limit 1
        """,
        inputs=[data_set_id],
        row_factory=class_row(DataSet),
    )