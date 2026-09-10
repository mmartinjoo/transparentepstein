from pprint import pprint
import logging
import aiohttp
from bs4 import BeautifulSoup
import re

from transparentepstein.core import db
from transparentepstein.ingestion.models import DataSet
from psycopg.rows import class_row

logger = logging.getLogger(__name__)

class RequestFailedError(Exception):
    pass

async def discover(data_set_id: int):
    data_set: DataSet = await db.select_one(
        query="""
            select *
            from ops.data_sets                   
            where id = %s
            limit 1
        """,
        inputs=[data_set_id],
        row_factory=class_row(DataSet),
    )
    
    next_page = data_set.processed_until_page + 1
    if next_page >= data_set.max_pages:
        logger.info(f"data set {data_set_id} has been fully processed")
        
    url = data_set.url + f"?page={next_page}"
    logger.info(f"discovering {url} for {data_set.name}")
    
    headers = {
        "Accept": "text/html",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
        "Sec-ch-ua": '"Not=A?Brand";v="99", "Google Chrome";v="151", "Chromium";v="151"',
        "Sec-ch-ua-mobile": "?0",
        "Sec-ch-ua-platform": '"macOS"',
        "Sec-fetch-dest": "document",
        "Sec-fetch-mode": "navigate",
        "Sec-fetch-site": "none",
        "Sec-fetch-user": "?1",
        "Upgrade-insecure-requests": "1",
    }
    
    async with aiohttp.ClientSession(headers=headers) as session:        
        async with session.get(url) as response:
            logging.info(f"status code {response.status}")
            
            html = await response.text()
            
            if response.status != 200:
                raise RequestFailedError(f"request to {url} failed with status {response.status} and response: {html[:1000]}")
            
            soup = BeautifulSoup(html, "html.parser")
            a_tags = soup.find_all(href=re.compile("https://www.justice.gov/epstein/files/"))
            urls = [a["href"] for a in a_tags]
            
            pool = await db.apool()
            async with pool.connection() as conn:
                async with conn.cursor() as cur:
                    for url in urls:
                        await cur.execute("""
                            insert into ops.ingestion_pipeline(data_set_id, url)
                            values(%s, %s)              
                        """, [data_set.id, url])
                        
                    await cur.execute("""
                        update ops.data_sets
                        set processed_until_page = processed_until_page + 1
                        where id = %s     
                    """, [data_set.id])
                    
                    await conn.commit()
                    
            logger.info(f"discovered {len(urls)} URLs on page {next_page}")