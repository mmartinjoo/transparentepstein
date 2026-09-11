import asyncio
import logging
from pprint import pprint
import aiohttp
from celery import Celery

from transparentepstein.core.config import settings
from transparentepstein.core import storage
from transparentepstein.ingestion import queue, selectors

app = Celery("tasks", broker=settings.redis_url, backend=settings.redis_url)

logger = logging.getLogger(__name__)

@app.task
def fetch(item_ids: list[int]):
    return asyncio.run(async_fetch(item_ids))

async def async_fetch(item_ids: list[int]):
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
    
    items = await queue.find_items(item_ids=item_ids)
    
    async with aiohttp.ClientSession(headers=headers) as session:
        for item in items:
            parts = item.url.split("/")
            filename = parts[-1]
            data_set = await selectors.find_data_set(data_set_id=item.data_set_id)
            
            async with session.get(item.url) as response:
                logger.info(f"status code {response.status} for {filename}")
                
                data: bytes = await response.read()
                
                storage.put_file(
                    data_set_name=data_set.name,
                    doc_name=filename,
                    data=data,
                )