import asyncio
import logging
import aiohttp
from celery import Celery

from transparentepstein.core.config import settings
from transparentepstein.core import storage
from transparentepstein.ingestion import queue, selectors

app = Celery("tasks", broker=settings.redis_url, backend=settings.redis_url)

logger = logging.getLogger(__name__)

HEADERS = {
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
    "Cookie": "justiceGovAgeVerified=true",
}

MAX_CONCURRENT_FETCHES = 8

@app.task
def fetch(item_ids: list[int]):
    return asyncio.run(async_fetch(item_ids))

async def async_fetch(item_ids: list[int]):
    items = await queue.find_items(item_ids=item_ids)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_FETCHES)
    
    # No total timeout since files can be large. Only fail if can't connect or see no data for 60s
    timeout = aiohttp.ClientTimeout(total=None, connect=10, sock_read=60)
    
    async with aiohttp.ClientSession(headers=HEADERS, timeout=timeout) as session:
        results = await asyncio.gather(
            *[fetch_one(session, semaphore, item.url, item.data_set_id) for item in items]
        )
        
        ok = [r for r in results if r["ok"]]
        failed = [r for r in results if not r["ok"]]
        logger.info(f"fetched {len(results)} URLs. Succeeded: {len(ok)}, failed: {len(failed)}")
        return {
            "total": len(results),
            "ok": len(ok),
            "failed": len(failed),
        }
    
async def fetch_one(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    url: str,
    data_set_id: int,
):
    async with semaphore:
        try:
            parts = url.split("/")
            filename = parts[-1]            
            async with session.get(url) as response:
                if response.status != 200:
                    return {"ok": False, "url": url, "error": f"{response.status}"}

                data = await response.read()
                
            data_set = await selectors.find_data_set(data_set_id=data_set_id)
            key = await asyncio.to_thread(storage.put_file, data_set.name, filename, data)
            logger.info(f"saved {len(data)} bytes to {key}")
            return {"ok": True, "url": url, "key": key}
            
        except Exception as exc:
            logger.info(f"fetch failed for {url} with error: {exc}")
            return {"ok": False, "url": url, "error": repr(exc)}