import logging
import aiohttp
from bs4 import BeautifulSoup
import re

from transparentepstein.core import db
from transparentepstein.ingestion.models import DataSet

logger = logging.getLogger(__name__)

class RequestFailedError(Exception):
    pass

async def discover(data_set: DataSet, page: int) -> list[str]:
    url = data_set.url + f"?page={page}"
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
            logger.info(f"status code {response.status}")
            
            html = await response.text()
            
            if response.status != 200:
                raise RequestFailedError(f"request to {url} failed with status {response.status} and response: {html[:1000]}")
            
            soup = BeautifulSoup(html, "html.parser")
            a_tags = soup.find_all(href=re.compile("https://www.justice.gov/epstein/files/"))
            urls = [a["href"] for a in a_tags]
            
            return urls
            
            