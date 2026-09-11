from celery import Celery

from transparentepstein.core.config import settings

app = Celery(
    "transparentepstein",
    broker=settings.redis_url,
    backend=settings.redis_url,
)