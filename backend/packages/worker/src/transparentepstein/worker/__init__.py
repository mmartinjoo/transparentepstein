from transparentepstein.core.celery import app

app.autodiscover_tasks([
    "transparentepstein.ingestion",
    "transparentepstein.classification",
])