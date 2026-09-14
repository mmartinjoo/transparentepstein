import asyncio
from dataclasses import dataclass, asdict
import logging
import traceback
from typing import TypeAlias

from transparentepstein.core import celery
from transparentepstein.classification import create_classifier
from transparentepstein.classification.classifier.base import ClassificationLabel, ClassifierType

logger = logging.getLogger(__name__)
    
@dataclass
class ClassificationRequest():
    document_id: int
    content: str
    
@dataclass
class ClassificationResponse():
    document_id: int
    ok: bool
    label: str | None = None
    error: str | None = None

DocumentId: TypeAlias = str
DocumentContent: TypeAlias = str

@celery.app.task
def classify(requests: list[dict]):
    requests_mapped = [ClassificationRequest(**r) for r in requests]
    for request in requests_mapped:
        assert request.content is not None and len(request.content) != 0
        
    return asyncio.run(async_classify(requests_mapped))

async def async_classify(requests: list[ClassificationRequest]) -> list[dict]:
    coros = []
    for request in requests:
        coros.append(classify_one(request))
        
    return await asyncio.gather(*coros)

async def classify_one(request: ClassificationRequest) -> dict:
    try:
        # it's a simple regex classifier so the :500 makes sure that those keywords don't just randomly come up in a long document and gets classified as EMAIL
        classifier = create_classifier(type=ClassifierType.REGEX)
        label: ClassificationLabel = classifier.classify(content=request.content[:500])
        
        if label is None:
            raise ValueError("classifier returned None")
        
        return asdict(ClassificationResponse(
            document_id=request.document_id,
            label=label.name,
            ok=True,
        ))
    except Exception as exc:
        return asdict(ClassificationResponse(
            document_id=request.document_id,
            label=None,
            ok=False,
            error=traceback.format_exc(exc),
        ))