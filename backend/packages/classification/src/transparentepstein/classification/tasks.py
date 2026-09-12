import asyncio
from dataclasses import dataclass, asdict
import logging
import traceback

from transparentepstein.core import celery
from transparentepstein.classification import create_classifier
from transparentepstein.classification.classifier.base import ClassificationLabel, ClassifierType

logger = logging.getLogger(__name__)

@dataclass
class ClassificationResult():
    document_id: int
    item_id: int
    ok: bool
    label: str | None = None
    error: str | None = None

@celery.app.task
def classify(context: dict[str, dict[str, any]]):
    for item_id in context.keys():
        assert "document_id" in context[item_id]
        assert "document_content" in context[item_id]
        assert context[item_id]["document_id"] is not None
        assert context[item_id]["document_content"] is not None

    return asyncio.run(async_classify(context))

async def async_classify(context: dict) -> list[dict]:
    coros = []
    for item_id in context.keys():
        document_id = context[item_id]["document_id"]
        document_content = context[item_id]["document_content"]
        
        coros.append(classify_one(document_content, document_id, item_id))
        
    return await asyncio.gather(*coros)

async def classify_one(content: str, document_id: int, item_id: int) -> dict:
    try:
        if content is None or len(content) == 0:
            raise ValueError(f"content is None for document {document_id}")
        
        # it's a simple regex classifier so the :500 makes sure that those keywords don't just randomly come up in a long document and gets classified as EMAIL
        classifier = create_classifier(type=ClassifierType.REGEX)
        label: ClassificationLabel = classifier.classify(content=content[:500])
        
        if label is None:
            raise ValueError("classifier returned None")
        
        return asdict(ClassificationResult(
            document_id=document_id,
            item_id=item_id,
            label=label.name,
            ok=True,
        ))
    except Exception as exc:
        return asdict(ClassificationResult(
            document_id=document_id,
            item_id=item_id,
            label=None,
            ok=False,
            error=traceback.format_exc(exc),
        ))