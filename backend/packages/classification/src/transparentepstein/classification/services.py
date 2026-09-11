from transparentepstein.core import db
from transparentepstein.classification.classifier.base import ClassificationLabel

async def update_document_main_classification_label(document_id: int, label: ClassificationLabel):
    await db.update(
        query="""
            update ops.documents
            set 
                main_classification_label = %s,
                updated_at = now()
            where id = %s
        """,
        inputs=[
            label.name,
            document_id,
        ],
    )