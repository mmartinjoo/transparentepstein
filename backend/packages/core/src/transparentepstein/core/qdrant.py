from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct

from transparentepstein.core.config import settings

client = QdrantClient(f"http://{settings.qdrant_host}:{settings.qdrant_port}")

def create_collection_if_not_exists(collection_name: str):
    if client.collection_exists(collection_name):
        return
    
    client.create_collection(
        collection_name=collection_name,
        vectors_config=VectorParams(
            size=settings.qdrant_vector_dims,
            distance=Distance.COSINE,
        ),
    )

def upsert(collection_name: str, vectors, document_id: int, document_chunk_ids: list[int]):
    assert document_id is not None
    assert len(document_chunk_ids) != 0
    
    create_collection_if_not_exists(collection_name)
    
    points = []
    for idx, vector in enumerate(vectors):
        points.append(PointStruct(
            id=document_chunk_ids[idx],
            vector=vector,
            payload={
                "document_id": document_id,
                "document_chunk_id": document_chunk_ids[idx],
            },
        ))
    
    client.upsert(
        collection_name="document_chunks",
        points=points,
    )
