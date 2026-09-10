from enum import Enum
from pickle import TRUE

from transparentepstein.ingestion.models import IngestionPipeline

class IngestionPipelineStatusTransitionError(Exception):
    pass

class IngestionPipelineStatus(Enum):
    WAITING_FOR_FETCH = "waiting_for_fetch"
    FETCHING = "fetching"
    FETCHED = "fetched"
    
    WAITING_FOR_CHUNK = "waiting_for_chunk"
    CHUNKING = "chunking"
    CHUNKED = "chunked"
    
    WAITING_FOR_CLASSIFICATION = "waiting_for_classification"
    CLASSIFYING = "classifying"
    CLASSIFIED = "classified"
    
    WAITING_FOR_EMBEDDING = "waiting_for_embedding"
    EMBEDDING = "embedding"
    EMBEDDED = "embedded"
    
    FINISHED = "finished"
    
def guard_transition(
    ingestion_pipeline: IngestionPipeline,
    to_status: IngestionPipelineStatus, 
):
    allowed: dict[IngestionPipelineStatus, dict] = {
        IngestionPipelineStatus.WAITING_FOR_FETCH: {
            "to_status": IngestionPipelineStatus.FETCHING,
            "criteria": lambda ip: ip.fetched == False,
        },
        IngestionPipelineStatus.FETCHING: {
            "to_status": IngestionPipelineStatus.FETCHED,
            "criteria": lambda ip: ip.fetched == False,
        },
        IngestionPipelineStatus.FETCHED: {
            "to_status": IngestionPipelineStatus.WAITING_FOR_CHUNK,
            "criteria": lambda ip: ip.fetched == True and ip.chunked == False,
        },
        
        IngestionPipelineStatus.WAITING_FOR_CHUNK: {
            "to_status": IngestionPipelineStatus.CHUNKING,
            "criteria": lambda ip: ip.fetched == True and ip.chunked == False,
        },
        IngestionPipelineStatus.CHUNKING: {
            "to_status": IngestionPipelineStatus.CHUNKED,
            "criteria": lambda ip: ip.fetched == True and ip.chunked == False,
        },
        IngestionPipelineStatus.CHUNKED: {
            "to_status": IngestionPipelineStatus.WAITING_FOR_CLASSIFICATION,
            "criteria": lambda ip: ip.fetched == True and ip.chunked == True and ip.classified == False,
        },
        
        IngestionPipelineStatus.WAITING_FOR_CLASSIFICATION: {
            "to_status": IngestionPipelineStatus.CLASSIFYING,
            "criteria": lambda ip: ip.fetched == True and ip.chunked == True and ip.classified == False,
        },
        IngestionPipelineStatus.CLASSIFYING: {
            "to_status": IngestionPipelineStatus.CLASSIFIED,
            "criteria": lambda ip: ip.fetched == True and ip.chunked == True and ip.classified == False,
        },
        IngestionPipelineStatus.CLASSIFIED: {
            "to_status": IngestionPipelineStatus.WAITING_FOR_EMBEDDING,
            "criteria": lambda ip: ip.fetched == True and ip.chunked == True and ip.classified == True and ip.embedded == False,
        },
        
        IngestionPipelineStatus.WAITING_FOR_EMBEDDING: {
            "to_status": IngestionPipelineStatus.EMBEDDING,
            "criteria": lambda ip: ip.fetched == True and ip.chunked == True and ip.classified == True and ip.embedded == False,
        },
        IngestionPipelineStatus.EMBEDDING: {
            "to_status": IngestionPipelineStatus.EMBEDDED,
            "criteria": lambda ip: ip.fetched == True and ip.chunked == True and ip.classified == True and ip.embedded == False,
        },
        
        IngestionPipelineStatus.EMBEDDED: {
            "to_status": IngestionPipelineStatus.FINISHED,
            "criteria": lambda ip: ip.fetched == True and ip.chunked == True and ip.classified == True and ip.embedded == True,
        },
    }
    
    from_status = IngestionPipelineStatus[ingestion_pipeline.status]
    transition = allowed[from_status]
    if to_status != transition["to_status"]:
        raise IngestionPipelineStatusTransitionError(f"transition from {ingestion_pipeline.status} to {to_status} is not allowed")
    
    if transition["criteria"](ingestion_pipeline) == False:
        raise IngestionPipelineStatusTransitionError(f"transition criteria failed from {ingestion_pipeline.status} to {to_status}. pipeline: {ingestion_pipeline}")
        