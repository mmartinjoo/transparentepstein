import asyncio
from dataclasses import asdict
from datetime import datetime
import logging
from pprint import pprint

from transparentepstein.classification.classifier.base import ClassificationLabel
from transparentepstein.ingestion import selectors, tasks
from transparentepstein.classification import services as classification_services
from transparentepstein.pipeline.services import update_data_set_processed_until, update_document_s3_key
from transparentepstein.core import db, storage
from transparentepstein.classification.tasks import classify as classify_task, ClassificationRequest, ClassificationResponse
from transparentepstein.embedding.tasks import embed, EmbedRequest, EmbedResponse
from transparentepstein.pipeline import document_queue, document_pipeline

logger = logging.getLogger(__name__)

class NothingToDiscoverError(Exception):
    pass

async def discover_stage():
    data_sets = await selectors.fetch_next_data_sets(n=5)
    task_results = []
    responses: list[tasks.DiscoverResponse] = []
    
    for data_set in data_sets:
        try:
            task_results.append(tasks.discover.delay(asdict(data_set)))
        except Exception as exc:
            logger.error(f"discover task for data set {data_set.id} failed: {repr(exc)}")
            continue
        
    results: list[dict] = await asyncio.gather(
        *[asyncio.to_thread(task_result.get, timeout=300) for task_result in task_results],
        return_exceptions=True,
    )
    
    for res in results:
        if isinstance(res, BaseException):
            logger.error(f"discover failed while collecting results: {repr(res)}")
            continue
        
        try:          
            responses.append(tasks.DiscoverResponse(**res))
        except Exception as exc:
            logger.error(f"discover failed for data set {res["data_set_id"]} while collecting results: {repr(exc)}")
            continue

    document_ids = []
    for res in responses:
        assert res.data_set_id is not None
        assert res.document_ids is not None
        
        if len(res.document_ids) == 0:
            logger.warning(f"nothing to discover in data set {res.data_set_id}")
            continue
        
        document_ids.extend(res.document_ids)

        try:
            await update_data_set_processed_until(data_set_id=res.data_set_id)
        except Exception as exc:
            logger.error(f"failed to update data set: {repr(exc)}")
            continue
        
    logger.info(f"discovered {len(document_ids)} documents in {len(data_sets)}")
    logger.info(f"data set IDs: {[ds.id for ds in data_sets]}")
    logger.info(f"document IDs: {document_ids}")
    
async def initialize_stage():
    documents = await selectors.fetch_uninitialized_documents()
    for document in documents:
        try:
            async with db.transaction():
                await document_queue.enqueue(document=document)
                await document_pipeline.initialize(document)
        except Exception as exc:
            logger.error(f"initializing document {document.id} failed: {repr(exc)}")
            continue
    logger.info(f"initialized {len(documents)} documents")

async def fetch_stage():
    documents = await document_queue.claim(stage=document_pipeline.Stage.FETCH)
    logger.info(f"fetching {len(documents)} documents")
    
    batch_size = len(documents) // 5
    task_results = []
    responses: list[tasks.FetchResponse] = []

    for i in range(5):
        try:
            async with db.transaction():            
                start = i * batch_size
                batch = documents[start:start + batch_size]
                document_ids = [d.id for d in batch]

                await document_pipeline.mark_many(
                    document_ids=document_ids,
                    stage_status=document_pipeline.StageStatus.IN_PROGRESS,
                )
                logger.info(f"marked {len(batch)} documents as in progress")
                task_results.append(tasks.fetch.delay([asdict(d) for d in batch]))
        except Exception as exc:
            async with db.transaction():
                await document_pipeline.mark_many(
                    document_id=document_ids,
                    stage_status=document_pipeline.StageStatus.FAILED,
                    error=f"fetch failed for batch {i} while dispatching tasks: {repr(exc)}",
                )
                logger.error(f"fetch failed for batch {i} while dispatching tasks: {repr(exc)}")
                continue
      
    results: list[list[dict]] = await asyncio.gather(
        *[asyncio.to_thread(task_result.get, timeout=300) for task_result in task_results],
        return_exceptions=True,
    )
        
    for r in results:
        for item in r:
            if isinstance(item, BaseException):
                logger.error(f"fetch failed while collecting results: {repr(item)}")
                continue
            
            try:          
                responses.append(tasks.FetchResponse(**item))
            except Exception as exc:
                await document_pipeline.mark_one(
                    document_id=item["document_id"],
                    stage_status=document_pipeline.StageStatus.FAILED,
                    error=f"fetch failed while collecting results: {repr(exc)}",
                )
                logger.error(f"fetch failed for document {item["document_id"]} while collecting results: {repr(exc)}")
            
    for res in responses:
        try:
            async with db.transaction():
                if res.ok:
                    await update_document_s3_key(document_id=res.document_id, s3_key=res.s3_key)
                    await document_pipeline.mark_one(
                        document_id=res.document_id, 
                        stage_status=document_pipeline.StageStatus.DONE,
                    )
                    logger.info(f"document {res.document_id} fetched with S3 key {res.s3_key}")
                else:
                    await document_pipeline.mark_one(
                        document_id=res.document_id,
                        stage_status=document_pipeline.StageStatus.FAILED,
                        error=res.error,
                    )
                    logger.error(f"fetch failed for {res.document_id}, error: {res.error}")
        except Exception as exc:
            # fresh txn: previous one already rolled back
            async with db.transaction():
                await document_pipeline.mark_one(
                    document_id=res.document_id,
                    stage_status=document_pipeline.StageStatus.FAILED,
                    error=f"fetch failed while processing results: {repr(exc)}",
                )
                logger.error(f"fetch failed for document {res.document_id} while processing results: {repr(exc)}")
    
async def transition_to_load_stage():
    documents = await document_pipeline.fetch(
        stage=document_pipeline.Stage.FETCH,
        stage_status=document_pipeline.StageStatus.DONE,
    )
    
    for document in documents:
        try:
            exists = await asyncio.to_thread(storage.exists, key=document.s3_key)
            if not exists:
                await document_pipeline.mark_one(
                    document_id=document.id,
                    stage_status=document_pipeline.StageStatus.FAILED,
                    error=f"S3 object does not exist at {document.s3_key} for document {document.id}",
                )
                logger.error(f"S3 object does not exist at {document.s3_key} for document {document.id}")
                continue
                
            empty = await asyncio.to_thread(storage.empty, key=document.s3_key)
            if empty:
                await document_pipeline.mark_one(
                    document_id=document.id,
                    stage_status=document_pipeline.StageStatus.FAILED,
                    error=f"S3 object exists but empty for document {document.id} at {document.s3_key}",
                )
                logger.error(f"S3 object exists but empty for document {document.id} at {document.s3_key}")
                continue

            await document_pipeline.transition_to_next_stage(
                document_id=document.id,
                current_stage=document_pipeline.Stage.FETCH,
            )
            logger.info(f"document {document.id} transitioned to {document_pipeline.Stage.LOAD}")
        except Exception as exc:
            await document_pipeline.mark_one(
                document_id=document.id,
                stage_status=document_pipeline.StageStatus.FAILED,
                error=f"transition to load stage failed: {repr(exc)}",
            )
            logger.error(f"transition to load stage failed: {exc}")
          
async def load_stage():
    documents = await document_queue.claim(stage=document_pipeline.Stage.LOAD)
    batch_size = len(documents) // 5
    task_results = []
    responses: list[tasks.LoadResponse] = []
    
    for i in range(5):
        try:
            async with db.transaction():                
                start = i * batch_size
                batch = documents[start:start + batch_size]
                document_ids = [d.id for d in batch]

                await document_pipeline.mark_many(
                    document_ids=document_ids,
                    stage_status=document_pipeline.StageStatus.IN_PROGRESS,
                )
                logger.info(f"marked {len(batch)} documents as in progress")
                task_results.append(tasks.load.delay([asdict(d) for d in batch]))
        except Exception as exc:
            # fresh txn: previous one already rolled back
            async with db.transaction():
                await document_pipeline.mark_many(
                    document_ids=[d.id for d in batch],
                    stage_status=document_pipeline.StageStatus.FAILED,
                    error=f"load failed for batch {i} while dispatching tasks: {repr(exc)}",
                )
                logger.error(f"load failed for batch {i} while dispatching tasks: {repr(exc)}")
                continue
    
    results: list[list[dict]] = await asyncio.gather(
        *[asyncio.to_thread(task_result.get, timeout=300) for task_result in task_results],
        return_exceptions=True,
    )
        
    for r in results:
        for item in r:
            if isinstance(item, BaseException):
                logger.error(f"load failed while collecting results: {repr(item)}")
                continue
            
            try:
                responses.append(tasks.LoadResponse(**item))
            except Exception as exc:
                await document_pipeline.mark_one(
                    document_id=item["document_id"],
                    stage_status=document_pipeline.StageStatus.FAILED,
                    error=f"load failed while collecting results: {repr(exc)}",
                )
                logger.error(f"load failed for document {item["document_id"]} while collecting results: {repr(exc)}, results: {item}")
                continue
    
    for res in responses:
        try:
            async with db.transaction():
                if res.ok:
                    await document_pipeline.mark_one(
                        document_id=res.document_id,
                        stage_status=document_pipeline.StageStatus.DONE,
                    )
                    logger.info(f"document {res.document_id} loaded")
                else:
                    await document_pipeline.mark_one(
                        document_id=res.document_id,
                        stage_status=document_pipeline.StageStatus.FAILED,
                        error=res.error
                    )
                    logger.error(f"load failed for {res.document_id}, error: {res.error}")
        except Exception as exc:
            # fresh txn: previous one already rolled back
            async with db.transaction():
                await document_pipeline.mark_one(
                    document_id=res.document_id,
                    stage_status=document_pipeline.StageStatus.FAILED,
                    error=f"load failed: {repr(exc)}",
                )
                logger.error(f"load failed for document {res.document_id}: {exc}")
            
async def transition_to_chunk_stage():
    documents = await document_pipeline.fetch(
        stage=document_pipeline.Stage.LOAD,
        stage_status=document_pipeline.StageStatus.DONE,
    )
    for document in documents:
        try:
            if document.content is None or len(document.content) == 0:
                await document_pipeline.mark_one(
                    document_id=document.id,
                    stage_status=document_pipeline.StageStatus.FAILED,
                    error=f"content is empty for {document.id}"
                )
                logger.error(f"document {document.id} marked as failed: content is empty")
                continue
                
            await document_pipeline.transition_to_next_stage(
                document_id=document.id,
                current_stage=document_pipeline.Stage.LOAD,
            )
            logger.info(f"document {document.id} transitioned to {document_pipeline.Stage.CHUNK} stage")            
        except Exception as exc:
            await document_pipeline.mark_one(
                document_id=document.id,
                stage_status=document_pipeline.StageStatus.FAILED,
                error=f"transition to chunk stage failed: {repr(exc)}",
            )
            logger.error(f"transition to chunk stage failed: {exc}")

async def chunk_stage():
    documents = await document_queue.claim(stage=document_pipeline.Stage.CHUNK)
    batch_size = len(documents) // 5
    task_results = []
    responses: list[tasks.ChunkResponse] = []
    
    for i in range(5):
        try:
            async with db.transaction():
                start = i * batch_size
                batch = documents[start:start + batch_size]
                document_ids = [d.id for d in batch]
            
                await document_pipeline.mark_many(
                    document_ids=document_ids,
                    stage_status=document_pipeline.StageStatus.IN_PROGRESS,
                )
                logger.info(f"marked {len(batch)} documents as in progress")
                task_results.append(tasks.chunk.delay([asdict(d) for d in batch]))            
        except Exception as exc:
            async with db.transaction():
                await document_pipeline.mark_many(
                    document_ids=document_ids,
                    stage_status=document_pipeline.StageStatus.FAILED,
                    error=f"chunk failed for batch {i} while dispatching tasks: {repr(exc)}",
                )
                logger.error(f"chunk failed for batch {i} while dispatching tasks: {repr(exc)}")
                continue
            
    results: list[list[dict]] = await asyncio.gather(
        *[asyncio.to_thread(task_result.get, timeout=300) for task_result in task_results],
        return_exceptions=True,
    )
    
    for r in results:
        if isinstance(r, BaseException):
            logger.error(f"chunk failed while collecting results: {repr(r)}")
            continue
        
        for item in r:            
            try:
                assert isinstance(item, dict)
                responses.append(tasks.ChunkResponse(**item))
            except Exception as exc:
                await document_pipeline.mark_one(
                    document_id=item["document_id"],
                    stage_status=document_pipeline.StageStatus.FAILED,
                    error=f"chunk failed while collecting results: {repr(exc)}",
                )
                logger.error(f"chunk failed for document {item["document_id"]} while collecting results: {repr(exc)}")
    
    for res in responses:
        try:
            async with db.transaction():                
                if res.ok:
                    if len(res.chunk_ids) == 0:
                        raise ValueError(f"no chunks for document {res.document_id}")
                    
                    await document_pipeline.mark_one(
                        document_id=res.document_id,
                        stage_status=document_pipeline.StageStatus.DONE,
                    )
                    logger.info(f"document {res.document_id} chunked")
                else:
                    await document_pipeline.mark_one(
                        document_id=res.document_id,
                        stage_status=document_pipeline.StageStatus.FAILED,
                        error=res.error
                    )
                    logger.error(f"chunk failed for {res.document_id}, error: {res.error}")
        except Exception as exc:
            # fresh txn: previous one already rolled back
            async with db.transaction():
                await document_pipeline.mark_one(
                    document_id=res.document_id,
                    stage_status=document_pipeline.StageStatus.FAILED,
                    error=f"chunk failed: {repr(exc)}",
                )
                logger.error(f"chunk failed for document {res.document_id}: {exc}")
            
async def transition_to_classification_stage():
    documents = await document_pipeline.fetch(
        stage=document_pipeline.Stage.CHUNK,
        stage_status=document_pipeline.StageStatus.DONE,
    )
    
    for document in documents:
        try:
            chunk_count = await selectors.count_document_chunks_by_document(document_id=document.id)
            if chunk_count is None or chunk_count == 0:
                await document_pipeline.mark_one(
                    document_id=document.id,
                    stage_status=document_pipeline.StageStatus.FAILED,
                    error="no chunks were created"
                )
                logger.error(f"no chunks were created for document {document.id}")
                continue
            
            await document_pipeline.transition_to_next_stage(
                document_id=document.id,
                current_stage=document_pipeline.Stage.CHUNK,
            )
            logger.info(f"document {document.id} transitioned to {document_pipeline.Stage.CLASSIFY} stage")            
        except Exception as exc:
            await document_pipeline.mark_one(
                document_id=document.id,
                stage_status=document_pipeline.StageStatus.FAILED,
                error=f"transition to classification stage failed: {repr(exc)}",
            )
            logger.error(f"transition to classification stage failed: {exc}")
            
async def classification_stage():
    documents = await document_queue.claim(stage=document_pipeline.Stage.CLASSIFY)
    batch_size = len(documents) // 5
    task_results = []
    responses: list[ClassificationResponse] = []
    
    for i in range(5):
        try:
            async with db.transaction():
                start = i * batch_size
                batch = documents[start:start + batch_size]
                document_ids = [d.id for d in batch]

                await document_pipeline.mark_many(
                    document_ids=document_ids,
                    stage_status=document_pipeline.StageStatus.IN_PROGRESS,
                )
                logger.info(f"marked {len(batch)} documents as in progress for classification")
                task_results.append(classify_task.delay([asdict(ClassificationRequest(
                    document_id=d.id,
                    content=d.content,    
                )) for d in batch]))
        except Exception as exc:
            # fresh txn: previous one already rolled back
            async with db.transaction():
                await document_pipeline.mark_many(
                    document_ids=[d.id for d in batch],
                    stage_status=document_pipeline.StageStatus.FAILED,
                    error=f"classify failed for batch {i} while dispatching tasks: {repr(exc)}",
                )
                logger.error(f"classify failed for batch {i} while dispatching tasks: {repr(exc)}")
                continue
            
    results: list[list[dict]] = await asyncio.gather(
        *[asyncio.to_thread(task_result.get, timeout=300) for task_result in task_results],
        return_exceptions=True,
    )
    
    for r in results:
        if isinstance(r, BaseException):
            logger.error(f"classify failed while collecting results: {repr(item)}")
            continue
        
        for item in r:
            if isinstance(item, BaseException):
                logger.error(f"classify failed while collecting results: {repr(item)}")
                continue
            
            try:
                responses.append(ClassificationResponse(**item))
            except Exception as exc:
                await document_pipeline.mark_one(
                    document_id=item["document_id"],
                    stage_status=document_pipeline.StageStatus.FAILED,
                    error=f"classify failed while collecting results: {repr(exc)}",
                )
                logger.error(f"classify failed for document {item["document_id"]} while collecting results: {repr(exc)}, results: {item}")
    
    for res in responses:
        try:
            async with db.transaction():
                if res.ok:
                    if res.label is None:
                        await document_pipeline.mark_one(
                            document_id=res.document_id,
                            stage_status=document_pipeline.StageStatus.FAILED,
                            error="empty label",
                        )
                        logger.error(f"classification failed for document {res.document_id}, error: empty label")
                        continue
                    
                    label = ClassificationLabel[res.label]
                    
                    await classification_services.update_document_main_classification_label(document_id=res.document_id, label=label)
                    await document_pipeline.mark_one(
                        document_id=res.document_id,
                        stage_status=document_pipeline.StageStatus.DONE,
                    )
                    logger.info(f"document {res.document_id} classified as {res.label}")
                else:
                    await document_pipeline.mark_one(
                        document_id=res.document_id,
                        stage_status=document_pipeline.StageStatus.FAILED,
                        error=f"classification failed for {res.document_id}, error: {res.error}",
                    )
                    logger.error(f"classification failed for {res.document_id}, error: {res.error}")
        except Exception as exc:
            # fresh txn: previous one already rolled back
            async with db.transaction():
                await document_pipeline.mark_one(
                    document_id=res.document_id,
                    stage_status=document_pipeline.StageStatus.FAILED,
                    error=f"classify failed: {repr(exc)}",
                )
                logger.error(f"classify failed for document {res.document_id}: {exc}")
                
async def transition_to_embedding_stage():
    documents = await document_pipeline.fetch(
        stage=document_pipeline.Stage.CLASSIFY,
        stage_status=document_pipeline.StageStatus.DONE,
    )
    
    for document in documents:
        try:
            if document.main_classification_label is None or document.main_classification_label == "":
                await document_pipeline.mark_one(
                    document_id=document.id,
                    stage_status=document_pipeline.StageStatus.FAILED,
                    error="no classification label were detected"
                )
                logger.error(f"no classification label were detected {document.id}")
                continue
            
            await document_pipeline.transition_to_next_stage(
                document_id=document.id,
                current_stage=document_pipeline.Stage.CLASSIFY,
            )
            logger.info(f"document {document.id} transitioned to {document_pipeline.Stage.EMBEDDING} stage")            
        except Exception as exc:
            await document_pipeline.mark_one(
                document_id=document.id,
                stage_status=document_pipeline.StageStatus.FAILED,
                error=f"transition to embedding stage failed: {repr(exc)}",
            )
            logger.error(f"transition to embedding stage failed: {exc}")
                
async def embedding_stage():
    documents = await document_queue.claim(stage=document_pipeline.Stage.EMBEDDING)
    responses: list[EmbedResponse] = []
    task_results = []
    
    for document in documents:
        try:
            logger.info(f"embedding document {document.id}")
            await document_pipeline.mark_one(
                document_id=document.id,
                stage_status=document_pipeline.StageStatus.IN_PROGRESS,
            )
            
            chunks = await selectors.fetch_document_chunks(document_id=document.id)
            request = asdict(EmbedRequest(
                document_id=document.id,
                chunks=[{"id": c.id, "content": c.content} for c in chunks],
            ))
            task_results.append(embed.delay(request))
        except Exception as exc:
            await document_pipeline.mark_one(
                document_id=document.id,
                stage_status=document_pipeline.StageStatus.FAILED,
                error=f"embedding failed while dispatching task: {repr(exc)}",
            )
            logger.error(f"embedding for document {document.id} failed while dispatching task: {repr(exc)}")
            continue
        
    results: list[dict] = await asyncio.gather(
        *[asyncio.to_thread(task_result.get, timeout=1800) for task_result in task_results],
        return_exceptions=True,
    )
        
    for res in results:
        if isinstance(res, BaseException):
            logger.error(f"embed failed while collecting results: {repr(res)}")
            continue
        
        if not isinstance(res, dict):
            logger.error(f"embed failed while collecting results: `res` should be a dict: {res}")
            continue
        
        try:
            responses.append(EmbedResponse(**res))
        except Exception as exc:
            await document_pipeline.mark_one(
                document_id=res["document_id"],
                stage_status=document_pipeline.StageStatus.FAILED,
                error=f"embed failed while collecting results: {repr(exc)}",
            )
            logger.error(f"embed failed for document {res["document_id"]} while collecting results: {repr(exc)}, results: {item}")
            continue
        
    for res in responses:
        try:
            async with db.transaction():
                if res.ok:
                    await document_pipeline.mark_one(
                        document_id=res.document_id,
                        stage_status=document_pipeline.StageStatus.DONE,
                    )
                    await document_pipeline.set_finished_at(
                        document_id=res.document_id,
                        finished_at=datetime.now(),
                    )
                    await document_queue.dequeue(document_id=res.document_id)
                    logger.info(f"document {res.document_id} embedded")
                else:
                    await document_pipeline.mark_one(
                        document_id=res.document_id,
                        stage_status=document_pipeline.StageStatus.FAILED,
                        error=f"embedding failed for {res.document_id}, error: {res.error}",
                    )
                    logger.error(f"embedding failed for {res.document_id}, error: {res.error}")
        except Exception as exc:
            # fresh txn: previous one already rolled back
            async with db.transaction():
                await document_pipeline.mark_one(
                    document_id=res.document_id,
                    stage_status=document_pipeline.StageStatus.FAILED,
                    error=f"embedding failed: {repr(exc)}",
                )
                logger.error(f"embedding failed for document {res.document_id}: {exc}")