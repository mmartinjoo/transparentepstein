import asyncio
from dataclasses import asdict
import logging
from pprint import pprint

from transparentepstein.classification.classifier.base import ClassificationLabel
from transparentepstein.ingestion import selectors, scraper, tasks, services
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
    data_set = await selectors.find_data_set(data_set_id=13)
    next_page = data_set.processed_until_page + 1
    if next_page >= data_set.max_pages:
        logger.info(f"data set {data_set.id} has been fully processed")
        return
    
    urls = await scraper.discover(data_set=data_set, page=next_page)
    if len(urls) == 0:
        raise NothingToDiscoverError(f"zero URLs discovered for {data_set.name} at page {next_page}")

    for url in urls:
        async with db.transaction():
            document = await services.create_document(
                url=url,
                data_set_id=data_set.id,
            )
            logger.info(f"document created {document.id}")
            
            await document_queue.enqueue(document=document)
            await document_pipeline.initialize(document)
            await update_data_set_processed_until(data_set_id=data_set.id)
    
    logger.info(f"discovered {len(urls)} URLs on page {next_page}")
    
async def fetch_stage():
    async with db.transaction():
        documents = await document_queue.claim(stage=document_pipeline.Stage.FETCH)
        logger.info(f"fetching {len(documents)} documents")

        await document_pipeline.mark_many(
            document_ids=[d.id for d in documents],
            stage_status=document_pipeline.StageStatus.IN_PROGRESS,
        )
        logger.info(f"marked {len(documents)} documents as in progress")
    
        task_results: list[dict] = []
        for document in documents:
            try:
                task_result = tasks.fetch.delay(asdict(document))
                task_results.append(task_result.get())
            except Exception as exc:
                await document_pipeline.mark_one(
                    document_id=document.id,
                    stage_status=document_pipeline.StageStatus.FAILED,
                    error=f"task failed: {repr(exc)}",
                )
                logger.error(exc)
                raise
        
    results = [tasks.FetchResponse(**r) for r in task_results]
    for res in results:
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
                    error=repr(exc),
                )
                logger.error(f"fetch failed for document {res.document_id}: {exc}")
    
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
    async with db.transaction():
        documents = await document_queue.claim(stage=document_pipeline.Stage.LOAD)
        batch_size = len(documents) // 5
        results: list[tasks.LoadResponse] = []
        
        for i in range(5):
            start = i * batch_size
            batch = documents[start:start + batch_size]
            document_ids = [d.id for d in batch]

            await document_pipeline.mark_many(
                document_ids=document_ids,
                stage_status=document_pipeline.StageStatus.IN_PROGRESS,
            )
            
            try:
                task_result = tasks.load.delay([asdict(d) for d in batch])
                load_results: list[tasks.LoadResponse] = [tasks.LoadResponse(**v) for v in task_result.get()]
                for r in load_results:
                    results.append(r)
            except Exception as exc:
                await document_pipeline.mark_many(
                    document_ids=[d.id for d in batch],
                    stage_status=document_pipeline.StageStatus.FAILED,
                    error=f"load task failed: {repr(exc)}",
                )
                logger.error(f"load task failed: {exc}")
                logger.warning(f"marked {len(batch)} documents as failed")
                
    for res in results:
        try:
            async with db.transaction():
                if res.ok:
                    if res.content is None:
                        await document_pipeline.mark_one(
                            document_id=res.document_id,
                            stage_status=document_pipeline.StageStatus.FAILED,
                            error=f"empty content"
                        )
                        logger.error(f"load failed for {res.document_id}, error: empty content")
                        continue
                    
                    await services.update_document_content(document_id=res.document_id, content=res.content)
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
    async with db.transaction():
        documents = await document_queue.claim(stage=document_pipeline.Stage.CHUNK)
        batch_size = len(documents) // 5
        results: list[tasks.ChunkResponse] = []
        batches = []
        
        for i in range(5):
            start = i * batch_size
            batch = documents[start:start + batch_size]
            document_ids = [d.id for d in batch]
            
            await document_pipeline.mark_many(
                document_ids=document_ids,
                stage_status=document_pipeline.StageStatus.IN_PROGRESS,
            )
            
            try:
                task_result = tasks.chunk.delay([asdict(doc) for doc in batch])
                load_results: list[tasks.ChunkResponse] = [tasks.ChunkResponse(**v) for v in task_result.get()]
                for r in load_results:
                    results.append(r)
            except Exception as exc:
                await document_pipeline.mark_many(
                    document_ids=[d.id for d in batch],
                    stage_status=document_pipeline.StageStatus.FAILED,
                    error=f"task failed: {repr(exc)}",
                )
                logger.error(exc)
                logger.warning(f"marked {len(batch)} documents as failed")
    
    for res in results:
        try:
            async with db.transaction():
                if res.ok:
                    if len(res.chunks) == 0:
                        await document_pipeline.mark_one(
                            document_id=res.document_id,
                            stage_status=document_pipeline.StageStatus.FAILED,
                            error=f"empty chunks"
                        )
                        logger.error(f"chunk failed for {res.document_id}, error: empty chunks")
                        continue
                    
                    await services.create_document_chunks(document_id=res.document_id, chunks=res.chunks)
                    await document_pipeline.mark_one(
                        document_id=res.document_id,
                        stage_status=document_pipeline.StageStatus.DONE,
                    )
                    logger.info(f"document {res.document_id} chunked")
                else:
                    await document_pipeline.mark_one(
                        document_id=res.document_id,
                        stage_status=document_pipeline.StageStatus.FAILED,
                        error=res.error,
                    )
                    logger.info(f"chunk failed for {res.document_id}, error: {res.error}")
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
    async with db.transaction():
        documents = await document_queue.claim(stage=document_pipeline.Stage.CLASSIFY)
        batch_size = len(documents) // 5
        results: list[ClassificationResponse] = []
        
        for i in range(5):
            start = i * batch_size
            batch = documents[start:start + batch_size]
            
            document_ids = [d.id for d in batch]
            await document_pipeline.mark_many(
                document_ids=document_ids,
                stage_status=document_pipeline.StageStatus.IN_PROGRESS,
            )
            
            requests: list[ClassificationRequest] = []
            for document in batch:
                requests.append(asdict(ClassificationRequest(
                    document_id=document.id,
                    content=document.content,
                )))
            
            try:
                task_result = classify_task.delay(requests)                    
                load_results: list[ClassificationResponse] = [ClassificationResponse(**v) for v in task_result.get()]
                for r in load_results:
                    results.append(r)
            except Exception as exc:
                await document_pipeline.mark_many(
                    document_ids=[d.id for d in batch],
                    stage_status=document_pipeline.StageStatus.FAILED,
                    error=f"classify task failed: {repr(exc)}",
                )
                logger.error(f"classify task failed: {exc}")
                logger.warning(f"marked {len(batch)} documents as failed")
    
    for res in results:
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
    async with db.transaction():
        documents = await document_queue.claim(stage=document_pipeline.Stage.EMBEDDING)
        for document in documents:
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
            
            try:
                task_result = embed.delay(request)                    
                result = EmbedResponse(**task_result.get())
            except Exception as exc:
                await document_pipeline.mark_one(
                    document_id=document.id,
                    stage_status=document_pipeline.StageStatus.FAILED,
                    error=f"embedding task failed: {repr(exc)}",
                )
                logger.error(f"embedding task failed: {exc}")
                logger.warning(f"marked document {document.id} as failed")
                continue
    
            try:
                async with db.transaction():
                    if result.ok:
                        await document_pipeline.mark_one(
                            document_id=result.document_id,
                            stage_status=document_pipeline.StageStatus.DONE,
                        )
                        await document_queue.dequeue(document_id=result.document_id)
                        logger.info(f"document {result.document_id}")
                    else:
                        await document_pipeline.mark_one(
                            document_id=result.document_id,
                            stage_status=document_pipeline.StageStatus.FAILED,
                            error=f"embedding failed for {result.document_id}, error: {result.error}",
                        )
                        logger.error(f"embedding failed for {result.document_id}, error: {result.error}")
            except Exception as exc:
                # fresh txn: previous one already rolled back
                async with db.transaction():
                    await document_pipeline.mark_one(
                        document_id=result.document_id,
                        stage_status=document_pipeline.StageStatus.FAILED,
                        error=f"embedding failed: {repr(exc)}",
                    )
                    logger.error(f"embedding failed for document {result.document_id}: {exc}")