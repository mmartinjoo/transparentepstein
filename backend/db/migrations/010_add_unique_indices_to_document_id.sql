alter table ops.document_queue
add constraint document_queue_document_id_unique unique(document_id);

alter table ops.document_pipeline
add constraint document_pipeline_document_id_unique unique(document_id);