alter table if exists ops.ingestion_queue
add column document_id int default null references ops.documents(id) on delete set null;