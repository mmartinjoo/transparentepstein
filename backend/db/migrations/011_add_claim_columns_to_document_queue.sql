alter table if exists ops.document_queue
add column claimed_at timestamptz default null;

alter table if exists ops.document_queue
add column claimed_until timestamptz default null;