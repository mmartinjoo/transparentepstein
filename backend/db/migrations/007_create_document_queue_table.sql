create table if not exists ops.document_queue(
    id serial primary key,
    document_id int not null references ops.documents(id),
    attempts int not null default 0,
    next_attempt_at timestamptz not null default now()
)