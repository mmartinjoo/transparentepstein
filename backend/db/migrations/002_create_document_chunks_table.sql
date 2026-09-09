create table if not exists ops.document_chunks(
    id serial primary key,
    document_id integer not null references ops.documents(id) on delete cascade,
    position integer not null,
    content text not null,
    created_at timestamptz default now(),
    updated_at timestamptz default null
)