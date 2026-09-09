create table if not exists ops.documents(
    id serial primary key,
    external_id text not null,
    url text not null,
    source text not null,
    data_set text default null,
    s3_key text,
    created_at timestamptz default now(),
    updated_at timestamptz default null
)