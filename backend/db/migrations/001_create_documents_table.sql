create table if not exists ops.documents(
    id serial primary key,
    url text not null,
    content text default null,
    data_set_id int not null references ops.data_sets(id),
    s3_key text not null,
    created_at timestamptz default now(),
    updated_at timestamptz default null
)