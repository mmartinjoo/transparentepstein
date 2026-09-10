create table if not exists ops.ingestion_queue(
    id serial primary key,
    data_set_id int not null references ops.data_sets(id),
    url text not null unique,
    fetched boolean not null default false,
    chunked boolean not null default false,
    classified boolean not null default false,
    embedded boolean not null default false,
    status text not null,
    error text default null,
    attempts int default 0,
    next_attempt_at timestamptz not null default now(),
    created_at timestamptz not null default now(),
    updated_at timestamptz default null,
    finished_at timestamptz default null
)