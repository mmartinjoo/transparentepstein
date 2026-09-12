create type pipeline_stage
as enum ('FETCH', 'LOAD', 'CHUNK', 'CLASSIFY', 'EMBEDDING');

create type pipeline_stage_status
as enum ('PENDING', 'IN_PROGRESS', 'DONE', 'FAILED');

create table if not exists ops.document_pipeline(
    id serial primary key,
    document_id int not null references ops.documents(id) on delete cascade,
    stage pipeline_stage not null default 'FETCH',
    stage_status pipeline_stage_status not null default 'PENDING',
    error text default null,
    created_at timestamptz not null default now(),
    updated_at timestamptz default null,
    finished_at timestamptz default null
)