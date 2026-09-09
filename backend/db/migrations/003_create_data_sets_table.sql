create table if not exists ops.data_sets(
    id serial primary key,
    name text not null unique,
    url text not null,
    max_pages int not null,
    main_document_type text default null,
    processed_until_page int not null default 0
)