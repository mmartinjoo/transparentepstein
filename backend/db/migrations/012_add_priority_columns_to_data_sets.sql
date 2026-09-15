alter table if exists ops.data_sets
add column priority int default 50 check (priority between 1 and 100);

alter table if exists ops.data_sets
add column last_scraped_at timestamptz default null;