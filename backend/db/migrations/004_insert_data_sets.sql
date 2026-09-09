insert into ops.data_sets(name, url, max_pages, main_document_type, processed_until_page)
values ('Data set 1', 'https://www.justice.gov/epstein/doj-disclosures/data-set-1-files', 62, 'fbi_photo', 0)
on conflict (name)
do nothing;

insert into ops.data_sets(name, url, max_pages, main_document_type, processed_until_page)
values ('Data set 2', 'https://www.justice.gov/epstein/doj-disclosures/data-set-2-files', 11, 'epstein_photo', 0)
on conflict (name)
do nothing;

insert into ops.data_sets(name, url, max_pages, main_document_type, processed_until_page)
values ('Data set 3', 'https://www.justice.gov/epstein/doj-disclosures/data-set-3-files', 11, 'epstein_photo', 0)
on conflict (name)
do nothing;

insert into ops.data_sets(name, url, max_pages, processed_until_page)
values ('Data set 4', 'https://www.justice.gov/epstein/doj-disclosures/data-set-4-files', 3, 0)
on conflict (name)
do nothing;

insert into ops.data_sets(name, url, max_pages, main_document_type, processed_until_page)
values ('Data set 5', 'https://www.justice.gov/epstein/doj-disclosures/data-set-5-files', 2, 'fbi_photo', 0)
on conflict (name)
do nothing;

insert into ops.data_sets(name, url, max_pages, processed_until_page)
values ('Data set 6', 'https://www.justice.gov/epstein/doj-disclosures/data-set-6-files', 1, 0)
on conflict (name)
do nothing;

insert into ops.data_sets(name, url, max_pages, main_document_type, processed_until_page)
values ('Data set 7', 'https://www.justice.gov/epstein/doj-disclosures/data-set-7-files', 1, 'court', 0)
on conflict (name)
do nothing;

insert into ops.data_sets(name, url, max_pages, processed_until_page)
values ('Data set 8', 'https://www.justice.gov/epstein/doj-disclosures/data-set-8-files', 220, 0)
on conflict (name)
do nothing;

insert into ops.data_sets(name, url, max_pages, processed_until_page)
values ('Data set 9', 'https://www.justice.gov/epstein/doj-disclosures/data-set-9-files', 10674, 0)
on conflict (name)
do nothing;

insert into ops.data_sets(name, url, max_pages, processed_until_page)
values ('Data set 10', 'https://www.justice.gov/epstein/doj-disclosures/data-set-10-files', 10081, 0)
on conflict (name)
do nothing;

insert into ops.data_sets(name, url, max_pages, processed_until_page)
values ('Data set 11', 'https://www.justice.gov/epstein/doj-disclosures/data-set-11-files', 6633, 0)
on conflict (name)
do nothing;

insert into ops.data_sets(name, url, max_pages, processed_until_page)
values ('Data set 12', 'https://www.justice.gov/epstein/doj-disclosures/data-set-12-files', 3, 0)
on conflict (name)
do nothing;