from dataclasses import dataclass


@dataclass
class DataSet():
    id: int
    name: str | None = None
    url: str | None = None
    main_document_type: str | None = None
    max_pages: int | None = None
    processed_until_page: int | None = None
