from dataclasses import dataclass
from datetime import datetime


@dataclass
class DataSet():
    id: int
    name: str | None = None
    url: str | None = None
    main_document_type: str | None = None
    max_pages: int | None = None
    processed_until_page: int | None = None

@dataclass
class Document():
    id: int
    url: str
    content: str
    s3_key: str
    data_set_id: int
    created_at: datetime
    updated_at: datetime