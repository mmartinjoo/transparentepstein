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
class IngestionPipeline():
    id: int
    url: str
    data_set_id: int
    fetched: bool
    chunked: bool
    classified: bool
    embedded: bool
    status: str
    error: str
    attempts: int
    next_attempt_at: datetime
    created_at: datetime
    updated_at: datetime
    finished_at: datetime