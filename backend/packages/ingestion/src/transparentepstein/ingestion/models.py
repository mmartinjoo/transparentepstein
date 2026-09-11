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
    data_set_id: int    
    created_at: datetime
    updated_at: datetime | None = None
    main_classification_label: str | None = None
    s3_key: str | None = None
    content: str | None = None
    
@dataclass
class DocumentChunk():
    id: int
    document_id: int
    position: int
    content: str
    created_at: datetime
    updated_at: datetime | None = None