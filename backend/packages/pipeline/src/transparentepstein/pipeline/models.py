from dataclasses import dataclass
from datetime import datetime


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
    
