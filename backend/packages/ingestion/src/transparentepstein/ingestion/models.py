from dataclasses import dataclass


@dataclass
class DataSet():
    id: int
    name: str
    url: str
    main_document_type: str
    max_pages: int
    processed_until_page: int