from enum import Enum
from abc import ABC

class ClassificationLabel(Enum):
    EMAIL = "email"
    COURT = "court"
    FBI_PHOTO = "fbi_photo"
    EPSTEIN_PHOTO = "epstein_photo"
    FINANCIAL = "financial"
    UNKNOWN = "unknown"
    
class ClassifierType(Enum):
    REGEX = "regex"
    
class Classifier(ABC):
    def classify(content: str) -> ClassificationLabel: ...