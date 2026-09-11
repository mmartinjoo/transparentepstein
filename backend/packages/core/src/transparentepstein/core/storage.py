import boto3
from transparentepstein.core.config import settings


s3 = boto3.client(
    "s3",
    endpoint_url=settings.s3_endpoint_url,
    aws_access_key_id=settings.s3_access_key,
    aws_secret_access_key=settings.s3_secret_key,
)

try:
    s3.head_bucket(Bucket=settings.s3_bucket)
except Exception:
    s3.create_bucket(Bucket=settings.s3_bucket)
    
def put_file(data_set_name: str, doc_name: str, data: bytes) -> str:
    key = f"{data_set_name}/{doc_name}"
    
    if doc_name.endswith(".pdf"):
        content_type = "application/pdf"
    elif doc_name.endswith(".mp4"):
        content_type = "video/mp4"
    elif doc_name.endswith(".mp3"):
        content_type = "audio/mpeg"
    elif doc_name.endswith(".xlsx"):
        content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    elif doc_name.endswith(".avi"):
        content_type = "video/x-msvideo"
    else:
        raise ValueError(f"unknown content type for {doc_name}")
        
    s3.put_object(
        Bucket=settings.s3_bucket,
        Key=key,
        Body=data,
        ContentType=content_type,
    )
    
    return key

def get_file(key: str) -> bytes:
    resp = s3.get_object(
        Bucket=settings.s3_bucket,
        Key=key,
    )
    
    return resp["Body"].read()

def exists(key: str) -> bool:
    try:
        s3.head_object(
            Bucket=settings.s3_bucket,
            Key=key,
        )
        return True
    except Exception:
        return False
    
def empty(key: str) -> bool:
    if not exists(key=key):
        return True
    
    resp = s3.head_object(
        Bucket=settings.s3_bucket,
        Key=key,
    )
    
    return resp["ContentLength"] == 0