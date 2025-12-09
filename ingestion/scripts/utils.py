import json
from minio import Minio

client = Minio(
    "minio:9000",
    access_key="admin",
    secret_key="admin123",
    secure=False
)

def save_to_minio(bucket, filename, content):
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    data = json.dumps(content).encode("utf-8")

    client.put_object(
        bucket,
        filename,
        data=data,
        length=len(data)
    )
