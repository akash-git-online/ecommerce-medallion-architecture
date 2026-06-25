import boto3
import os
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
today = datetime.now()

# MinIO configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_DOCKER")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")
DATA_DIRECTORY = Path(__file__).parent.parent / "data"


# Create S3 client for MinIO
s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name='us-east-1'
)

# Create bucket if it doesn't exist
try:
    s3_client.head_bucket(Bucket=BUCKET_NAME)
    print(f"Bucket '{BUCKET_NAME}' already exists")
except Exception as e:
    print(f"Creating bucket '{BUCKET_NAME}'...")
    s3_client.create_bucket(Bucket=BUCKET_NAME)
    print(f"Bucket '{BUCKET_NAME}' created successfully")

# Upload all CSV files to MinIO
for file in DATA_DIRECTORY.glob("*.csv"):
    file_name = f"{file.stem}/{today.year}/{today.month:02d}/{today.day:02d}/{file.name}"
    try:
        print(f"Uploading {file_name}...")
        s3_client.upload_file(str(file), BUCKET_NAME, file_name)
        print(f"✓ {file_name} uploaded successfully")
    except Exception as e:
        print(f"✗ Error uploading {file_name}: {str(e)}")

print("\nUpload process completed!")
        

