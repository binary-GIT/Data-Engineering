import boto3
import os

# Your file and bucket info
file_path = 'data_related_videos.csv'
bucket_name = 'youtube-analytics-3633'  # ✅ your actual bucket name
object_name = 'raw/data_related_videos.csv'     # Path inside bucket

# Create boto3 S3 client
s3 = boto3.client('s3')

try:
    s3.upload_file(file_path, bucket_name, object_name)
    print(f"✅ File uploaded successfully to s3://{bucket_name}/{object_name}")
except Exception as e:
    print(f"❌ Error uploading file: {e}")
