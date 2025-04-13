import boto3
import pandas as pd
from io import StringIO
import os
from dotenv import load_dotenv

# ------------ Load .env ------------
load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
# -----------------------------------

# ------------ File Info ------------
s3_folder = "processed/"
file_name = "processed_youtube_data.csv"
local_csv_path = "processed_youtube_data.csv"
# -----------------------------------

def upload_csv_to_s3(df: pd.DataFrame, bucket: str, key: str):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )

    s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    print(f"✅ Uploaded to s3://{bucket}/{key}")

# ------------ Main ------------
if __name__ == "__main__":
    # Load DataFrame (or import from transformation.py if still in memory)
    df = pd.read_csv(local_csv_path)

    s3_key = s3_folder + file_name
    upload_csv_to_s3(df, S3_BUCKET, s3_key)
