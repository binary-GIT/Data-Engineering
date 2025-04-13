import os
import boto3
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from io import StringIO

# Load environment variables
load_dotenv()

# AWS + RDS Credentials from .env
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = "youtube-analytics-3633"
S3_KEY = "processed/processed_youtube_data.csv"

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Step 1: Read processed CSV from S3
s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

response = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
csv_data = response["Body"].read().decode("utf-8")

df = pd.read_csv(StringIO(csv_data))

# Step 2: Connect to PostgreSQL RDS
conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)
cursor = conn.cursor()

# Step 3: Create Table if not exists
create_table_query = """
CREATE TABLE IF NOT EXISTS youtube_data (
    video_title TEXT,
    video_id TEXT,
    video_published_at TIMESTAMP,
    views BIGINT,
    likes BIGINT,
    video_url TEXT,
    channel_name TEXT,
    channel_id TEXT,
    channel_published_at TIMESTAMP,
    subscribers BIGINT,
    total_views BIGINT,
    total_videos BIGINT,
    country TEXT,
    channel_url TEXT
);
"""
cursor.execute(create_table_query)
conn.commit()

# Step 4: Insert data into the table
insert_query = """
INSERT INTO youtube_data (
    video_title, video_id, video_published_at, views, likes, video_url,
    channel_name, channel_id, channel_published_at,
    subscribers, total_views, total_videos, country, channel_url
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

for _, row in df.iterrows():
    cursor.execute(insert_query, (
        row['Video Title'], row['Video ID'], row['Video Published At'], row['Views'],
        row['Likes'], row['Video URL'], row['Channel Name'], row['Channel ID'],
        row['Channel Published At'], row['Subscribers'], row['Total Views'],
        row['Total Videos'], row['Country'], row['Channel URL']
    ))

conn.commit()
cursor.close()
conn.close()

print("✅ Data loaded into RDS successfully!")
