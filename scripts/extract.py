import pandas as pd
from googleapiclient.discovery import build
from dotenv import load_dotenv
import os
import time

# Load environment variables from .env file
load_dotenv()

# 🔑 Fetch YouTube API key from the .env file
API_KEY = os.getenv('YOUTUBE_API_KEY')
if not API_KEY:
    raise ValueError("API Key not found in .env file")

YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'

# Initialize YouTube API
youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY)

# 🔍 New keywords for your niche
search_keywords = ['data engineering', 'data science', 'data analysis']

# Store results
video_data = []

# Function to get channel details
def get_channel_details(channel_id):
    response = youtube.channels().list(
        part='snippet,statistics,brandingSettings',
        id=channel_id
    ).execute()

    if response['items']:
        item = response['items'][0]
        snippet = item['snippet']
        stats = item['statistics']
        branding = item.get('brandingSettings', {})
        thumbnails = snippet.get('thumbnails', {})
        high_thumb = thumbnails.get('high', {}).get('url') or thumbnails.get('default', {}).get('url')

        return {
            'Channel Name': snippet.get('title'),
            'Channel ID': item['id'],
            'Channel Description': snippet.get('description'),
            'Channel Published At': snippet.get('publishedAt'),
            'Country': snippet.get('country', 'N/A'),
            'Subscribers': stats.get('subscriberCount', 'N/A'),
            'Total Views': stats.get('viewCount', 'N/A'),
            'Total Videos': stats.get('videoCount', 'N/A'),
            'Custom URL': branding.get('channel', {}).get('customUrl', 'N/A'),
            'Channel URL': f"https://www.youtube.com/channel/{item['id']}",
            'Channel Logo': high_thumb
        }

    return {}

# Function to get video + channel details
def get_video_and_channel_details(video_id):
    response = youtube.videos().list(
        part='snippet,statistics',
        id=video_id
    ).execute()

    if response['items']:
        item = response['items'][0]
        snippet = item['snippet']
        stats = item['statistics']
        channel_id = snippet['channelId']

        # Get channel info
        channel_info = get_channel_details(channel_id)

        # Combine video + channel data
        return {
            'Video Title': snippet.get('title'),
            'Video ID': item['id'],
            'Video Description': snippet.get('description'),
            'Video Published At': snippet.get('publishedAt'),
            'Views': stats.get('viewCount', 'N/A'),
            'Likes': stats.get('likeCount', 'N/A'),
            'Video URL': f"https://www.youtube.com/watch?v={item['id']}",
            **channel_info
        }

    return None

# Function to execute multiple paginated searches for videos
def search_videos():
    for keyword in search_keywords:
        print(f"🔍 Searching for videos related to: {keyword}")
        next_page_token = None
        for page in range(10):  # Run 10 pages per keyword (10 x 50 = 500 videos max)
            search_request = youtube.search().list(
                q=keyword,
                type='video',
                part='snippet',
                maxResults=50,
                pageToken=next_page_token
            )
            search_response = search_request.execute()

            # Collect video + channel details
            for item in search_response['items']:
                video_id = item['id']['videoId']
                details = get_video_and_channel_details(video_id)
                if details and details not in video_data:
                    video_data.append(details)

            next_page_token = search_response.get('nextPageToken')
            time.sleep(1)
            if not next_page_token:
                break

# Run the search
search_videos()

# Save to CSV
df = pd.DataFrame(video_data)
df.to_csv('data_related_videos.csv', index=False)
print(f"✅ Done! Saved {len(df)} videos with channel info to data_related_videos.csv")
print("📊 DataFrame Preview:")
print(df.head())
print("📁 CSV File Preview:")