import pandas as pd

df = pd.read_csv('data_related_videos.csv')
# print(df.head(10))

# print(df.columns)
# print(df.info())
# print(df.describe())

# Define columns to keep
columns_to_keep = [
    'Video Title', 'Video ID', 'Video Published At',
    'Views', 'Likes', 'Video URL',
    'Channel Name', 'Channel ID', 'Channel Published At',
    'Subscribers', 'Total Views', 'Total Videos',
    'Country', 'Channel URL'
]

# # Filter the DataFrame
# df = df[columns_to_keep]

# # Optional: Preview result
# print("✅ Cleaned columns:")
# print(df.columns)
# # print(df.head(2))

# # 🔍 Check for missing values per column
# print("🧼 Missing values per column:\n")
# print(df.isnull().sum())

# # 📊 Percentage of missing values (optional)
# print("\n📊 Percentage missing:")
# print((df.isnull().mean() * 100).round(2))

# # 🧠 Check for duplicate rows (entirely identical)
# duplicate_rows = df.duplicated()
# print(f"\n🧾 Number of duplicate rows: {duplicate_rows.sum()}")

# # Optional: View the actual duplicates (if any)
# if duplicate_rows.sum() > 0:
#     print("\n📝 Duplicate rows preview:")
#     print(df[duplicate_rows])


# pd.set_option('display.max_columns', None)
# print(df.head())
# df.head().to_csv("head_preview.csv", index=False)


# Transformations

# ✅ 1. Convert date columns to datetime
df["Video Published At"] = pd.to_datetime(df["Video Published At"], format='ISO8601')
df["Channel Published At"] = pd.to_datetime(df["Channel Published At"], format='ISO8601')

# ✅ 2. Fill missing Likes with 0
df["Likes"] = df["Likes"].fillna(0)

# ✅ 3. Map country codes to full country names
country_map = {
    "IN": "India",
    "US": "United States",
    "GB": "United Kingdom",
    "CA": "Canada",
    "DE": "Germany",
    "FR": "France",
    "JP": "Japan",
    "KR": "South Korea",
    "RU": "Russia",
    "BR": "Brazil",
    "AU": "Australia",
    "PK": "Pakistan",
    "NG": "Nigeria",
    "ID": "Indonesia",
    "MX": "Mexico",
    "IT": "Italy",
    "ES": "Spain",
    "TR": "Turkey",
    "SA": "Saudi Arabia",
    "AE": "United Arab Emirates",
    "PH": "Philippines"
    # You can add more based on your dataset
}
df["Country"] = df["Country"].map(country_map)

# ✅ 4. Drop duplicate rows (if any)
df = df.drop_duplicates()

# ✅ 5. Strip whitespaces from strings
str_columns = ["Video Title", "Channel Name", "Channel URL", "Video URL"]
for col in str_columns:
    df[col] = df[col].astype(str).str.strip()

# ✅ 6. Optional: save processed data locally before uploading to S3
df.to_csv("processed_youtube_data.csv", index=False)

print("✅ Data transformation completed successfully!")