SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'public';


SELECT * FROM youtube_data;

-- Total videos in the table
SELECT COUNT(*) AS total_videos FROM youtube_data;

-- Most viewed videos
SELECT video_title, views FROM youtube_data 
ORDER BY views DESC LIMIT 5;

-- Average views per channel
SELECT 
    channel_name, 
    ROUND(AVG(views)::numeric, 1) AS avg_views
FROM 
    youtube_data 
GROUP BY 
    channel_name 
ORDER BY 
    avg_views DESC;

-- Videos with >1M views
SELECT video_title, views, likes 
FROM youtube_data 
WHERE views > 1000000;


-- Top performing videos by engagement rate
SELECT 
    video_title,
    channel_name,
    views,
    likes,
    ROUND((likes::numeric/NULLIF(views, 0))*100, 2) AS engagement_rate_percentage
FROM 
    youtube_data
WHERE 
    views > 0  -- Exclude videos with 0 views
ORDER BY 
    engagement_rate_percentage DESC
LIMIT 10;


-- Monthly Video Upload Trend
SELECT 
    channel_name,
    DATE_TRUNC('month', video_published_at) AS month,
    COUNT(*) AS videos_uploaded
FROM 
    youtube_data
GROUP BY 
    channel_name, DATE_TRUNC('month', video_published_at)
ORDER BY 
    channel_name, month;


-- Comparison of Video Performance to Channel Average
SELECT 
    video_title,
    channel_name,
    views,
    AVG(views) OVER (PARTITION BY channel_name) AS channel_avg_views,
    views - AVG(views) OVER (PARTITION BY channel_name) AS difference_from_avg
FROM 
    youtube_data
ORDER BY 
    difference_from_avg DESC;


-- Channel Ranking by Subscriber Growth Rate
SELECT 
    channel_name,
    subscribers,
    total_views,
    ROUND(total_views::numeric/subscribers, 2) AS views_per_subscriber,
    RANK() OVER (ORDER BY total_views::numeric/subscribers DESC) AS growth_rank
FROM 
    (SELECT DISTINCT channel_name, subscribers, total_views FROM youtube_data) t
ORDER BY 
    growth_rank;
