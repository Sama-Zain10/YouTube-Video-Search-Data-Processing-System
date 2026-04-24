from googleapiclient.discovery import build
from dotenv import load_dotenv
import os

# dotenv :ده اللى هيخلينا نعرف نقرأ من ال .env file
# Load environment variables
load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")
SEARCH_KEYWORD = os.getenv("SEARCH_KEYWORD")
MAX_RESULTS = int(os.getenv("MAX_RESULTS", 10))

# Connect to YouTube API
youtube = build('youtube', 'v3', developerKey=API_KEY)

# Search videos based on keyword
search_request = youtube.search().list(
    part="snippet",
    q=SEARCH_KEYWORD,
    maxResults=MAX_RESULTS,
    type="video"
)

search_response = search_request.execute()

print("Videos Found:\n")

for item in search_response['items']:
    video_title = item['snippet']['title']
    video_id = item['id']['videoId']

    print(f"{video_title}")

    # Get comments for each video
    comment_request = youtube.commentThreads().list(
        part="snippet",
        videoId=video_id,
        maxResults=5,
        textFormat="plainText"
    )

    try:
        comment_response = comment_request.execute()

        for comment_item in comment_response['items']:
            comment = comment_item['snippet']['topLevelComment']['snippet']['textDisplay']

            # 🎯 FILTERING LOGIC
            if "AI" in comment.lower() or "data" in comment.lower():
                print("FILTERED COMMENT:", comment)

    except Exception as e:
        print("No comments or error:", e)