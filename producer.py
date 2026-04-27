import os
import time
import json
import random
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from googleapiclient.discovery import build
from kafka import KafkaProducer

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger(__name__)

# الإعدادات
API_KEY = os.getenv("YOUTUBE_API_KEY")
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "social-media-topic")
# قائمة كلمات بحث متنوعة عشان الـ Spark يفضل يجيله بيانات مختلفة
KEYWORDS = ["data science", "big data", "machine learning", "apache spark", "kafka tutorial", "python for data"]

def get_youtube_client():
    return build("youtube", "v3", developerKey=API_KEY)

def fetch_and_send():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVERS],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    youtube = get_youtube_client()

    while True:
        search_term = random.choice(KEYWORDS)
        log.info(f"▶ Searching YouTube for: '{search_term}'")
        
        try:
            request = youtube.search().list(
                part="snippet", q=search_term, maxResults=15, type="video", order="date"
            )
            response = request.execute()

            for item in response.get("items", []):
                data = {
                    "channel": item["snippet"]["channelTitle"],
                    "title": item["snippet"]["title"],
                    "video_id": item["id"]["videoId"],
                    "published_at": item["snippet"]["publishedAt"],
                    "ingested_at": datetime.now(timezone.utc).isoformat()
                }
                producer.send(TOPIC, data)
            
            producer.flush()
            log.info(f"✔ Sent batch for '{search_term}' to Kafka.")
            
        except Exception as e:
            log.error(f"Error: {e}")
            if "quotaExceeded" in str(e):
                log.warning("Quota finished! Sleeping for longer...")
                time.sleep(3600) # ريح ساعة لو الكوتا خلصت

        log.info(f"⏱ Waiting for next cycle...")
        time.sleep(int(os.getenv("POLL_INTERVAL_SECONDS", 60)))

if __name__ == "__main__":
    fetch_and_send()