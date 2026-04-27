import os
import sys
import io
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, split, lower, regexp_replace, length, window, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

KAFKA_SERVERS = "localhost:9092"
KAFKA_TOPIC = "social-media-topic"

SCHEMA = StructType([
    StructField("channel",      StringType(), True),
    StructField("title",        StringType(), True),
    StructField("video_id",     StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("ingested_at",   StringType(), True),
])

def process_stream():
    spark = SparkSession.builder \
        .appName("YouTubeRealTimeAnalytics") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # القراءة من كافكا
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # تحويل البيانات وإضافة Timestamp للتحليل الزمني
    parsed_df = raw_df.selectExpr("CAST(value AS STRING) AS json_str") \
        .select(from_json(col("json_str"), SCHEMA).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", current_timestamp())

    # --- [ الـ 3 Aggregations ] ---
    # 1. تحليل القنوات
    channels_df = parsed_df.groupBy("channel").count().orderBy(col("count").desc())

    # 2 تحليل الكلمات
    words_df = parsed_df.select(
        explode(split(regexp_replace(lower(col("title")), r"[^\w\s]", ""), r"\s+")).alias("word")
    ).filter(length(col("word")) > 3) \
    .groupBy("word").count().orderBy(col("count").desc())


    # 3. التحليل الزمني (سرعة التدفق)
    time_analysis_df = parsed_df \
        .groupBy(window(col("event_time"), "1 minute")).count() \
        .select(col("window.start").alias("start_time"), col("count").alias("videos_received")) \
        .orderBy("start_time")

# --- [ عرض المخرجات بشكل منظم ] ---

    # 1. الجدول الأساسي (سجل الفيديوهات)
    q1 = parsed_df.select("channel", "title", "ingested_at") \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("numRows", 10) \
        .option("truncate", "true") \
        .queryName("RAW_DATA") \
        .option("title", "--- [ TABLE 1: RECENT INCOMING VIDEOS ] ---") \
        .start()

    # 2. جدول تحليل الكلمات
    q2 = words_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("numRows", 20) \
        .option("truncate", "false") \
        .queryName("WORD_COUNT") \
        .option("title", "--- [ TABLE 2: TOP TRENDING KEYWORDS ] ---") \
        .start()

    # 3. جدول تحليل القنوات
    q3 = channels_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("numRows", 15) \
        .queryName("CHANNELS") \
        .option("title", "--- [ TABLE 3: MOST ACTIVE CHANNELS ] ---") \
        .start()

    # 4. جدول التحليل الزمني
    q4 = time_analysis_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("numRows", 10) \
        .queryName("THROUGHPUT") \
        .option("title", "--- [ TABLE 4: SYSTEM THROUGHPUT (PER MINUTE) ] ---") \
        .start()

    print("🚀 Spark Pipeline is active with 3 Aggregations. Monitoring Kafka Topic...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    process_stream()