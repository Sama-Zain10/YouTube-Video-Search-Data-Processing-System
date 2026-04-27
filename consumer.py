# spark_processor.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, split, lower, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType

# 1. إعداد الجلسة (Spark Session)
spark = SparkSession.builder \
    .appName("YouTubeRealTimeAnalysis") \
    .getOrCreate()

# تقليل الزحمة في الـ Logs عشان تشوفي الجداول بوضوح
spark.sparkContext.setLogLevel("WARN")

# 2. تحديد الـ Schema (نفس الحقول اللي جاية من الـ Producer)
schema = StructType([
    StructField("channel", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("publishedAt", StringType(), True)
])

# 3. القراءة من Kafka
# استخدمنا "earliest" عشان يسحب كل البيانات اللي بعتيها قبل كدة
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "social-media-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. تحويل البيانات من JSON وتجهيزها
# بنحول القيمة من Binary لـ String وبعدين نفك الـ JSON
base_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 5. تطبيق الـ Filtering (المطلوب رقم 1 في المشروع)
# هنا بنفلتر عشان نركز على المحتوى التعليمي أو التقني
filtered_posts = base_df.filter(
    col("title").contains("Data") | 
    col("title").contains("Python") | 
    col("title").contains("Science")
)

# 6. معالجة البيانات (Transformation & Word Count)
# أ) تنظيف العناوين: تحويل لحروف صغيرة وإزالة علامات الترقيم (Pro-Tip)
posts_cleaned = filtered_posts.select(
    regexp_replace(lower(col("title")), r'[^\w\s]', "").alias("clean_title"),
    col("channel"),
    col("publishedAt")
)

# ب) تقسيم العنوان لكلمات وعمل الـ Count
words = posts_cleaned.select(explode(split(col("clean_title"), " ")).alias("word")) \
    .filter("length(word) > 3") # تجاهل الكلمات الصغيرة جداً

word_counts = words.groupBy("word").count().orderBy(col("count").desc())

# 7. المخرجات (Console) -Formatted for easy reading
# أ) عرض المنشورات بعد الفلترة والتنظيف
query1 = filtered_posts.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# ب) عرض إحصائيات الكلمات (النتائج التراكمية)
query2 = word_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# تشغيل الـ Streams معاً
spark.streams.awaitAnyTermination()