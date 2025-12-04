import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct, from_json, coalesce, round, when, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

spark = SparkSession.builder \
    .appName("YouTubeAnalysis") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

kafka_bootstrap_servers = 'localhost:9092'

TOPIC_FEATURES = 'youtube-features'
TOPIC_BASE_METRICS = 'youtube-base-metrics'
TOPIC_PERSON_METRICS = 'youtube-person-metrics'

# Загрузка данных
try:
    df = spark.read.csv("./RU_youtube_trending_data.csv", header=True, inferSchema=False)
except Exception as e:
    print("Ошибка загрузки CSV файла")
    spark.stop()
    exit()

df.createOrReplaceTempView("raw_data")

dedup_df = spark.table("raw_data").dropDuplicates(["video_id", "trending_date"])
dedup_df.createOrReplaceTempView("dedup_data")

# Подготовка признаков
spark.sql("""
CREATE OR REPLACE TEMP VIEW data_with_features_dirty AS
SELECT 
    *,
    TRY_CAST(view_count AS LONG) as views_long,
    TRY_CAST(likes AS LONG) as likes_long,
    TRY_CAST(comment_count AS LONG) as comments_long
FROM dedup_data
WHERE description IS NOT NULL OR title IS NOT NULL
""")

data_with_features_df = spark.sql("""
CREATE OR REPLACE TEMP VIEW data_with_features AS
SELECT 
    video_id,
    title,
    channelTitle,
    categoryId,
    trending_date,
    SUBSTRING(trending_date, 1, 7) as ym,
    views_long as view_count,
    likes_long as likes,
    comments_long as comment_count,
    
    CASE 
        WHEN UPPER(title) LIKE '%МУЗЫКА%' OR UPPER(title) LIKE '%MUSIC%' OR 
             UPPER(description) LIKE '%МУЗЫКА%' OR UPPER(description) LIKE '%MUSIC%' THEN 'music'
        WHEN UPPER(title) LIKE '%ОБРАЗОВАНИЕ%' OR UPPER(title) LIKE '%EDUCATION%' OR 
             UPPER(description) LIKE '%ОБРАЗОВАНИЕ%' OR UPPER(description) LIKE '%EDUCATION%' THEN 'education'
        WHEN UPPER(title) LIKE '%ЮМОР%' OR UPPER(title) LIKE '%COMEDY%' OR 
             UPPER(description) LIKE '%ЮМОР%' OR UPPER(description) LIKE '%COMEDY%' THEN 'comedy'
        WHEN UPPER(title) LIKE '%ПУТЕШЕСТВИЯ%' OR UPPER(title) LIKE '%TRAVEL%' OR 
             UPPER(description) LIKE '%ПУТЕШЕСТВИЯ%' OR UPPER(description) LIKE '%TRAVEL%' THEN 'travel'
        WHEN UPPER(title) LIKE '%РАЗВЛЕЧЕНИЕ%' OR UPPER(title) LIKE '%ENTERTAINMENT%' OR 
             UPPER(description) LIKE '%РАЗВЛЕЧЕНИЕ%' OR UPPER(description) LIKE '%ENTERTAINMENT%' THEN 'entertainment'
        ELSE 'other'
    END as content_category,
    
    CASE 
        WHEN UPPER(title) LIKE '%СРОЧНО%' OR UPPER(title) LIKE '%URGENT%' OR
             UPPER(title) LIKE '%ПОСЛЕДНИЙ ДЕНЬ%' OR UPPER(title) LIKE '%LAST DAY%' OR
             UPPER(title) LIKE '%ОСТАЛОСЬ 7%' OR UPPER(title) LIKE '%7 DAYS%' OR
             UPPER(title) LIKE '%24 ЧАСА%' OR UPPER(title) LIKE '%24 HOURS%' THEN 1
        ELSE 0
    END as urgency_indicator,
    
    CASE 
        WHEN UPPER(title) LIKE '%IPHONE%' OR UPPER(title) LIKE '%TESLA%' OR 
             UPPER(title) LIKE '%LOUIS VUITTON%' OR UPPER(title) LIKE '%GUCCI%' OR
             UPPER(title) LIKE '%NIKE%' OR UPPER(title) LIKE '%ADIDAS%' THEN 1
        ELSE 0
    END as trending_brands,
    
    (likes_long * 100.0 / views_long) as vlr_ratio
    
FROM data_with_features_dirty
WHERE views_long IS NOT NULL 
  AND likes_long IS NOT NULL 
  AND views_long > 0
  AND likes_long >= 0
""")

# Отправка в топик features
data_with_features_df = spark.table("data_with_features")
data_with_features_df.select(
    to_json(struct([col(c) for c in data_with_features_df.columns])).alias("value")
).write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("topic", TOPIC_FEATURES) \
    .save()

# Базовая агрегация
base_monthly_content_df = spark.sql("""
CREATE OR REPLACE TEMP VIEW base_monthly_content AS
SELECT 
    ym,
    content_category,
    COUNT(*) as base_cnt,
    SUM(likes) as base_like,
    SUM(view_count) as base_view,
    AVG(vlr_ratio) as base_lvr
FROM data_with_features
GROUP BY ym, content_category
""")

base_monthly_content_df = spark.table("base_monthly_content")
base_monthly_content_df.select(
    to_json(struct([col(c) for c in base_monthly_content_df.columns])).alias("value")
).write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("topic", TOPIC_BASE_METRICS) \
    .save()

# Агрегация по бинарным признакам
content_with_binary_features_df = spark.sql("""
CREATE OR REPLACE TEMP VIEW content_with_binary_features AS
SELECT 
    ym,
    content_category,
    COUNT(*) as matching_cnt,
    SUM(likes) as person_like,
    SUM(view_count) as person_view,
    AVG(vlr_ratio) as person_lvr
FROM data_with_features
WHERE urgency_indicator = 1 OR trending_brands = 1
GROUP BY ym, content_category
""")

content_with_binary_features_df = spark.table("content_with_binary_features")
content_with_binary_features_df.select(
    to_json(struct([col(c) for c in content_with_binary_features_df.columns])).alias("value")
).write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("topic", TOPIC_PERSON_METRICS) \
    .save()

time.sleep(5)

# Чтение из Kafka и финальный расчет
schema_base = StructType([
    StructField("ym", StringType(), True),
    StructField("content_category", StringType(), True),
    StructField("base_cnt", LongType(), True),
    StructField("base_like", LongType(), True),
    StructField("base_view", LongType(), True),
    StructField("base_lvr", DoubleType(), True)
])

schema_person = StructType([
    StructField("ym", StringType(), True),
    StructField("content_category", StringType(), True),
    StructField("matching_cnt", LongType(), True),
    StructField("person_like", LongType(), True),
    StructField("person_view", LongType(), True),
    StructField("person_lvr", DoubleType(), True)
])

raw_base_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", TOPIC_BASE_METRICS) \
    .option("startingOffsets", "earliest") \
    .load()

base_df = raw_base_df \
    .select(col("value").cast("string").alias("json_string")) \
    .select(from_json(col("json_string"), schema_base).alias("data")) \
    .select("data.*").dropDuplicates(["ym", "content_category"])

raw_person_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", TOPIC_PERSON_METRICS) \
    .option("startingOffsets", "earliest") \
    .load()

person_df = raw_person_df \
    .select(col("value").cast("string").alias("json_string")) \
    .select(from_json(col("json_string"), schema_person).alias("data")) \
    .select("data.*").dropDuplicates(["ym", "content_category"])

bp_analysis_df = base_df.alias("b").join(
    person_df.alias("c"),
    (col("b.ym") == col("c.ym")) & (col("b.content_category") == col("c.content_category")),
    "left"
).select(
    col("b.ym"),
    col("b.content_category"),
    col("b.base_like"),
    col("b.base_view"),
    col("b.base_lvr"),
    col("b.base_cnt"),
    coalesce(col("c.matching_cnt"), lit(0)).alias("matching_cnt"),
    coalesce(col("c.person_like"), lit(0)).alias("person_like"),
    coalesce(col("c.person_view"), lit(0)).alias("person_view"),
    coalesce(col("c.person_lvr"), lit(0.0)).alias("person_lvr")
).where(
    col("base_view") > 0
)

kp_analysis_df = bp_analysis_df.withColumn(
    "diff_abs",
    round(col("person_lvr") - col("base_lvr"), 2)
).withColumn(
    "diff_relative",
    when(col("base_lvr") > 0, round(col("person_lvr") / col("base_lvr"), 2)).otherwise(lit(0.0))
).where(
    col("matching_cnt") > 0
).select(
    col("ym").cast(StringType()).alias("ym"),
    col("content_category").cast(StringType()).alias("content_category"),
    col("base_like").cast(LongType()).alias("base_like"),
    col("base_view").cast(LongType()).alias("base_view"),
    round(col("base_lvr").cast(DoubleType()), 2).alias("base_lvr"),
    col("base_cnt").cast(LongType()).alias("base_cnt"),
    col("matching_cnt").cast(LongType()).alias("matching_cnt"),
    col("person_like").cast(LongType()).alias("person_like"),
    col("person_view").cast(LongType()).alias("person_view"),
    round(col("person_lvr").cast(DoubleType()), 2).alias("person_lvr"),
    col("diff_abs").cast(DoubleType()).alias("diff_abs"),
    col("diff_relative").cast(DoubleType()).alias("diff_relative")
).orderBy(col("ym"), col("diff_relative").desc())

kp_analysis_df.show(5, truncate=False)
kp_analysis_df.printSchema()

spark.stop()