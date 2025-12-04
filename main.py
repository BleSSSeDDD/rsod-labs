from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("YouTubeLVRanalysis_SQL_Pipeline") \
    .getOrCreate()

print("ЗАГРУЗКА И ДЕДУПЛИКАЦИЯ")

df = spark.read.csv("./RU_youtube_trending_data.csv", header=True, inferSchema=False)
df.createOrReplaceTempView("raw_data")

dedup_df = spark.table("raw_data").dropDuplicates(["video_id", "trending_date"])
dedup_df.createOrReplaceTempView("dedup_data")

spark.sql("SELECT COUNT(*) as dedup_count FROM dedup_data").show()

print("СОЗДАНИЕ ПРИЗНАКОВ")

spark.sql("""
CREATE OR REPLACE TEMP VIEW data_with_features_dirty AS
SELECT 
    *,
    TRY_CAST(view_count AS LONG) as views_long,
    TRY_CAST(likes AS LONG) as likes_long,
    TRY_CAST(comment_count AS LONG) as comments_long
FROM dedup_data
WHERE description IS NOT NULL OR title IS NOT NULL -- Используются в CASE
""")

spark.sql("""
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

print("Создано представление 'data_with_features'")
spark.sql("SELECT COUNT(*) as features_count FROM data_with_features").show()

print("СОЗДАНИЕ ВСПОМОГАТЕЛЬНЫХ ПРЕДСТАВЛЕНИЙ")

spark.sql("""
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

spark.sql("""
CREATE OR REPLACE TEMP VIEW content_with_binary_features AS
SELECT 
    ym,
    content_category,
    COUNT(*) as matching_cnt,
    SUM(likes) as person_like, -- Переименовано согласно примеру
    SUM(view_count) as person_view, -- Переименовано
    AVG(vlr_ratio) as person_lvr -- Переименовано
FROM data_with_features
WHERE urgency_indicator = 1 OR trending_brands = 1
GROUP BY ym, content_category
""")

print("ВИТРИНА БП")

spark.sql("""
CREATE OR REPLACE TEMP VIEW bp_analysis AS
SELECT 
    b.ym,
    b.content_category, -- Оставляем для след. шага
    b.base_like,
    b.base_view,
    b.base_lvr,
    b.base_cnt,
    COALESCE(c.matching_cnt, 0) AS matching_cnt,
    COALESCE(c.person_like, 0) AS person_like,
    COALESCE(c.person_view, 0) AS person_view,
    COALESCE(c.person_lvr, 0.0) AS person_lvr,
    (COALESCE(c.person_lvr, 0.0) - b.base_lvr) AS diff_abs,
    (CASE WHEN b.base_lvr > 0 THEN COALESCE(c.person_lvr, 0.0) / b.base_lvr ELSE 0.0 END) AS diff_relative
FROM base_monthly_content b
LEFT JOIN content_with_binary_features c ON b.ym = c.ym AND b.content_category = c.content_category
WHERE b.base_view > 0 AND b.base_like >= 0
""")

print("РЕЗУЛЬТАТЫ БП")
bp_result = spark.sql("""
SELECT 
    CAST(ym AS STRING) as ym,
    -- В БП категория не так важна, но оставим для проверки
    CAST(base_like AS BIGINT) as base_like,
    CAST(base_view AS BIGINT) as base_view,
    CAST(base_lvr AS DOUBLE) as base_lvr,
    CAST(base_cnt AS BIGINT) as base_cnt,
    CAST(matching_cnt AS BIGINT) as matching_cnt,
    CAST(person_like AS BIGINT) as person_like,
    CAST(person_view AS BIGINT) as person_view,
    CAST(person_lvr AS DOUBLE) as person_lvr,
    CAST(diff_abs AS DOUBLE) as diff_abs,
    CAST(diff_relative AS DOUBLE) as diff_relative
FROM bp_analysis 
ORDER BY ym, base_like DESC 
LIMIT 15
""")
bp_result.show(5)

print("ПЛАН ВЫПОЛНЕНИЯ БП:")
spark.sql("SELECT * FROM bp_analysis LIMIT 1").explain()

print("ВИТРИНА КП")

spark.sql("""
CREATE OR REPLACE TEMP VIEW kp_analysis AS
SELECT *
FROM bp_analysis
WHERE matching_cnt > 0
""")

print("РЕЗУЛЬТАТЫ КП")
kp_result = spark.sql("""
SELECT 
    CAST(ym AS STRING) as ym,
    CAST(content_category AS STRING) as content_category,
    CAST(base_like AS BIGINT) as base_like,
    CAST(base_view AS BIGINT) as base_view,
    CAST(base_lvr AS DOUBLE) as base_lvr,
    CAST(base_cnt AS BIGINT) as base_cnt,
    CAST(matching_cnt AS BIGINT) as matching_cnt,
    CAST(person_like AS BIGINT) as person_like,
    CAST(person_view AS BIGINT) as person_view,
    CAST(person_lvr AS DOUBLE) as person_lvr,
    CAST(diff_abs AS DOUBLE) as diff_abs,
    CAST(diff_relative AS DOUBLE) as diff_relative
FROM kp_analysis 
ORDER BY ym, content_category 
LIMIT 15
""")
kp_result.show(5)

print("ПЛАН ВЫПОЛНЕНИЯ КП:")
spark.sql("SELECT * FROM kp_analysis LIMIT 1").explain()

print("ПРОВЕРКА КАЧЕСТВА ДАННЫХ")

print("Статистика по витринам:")
spark.sql("""
SELECT 
    'БП' as vitrina, 
    COUNT(*) as record_count,
    AVG(base_lvr) as avg_base_lvr,
    AVG(person_lvr) as avg_person_lvr,
    AVG(diff_relative) as avg_impact
FROM bp_analysis
UNION ALL
SELECT 
    'КП' as vitrina, 
    COUNT(*) as record_count,
    AVG(base_lvr) as avg_base_lvr, 
    AVG(person_lvr) as avg_person_lvr,
    AVG(diff_relative) as avg_impact
FROM kp_analysis
""").show()

print("Распределение по категориям контента в КП:")
spark.sql("""
SELECT 
    content_category,
    COUNT(*) as record_count,
    ROUND(AVG(base_lvr), 2) as avg_base_lvr,
    ROUND(AVG(person_lvr), 2) as avg_person_lvr,
    ROUND(AVG(diff_relative), 2) as avg_impact
FROM kp_analysis 
GROUP BY content_category
ORDER BY avg_impact DESC
""").show()

print("Проверка на NULL значения:")
spark.sql("""
SELECT 
    'БП' as vitrina,
    COUNT(*) as total,
    SUM(CASE WHEN base_like IS NULL THEN 1 ELSE 0 END) as null_base_like,
    SUM(CASE WHEN person_like IS NULL THEN 1 ELSE 0 END) as null_person_like,
    SUM(CASE WHEN base_lvr IS NULL THEN 1 ELSE 0 END) as null_base_lvr
FROM bp_analysis
UNION ALL
SELECT 
    'КП' as vitrina,
    COUNT(*) as total,
    SUM(CASE WHEN base_like IS NULL THEN 1 ELSE 0 END) as null_base_like,
    SUM(CASE WHEN person_like IS NULL THEN 1 ELSE 0 END) as null_person_like,
    SUM(CASE WHEN base_lvr IS NULL THEN 1 ELSE 0 END) as null_base_lvr
FROM kp_analysis
""").show()

print("ФИНАЛЬНАЯ ВИТРИНА")

final_kp_table = spark.sql("""
SELECT 
    CAST(ym AS STRING) as ym,
    CAST(content_category AS STRING) as content_category,
    CAST(base_like AS BIGINT) as base_like,
    CAST(base_view AS BIGINT) as base_view,
    ROUND(CAST(base_lvr AS DOUBLE), 2) as base_lvr,
    CAST(base_cnt AS BIGINT) as base_cnt,
    CAST(matching_cnt AS BIGINT) as matching_cnt,
    CAST(person_like AS BIGINT) as person_like,
    CAST(person_view AS BIGINT) as person_view,
    ROUND(CAST(person_lvr AS DOUBLE), 2) as person_lvr,
    ROUND(CAST(diff_abs AS DOUBLE), 2) as diff_abs,
    ROUND(CAST(diff_relative AS DOUBLE), 2) as diff_relative
FROM kp_analysis 
ORDER BY ym, diff_relative DESC
""")

final_kp_table.show(50, truncate=False)

spark.stop()
