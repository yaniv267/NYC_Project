from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# יצירת הסשן (וודא שהגדרות ה-S3/MinIO שלך מופיעות פה)
spark = SparkSession.builder \
    .appName("Bronze_Audit") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()


# קריאת כל שכבת הברונזה
df_bronze = spark.read.parquet("s3a://spark/bronze/nyc_traffic_violations/")

# 1. בדיקה כללית - מה התאריך הכי חדש בברונזה?
print("📅 Absolute Max Date in Bronze:")
df_bronze.select(F.max("issue_date")).show()

# 2. פילוח לפי חודשים (נשתמש ב-substring כי בברונזה זה לרוב string)
print("📊 Record Count by Month in Bronze:")
df_bronze.withColumn("month_extract", F.substring(F.col("issue_date"), 1, 7)) \
    .groupBy("month_extract") \
    .count() \
    .orderBy("month_extract", ascending=False) \
    .show()

# 3. בדיקה אם יש נתונים של אפריל
april_count = df_bronze.filter(F.col("issue_date").contains("2026-04")).count()
print(f"🚀 Total records found for April 2026: {april_count}")

spark.stop()