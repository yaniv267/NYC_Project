from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim, concat_ws
from nyc_schema import silver_address_schema

spark = SparkSession.builder \
    .appName("silver_nyc_addresses") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

input_path = "s3a://spark/bronze/addresses/"
output_path = "s3a://spark/silver/addresses/"

print(f"🧹 Refining Bronze data to Silver...")

# 1. קריאת כל ה-JSONs מהברונז בבת אחת
df_raw = spark.read.json(input_path)

# 2. עיבוד והתאמה לסכימה
df_silver = df_raw.select(
    col("addresspointid").cast("string").alias("address_id"),
    upper(trim(col("full_street_name"))).alias("street_name_clean"),
    upper(trim(concat_ws(" ", col("house_number"), col("full_street_name")))).alias("full_address"),
    col("zipcode").cast("string").alias("zip_code"),
    # חילוץ קואורדינטות מהמבנה הגיאומטרי
    col("the_geom.coordinates")[0].cast("double").alias("longitude"),
    col("the_geom.coordinates")[1].cast("double").alias("latitude")
)

# 3. שמירה כ-Parquet (מהיר פי 10 ל-Join ב-Gold)
df_silver.write.mode("overwrite").parquet(output_path)

print(f"✅ Silver Layer Ready at {output_path}")