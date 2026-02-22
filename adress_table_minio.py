from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract

# 1️⃣ Spark session עם חבילות MinIO
spark = SparkSession.builder \
    .appName("Addresses_to_MinIO") \
    .config("spark.jars.packages", 
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 2️⃣ קריאה של CSV עם כתובות
df_address = spark.read.csv("addresspoints.csv", header=True, inferSchema=True)
# df_address.select("the_geom").show(10, truncate=False)

df_latlon = df_address.select(
    col("House Number").alias("house_number"),
    col("Street Name").alias("street_name"),
    col("Borough Code").alias("borough_code"),
    regexp_extract(col("the_geom"), r'POINT\s*\(\s*([-\d\.]+)\s+([-\d\.]+)\s*\)', 1).cast("double").alias("longitude"),
    regexp_extract(col("the_geom"), r'POINT\s*\(\s*([-\d\.]+)\s+([-\d\.]+)\s*\)', 2).cast("double").alias("latitude")
)

# df_latlon.show(10)

# 4️⃣ שמירה ל-MinIO בפורמט Parquet
output_path = "s3a://spark/data/dims/address"

df_latlon.write.mode("overwrite").parquet("s3a://spark/data/dims/address")

print(f"--- Writing addresses to MinIO: {output_path} ---")

# הצגת כל השדות והדוגמה שלהם
df_address.printSchema()
df_address.show(5, truncate=False)



# # 5️⃣ הצגה לבדיקה
# df_latlon.show(100)

# spark.stop()
# print("--- DONE! Addresses are now in MinIO ---")
