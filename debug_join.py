from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, split
)

# 1. הקמת Spark Session עם חיבור ל-MinIO

spark = SparkSession.builder \
    .appName("Debug_NYC_Join") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

df_kafka_raw = spark.read.parquet("s3a://spark/nyc_parking_raw.parquet")
df_addresses_raw = spark.read.parquet("s3a://spark/data/dims/address")


# 3. Remove Duplicates
# Kafka: unique by summons_number
df_kafka_unique = df_kafka_raw.dropDuplicates(["summons_number"])

# Addresses: unique by house_number and street_name
df_addresses_unique = df_addresses_raw.dropDuplicates(["house_number", "street_name", "borough_code"])

# 4. Display Results with English Headers
print("\n" + "="*50)
print("DATA SUMMARY")
print("="*50)
print(f"KAFKA: Original Count = {df_kafka_raw.count()}")
print(f"KAFKA: Unique Count   = {df_kafka_unique.count()}")
print(f"ADDRESSES: Original Count = {df_addresses_raw.count()}")
print(f"ADDRESSES: Unique Count   = {df_addresses_unique.count()}")

print("\n" + "="*50)
print("RAW KAFKA SAMPLE (UNIQUE)")
print("="*50)
df_kafka_unique.select("summons_number", "house_number", "street_name").show(100, truncate=False)

print("\n" + "="*50)
print("RAW ADDRESSES SAMPLE (UNIQUE)")
print("="*50)
df_addresses_unique.select("house_number", "street_name", "longitude", "latitude").show(20, truncate=False)

# 5. Schema Check
print("\n" + "="*50)
print("DATA SCHEMAS")
print("="*50)
print("Kafka Schema:")
df_kafka_unique.printSchema()
print("Address Schema:")
df_addresses_unique.printSchema()

spark.stop()