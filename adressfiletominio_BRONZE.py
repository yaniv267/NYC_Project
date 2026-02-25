import requests
from pyspark.sql import SparkSession

# ----------------------------------
# Spark + MinIO Config
# ----------------------------------
spark = SparkSession.builder \
    .appName("NYC_Addresses_Bronze_Ingestion") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
# --------------------------------

# NYC Address Points API
# ----------------------------------
url = "https://data.cityofnewyork.us/resource/uf93-f8nk.csv?$limit=500000"
r = requests.get(url)
with open("nyc_addresses.csv", "wb") as f:
    f.write(r.content)
print("Downloaded CSV ✅")
df_address_raw = spark.read.csv("nyc_addresses.csv", header=True, inferSchema=True)

df_address_raw.printSchema()
df_address_raw.show(5)

# כתיבה ל-MinIO
df_address_raw.write.mode("overwrite").parquet("s3a://spark/bronze/nyc_addresses_parquet")
print("Finished writing to MinIO 🚀")
