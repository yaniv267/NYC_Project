# load_violation_codes_to_s3.py

import requests
import pandas as pd
from pyspark.sql import SparkSession

# ------------------------------
# 1. יצירת SparkSession
# ------------------------------
spark = SparkSession.builder \
    .appName("load_violation_minio") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# ------------------------------
# 2. איתור הקובץ הכי עדכני
# ------------------------------
metadata_url = "https://data.cityofnewyork.us/api/views/nc67-uf89"
metadata = requests.get(metadata_url).json()

files = metadata.get("metadata", {}).get("attachments", [])
violation_files = [f for f in files if "ParkingViolationCodes" in f["filename"]]

if not violation_files:
    raise Exception("לא נמצא קובץ ParkingViolationCodes במאגר!")

latest_file = violation_files[0]
download_url = f"https://data.cityofnewyork.us/api/views/nc67-uf89/files/{latest_file['assetId']}?download=true"

print("מוריד את הקובץ:", latest_file['filename'])
r = requests.get(download_url)
local_excel_path = "/tmp/violation_codes_latest.xlsx"
with open(local_excel_path, "wb") as f:
    f.write(r.content)
print("הקובץ הורד בהצלחה!")

# ------------------------------
# 3. קריאה ל-Pandas ואז Spark
# ------------------------------
pdf = pd.read_excel(local_excel_path)
violation_df = spark.createDataFrame(pdf)

violation_df.show(5)
violation_df.printSchema()

# ------------------------------
# 4. כתיבה ל-S3 / MinIO כ-Parquet
# ------------------------------
violation_df.write.mode("overwrite").parquet("s3a://spark/parking_violation_codes")

print("קובץ Parquet נשמר בהצלחה ב-S3 / MinIO!")

spark.stop()

