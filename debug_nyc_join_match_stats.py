from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, regexp_replace, trim ,split

# =========================
# SparkSession
# =========================
spark = SparkSession.builder \
    .appName("Debug_NYC_Join_Stats") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# =========================
# Load Data
# =========================
df_kafka_raw = spark.read.parquet("s3a://spark/nyc_parking_raw.parquet")
df_addresses_raw = spark.read.parquet("s3a://spark/data/dims/dim_address")

# =========================
# Clean Kafka Data
# =========================
df_kafka_clean = df_kafka_raw.fillna({"house_number": "", "street_name": ""}) \
    .withColumn("house_number_clean", regexp_replace(col("house_number"), "[-\\s]", "")) \
    .withColumn("street_name_clean", upper(trim(regexp_replace(col("street_name"), "[^A-Z0-9]", ""))))

# =========================
# Clean Addresses Data
# =========================
df_addresses_clean = df_addresses_raw.fillna({"H_NO": "", "ST_NAME": ""}) \
    .withColumn("H_NO_clean", regexp_replace(col("H_NO"), "[-\\s]", "")) \
    .withColumn("ST_NAME_clean", upper(trim(regexp_replace(col("ST_NAME"), "[^A-Z0-9]", ""))))

# =========================
# Join on cleaned columns
# =========================
df_joined = df_kafka_clean.join(
    df_addresses_clean,
    (df_kafka_clean.house_number_clean == df_addresses_clean.H_NO_clean) &
    (df_kafka_clean.street_name_clean == df_addresses_clean.ST_NAME_clean),
    how="left"
)

# =========================
# Compute statistics
# =========================
total_rows = df_kafka_clean.count()
matched_rows = df_joined.filter(col("LAT").isNotNull() & col("LON").isNotNull()).count()
match_percentage = (matched_rows / total_rows) * 100 if total_rows > 0 else 0

print("="*50)
print("JOIN STATISTICS")
print("="*50)
print(f"Total Kafka rows          : {total_rows}")
print(f"Rows matched to addresses : {matched_rows}")
print(f"Match percentage          : {match_percentage:.2f}%")

# =========================
# Show sample joined data
# =========================
print("\n" + "="*50)
print("SAMPLE JOINED DATA")
print("="*50)
df_joined.select(
    "summons_number", "house_number", "street_name", "LAT", "LON"
).show(20, truncate=False)

spark.stop()