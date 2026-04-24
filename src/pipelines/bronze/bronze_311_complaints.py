

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, to_timestamp, year, month
from Nyc_Project.src.common.nyc_schema import bronze_311_schema

# =========================
# 2. CONFIGURATION & PATHS
# =========================

KAFKA_BOOTSTRAP_SERVERS = "course-kafka:9093"
KAFKA_TOPIC = "complaints_stream"
MINIO_OUTPUT_PATH = "s3a://spark/bronze/311_complaints/"
CHECKPOINT_PATH = "s3a://spark/bronze/311_complaints/_checkpoints/"

# =========================
# 3. INITIALIZE SPARK SESSION
# =========================

spark = (SparkSession.builder
    .appName("NYC_311_Bronze_Ingestion")
    .config("spark.jars.packages", 
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# =========================
# 4. READ STREAM FROM KAFKA
# =========================

df_raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load())

# ==========================================
# 5. JSON PARSING & FEATURE ENGINEERING
# ==========================================
df_parsed = (df_raw
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), bronze_311_schema).alias("data"))
    .select("data.*")
    .withColumn("ingested_at", current_timestamp())
    .withColumn("timestamp_created", to_timestamp(col("created_date")))
    .withColumn("ingest_year", year(col("timestamp_created")))
    .withColumn("ingest_month", month(col("timestamp_created")))
)

# ==========================================
# 6. STREAM SINK: WRITING TO BRONZE LAYER
# ==========================================
query = (df_parsed.writeStream
    .format("parquet")
    .option("path", MINIO_OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("ingest_year", "ingest_month")
    .outputMode("append")
    .trigger(availableNow=True) \
    .start()
    # .trigger(processingTime='1 minute')
    # .start()
)
# ==========================================
# 7. LOGGING & STREAM SYNCHRONIZATION
# ==========================================
print("✅ Data successfully ingested into MinIO Bronze layer")
print(f"Listening to Kafka Topic: {KAFKA_TOPIC}")
print(f"Writing Parquet to: {MINIO_OUTPUT_PATH}")

query.awaitTermination()
spark.stop()