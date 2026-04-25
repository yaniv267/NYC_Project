from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col ,year,month,to_timestamp
from Nyc_Project.src.common.nyc_schema import bronze_parking_schema

# =========================
# 1. CONFIGURATION & PATHS
# =========================

KAFKA_BOOTSTRAP_SERVERS = "course-kafka:9093"
KAFKA_TOPIC = "nyc_traffic_violations_stream"
MINIO_OUTPUT_PATH = "s3a://spark/bronze/nyc_parking_violation/"
CHECKPOINT_PATH = "s3a://spark/bronze/nyc_parking_violation/_checkpoints/"

# =========================
# 2. INITIALIZE SPARK SESSION
# =========================

spark = SparkSession.builder \
.appName("NYC Parking Bronze Ingestion") \
.config("spark.jars.packages", 
"org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
"org.apache.hadoop:hadoop-aws:3.3.4,"
"com.amazonaws:aws-java-sdk-bundle:1.12.262") \
.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
.config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
.config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
.config("spark.hadoop.fs.s3a.path.style.access", "true") \
.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
.getOrCreate()


spark.sparkContext.setLogLevel("WARN")

# =========================
# 3. READ STREAM FROM KAFKA
# =========================

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# =========================
# 4. PARSE JSON DATA & SCHEMA
# =========================

df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str")

df_parsed = df_json.select(
    from_json(col("json_str"), bronze_parking_schema).alias("data")
).select("data.*")

# =========================
# 5. DATA TRANSFORMATIONS
# =========================

df_final=df_parsed\
         .withColumn("issue_timestamp", to_timestamp(col("issue_date"))) \
         .withColumn("year", year(col("issue_timestamp"))) \
         .withColumn("month", month(col("issue_timestamp")))
         
# =========================
# 6. WRITE STREAM TO MINIO (BRONZE)
# =========================         

query = df_final.writeStream \
    .format("parquet") \
    .option("path", MINIO_OUTPUT_PATH) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .partitionBy("year", "month") \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .start()
    # .start()

print("✅ Data successfully ingested into MinIO Bronze layer")

query.awaitTermination()
spark.stop()

