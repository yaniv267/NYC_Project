# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from nyc_schema import bronze_311_schema

# spark = SparkSession.builder \
# .appName("NYC Parking Bronze Ingestion") \
# .config("spark.jars.packages", 
# "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
# "org.apache.hadoop:hadoop-aws:3.3.4,"
# "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
# .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
# .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
# .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
# .config("spark.hadoop.fs.s3a.path.style.access", "true") \
# .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
# .getOrCreate()
  
# kafka_bootstrap_servers = "course-kafka:9093"
# kafka_topic = "complaints_Stream"

# df_raw = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
#     .option("subscribe", kafka_topic) \
#     .option("startingOffsets", "earliest") \
#     .load()



# df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str")

# df_parsed = df_json.select(
#     from_json(col("json_str"), bronze_311_schema).alias("data")
# ).select("data.*")


# minio_endpoint = "s3a://spark/bronze/311_complaints/"
# checkpoint_path = "s3a://spark/bronze/311_complaints/_checkpoints/"

# query = df_parsed.writeStream \
#     .format("parquet") \
#     .option("path", minio_endpoint) \
#     .option("checkpointLocation", checkpoint_path) \
#     .outputMode("append") \
#     .start()

# print("✅ Data successfully ingested into MinIO Bronze layer")

# query.awaitTermination()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, to_timestamp, year, month
from nyc_schema import bronze_311_schema

# --- CONFIGURATION ---
KAFKA_BOOTSTRAP_SERVERS = "course-kafka:9093"
KAFKA_TOPIC = "complaints_Stream"
MINIO_OUTPUT_PATH = "s3a://spark/bronze/311_complaints/"
CHECKPOINT_PATH = "s3a://spark/bronze/311_complaints/_checkpoints/"

# 1. יצירת ה-Spark Session
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

# ביטול לוגים מיותרים
spark.sparkContext.setLogLevel("WARN")

# 2. קריאת הסטרים מ-Kafka
df_raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load())

# 3. עיבוד הנתונים (פירוק JSON והוספת עמודות זמן למחיצות)
df_parsed = (df_raw
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), bronze_311_schema).alias("data"))
    .select("data.*")
    .withColumn("ingested_at", current_timestamp())
    # המרה אוטומטית ל-Timestamp וחילוץ שנה וחודש
    .withColumn("timestamp_created", to_timestamp(col("created_date")))
    .withColumn("ingest_year", year(col("timestamp_created")))
    .withColumn("ingest_month", month(col("timestamp_created")))
)

# 4. כתיבה ל-MinIO בפורמט Parquet עם Partitioning
query = (df_parsed.writeStream
    .format("parquet")
    .option("path", MINIO_OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("ingest_year", "ingest_month")
    .outputMode("append")
    .trigger(processingTime='1 minute')
    .start())

print(f"Ingestion started successfully!")
print(f"Listening to Kafka Topic: {KAFKA_TOPIC}")
print(f"Writing Parquet to: {MINIO_OUTPUT_PATH}")

# 5. השארת הסטרים פעיל
query.awaitTermination()