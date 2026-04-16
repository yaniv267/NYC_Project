from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, to_timestamp, year, month
from nyc_schema import bronze_traffic_schema

# --- CONFIGURATION ---
KAFKA_BOOTSTRAP_SERVERS = "course-kafka:9093"
KAFKA_TOPIC = "nyc_traffic_stream"
MINIO_OUTPUT_PATH = "s3a://spark/bronze/nyc_traffic/"
CHECKPOINT_PATH = "s3a://spark/bronze/nyc_traffic/_checkpoints/"

spark = (SparkSession.builder
    .appName("NYC_Traffic_Bronze_Ingestion")
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

# 1. קריאה מהקפקא
df_raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load())

# 2. עיבוד הנתונים לפי השדות החדשים
df_parsed = (df_raw
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), bronze_traffic_schema).alias("data"))
    .select("data.*")
    # הוספת חותמת זמן של Spark
    .withColumn("spark_ingested_at", current_timestamp())
    # שימוש ב-data_as_of לצורך חילוץ שנה וחודש למחיצות
    .withColumn("event_ts", to_timestamp(col("data_as_of"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
    .withColumn("ingest_year", year(col("event_ts")))
    .withColumn("ingest_month", month(col("event_ts")))
)

# 3. כתיבה ל-MinIO עם מחיצות וטריגר
query = (df_parsed.writeStream
    .format("parquet")
    .option("path", MINIO_OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("ingest_year", "ingest_month")
    .outputMode("append")
    .trigger(processingTime='1 minute')
    .start())

print(f"🚀 Traffic Speed Ingestion started!")
print(f"📡 Topic: {KAFKA_TOPIC}")

query.awaitTermination()