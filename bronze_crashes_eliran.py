from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, year, month, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType
from nyc_schema_eliran import crashes_bronze_schema

# --- CONFIGURATION ---
KAFKA_BOOTSTRAP_SERVERS = "course-kafka:9093"
KAFKA_TOPIC = "nyc_crashes_bronze"
MINIO_OUTPUT_PATH = "s3a://spark/bronze/nyc_crashes/"
CHECKPOINT_PATH = "s3a://spark/bronze/nyc_crashes/_checkpoints/"

spark = SparkSession.builder \
.appName("NYC_Crashes_Bronze_Ingestion") \
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
  
kafka_bootstrap_servers = "course-kafka:9093"
kafka_topic = "nyc_crashes_bronze"

    # ביטול לוגים מיותרים כדי לראות רק שגיאות
spark.sparkContext.setLogLevel("WARN")

    # 2. קריאת הסטרים מ-Kafka
df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()



df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str")

df_parsed = df_json.select(
    from_json(col("json_str"), crashes_bronze_schema).alias("data")
).select("data.*")




# minio_endpoint = "s3a://spark/bronze/nyc_crashes/"
# checkpoint_path = "s3a://spark/bronze/nyc_crashes/_checkpoints/"


df_final = df_parsed \
        .filter(col("CRASH_DATE").isNotNull()) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("event_date", to_date(col("CRASH_DATE"), "MM/dd/yyyy")) \
        .withColumn("event_year", year(col("event_date"))) \
        .withColumn("event_month", month(col("event_date")))
df_final.printSchema()

# query = df_final.writeStream \
#     .format("parquet") \
#     .option("path", minio_endpoint) \
#     .option("checkpointLocation", checkpoint_path) \
#     .outputMode("append") \
#     .start()

query = df_final.writeStream \
        .format("parquet") \
        .option("path", MINIO_OUTPUT_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .partitionBy("event_year", "event_month") \
        .outputMode("append") \
        .trigger(processingTime='1 minute') \
        .start()


print("✅ Data successfully ingested into MinIO Bronze layer")

query.awaitTermination()


