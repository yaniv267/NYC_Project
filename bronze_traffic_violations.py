from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from nyc_schema import bronze_traffic_violations

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
  
kafka_bootstrap_servers = "course-kafka:9093"
kafka_topic = "nyc_traffic_violations_stream"

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()



df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str")

df_parsed = df_json.select(
    from_json(col("json_str"), bronze_traffic_violations).alias("data")
).select("data.*")


minio_endpoint = "s3a://spark/bronze/nyc_traffic_violations/"
checkpoint_path = "s3a://spark/bronze/nyc_traffic_violations/_checkpoints/"

query = df_parsed.writeStream \
    .format("parquet") \
    .option("path", minio_endpoint) \
    .option("checkpointLocation", checkpoint_path) \
    .outputMode("append") \
    .start()

print("✅ Data successfully ingested into MinIO Bronze layer")

query.awaitTermination()