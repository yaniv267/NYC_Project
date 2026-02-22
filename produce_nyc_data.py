from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

# 1. יצירת Session עם הגדרות Kafka ו-MinIO
spark = SparkSession.builder \
    .appName("NYC_Streaming_to_MinIO") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 2. קריאת Stream מ-Kafka
raw_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:9093") \
    .option("subscribe", "nyc_parking_data_last_date") \
    .option("startingOffsets", "earliest") \
    .load()

# 3. המרת הנתונים מפורמט Binary לטקסט
streaming_df = raw_stream_df.selectExpr("CAST(value AS STRING)")

# 4. כתיבת הנתונים ל-MinIO בפורמט Parquet
# שימוש ב-Checkpoint חיוני ל-Streaming כדי לעקוב אחרי ההתקדמות
query = streaming_df.writeStream \
    .format("parquet") \
    .option("path", "s3a://spark/nyc_parking_streaming.parquet") \
    .option("checkpointLocation", "s3a://spark/checkpoints/nyc_parking") \
    .outputMode("append") \
    .start()

print("🚀 ה-Streaming התחיל! הנתונים נכתבים ל-MinIO...")
query.awaitTermination()