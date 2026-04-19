from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, year, month, current_timestamp
from nyc_schema import bronze_crashes_schema

# --- CONFIGURATION ---
KAFKA_BOOTSTRAP_SERVERS = "course-kafka:9093"
KAFKA_TOPIC = "nyc_crashes_stream"
MINIO_OUTPUT_PATH = "s3a://spark/bronze/nyc_crashes/"
CHECKPOINT_PATH = "s3a://spark/bronze/nyc_crashes/_checkpoints/"


    # 1. יצירת ה-Spark Session עם כל הקונפיגורציות ל-Minio וקפקא
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

    # 3. עיבוד הנתונים: המרה מ-Binary ל-JSON ופירוק לפי ה-Schema
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), bronze_crashes_schema).alias("data")) \
        .select("data.*")

    # 4. הוספת עמודות מטא-דאטה וחלוקה לזמנים (Partitions)
    # שים לב לשימוש ב-dd קטן עבור היום בחודש
df_final = df_parsed \
        .filter(col("CRASH_DATE").isNotNull()) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("event_date", to_date(col("CRASH_DATE"), "MM/dd/yyyy")) \
        .withColumn("event_year", year(col("event_date"))) \
        .withColumn("event_month", month(col("event_date")))

    # 5. כתיבת הסטרים ל-Minio בפורמט Parquet
query = df_final.writeStream \
        .format("parquet") \
        .option("path", MINIO_OUTPUT_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .partitionBy("event_year", "event_month") \
        .outputMode("append") \
        .trigger(processingTime='1 minute') \
        .start()

print(f"Ingestion started successfully!")
print(f"Writing to: {MINIO_OUTPUT_PATH}")
print(f"Checkpoints at: {CHECKPOINT_PATH}")

query.awaitTermination()