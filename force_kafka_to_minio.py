from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 1. הקמת ה-Spark Session עם חבילות קפקא ו-MinIO
spark = SparkSession.builder \
    .appName("Kafka_to_MinIO_Structured") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 2. הגדרת הסכימה המלאה לפי המבנה של NYC Open Data
schema = StructType([
    StructField("summons_number", StringType(), True),
    StructField("issue_date", StringType(), True),
    StructField("violation_code", StringType(), True),
    StructField("violation_description", StringType(), True),
    StructField("house_number", StringType(), True),
    StructField("street_name", StringType(), True),
    StructField("violation_county", StringType(), True),
    StructField("plate_id", StringType(), True)
    # ניתן להוסיף כאן עוד שדות מה-JSON במידת הצורך
])

print("--- Reading from Kafka ---")
# 3. קריאה מקפקא
df_kafka = spark.read.format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:9093") \
    .option("subscribe", "nyc_parking_data_last_date") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. פירוק ה-JSON לעמודות נפרדות (זה השלב שמונע את השגיאות הבאות)
parsed_df = df_kafka.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# 5. בדיקה שהצלחנו לפרק (הדפסת סכימה ודוגמה)
print("--- Structured Data Schema: ---")
parsed_df.printSchema()
parsed_df.show(5)

# 6. שמירה ל-MinIO בפורמט פארקט
# מצב 'overwrite' ימחק את התיקייה הריקה הישנה ויכתוב נתונים חדשים ומסודרים
output_path = "s3a://spark/nyc_parking_geocoded"
print(f"--- Writing structured data to MinIO: {output_path} ---")

parsed_df.write.mode("overwrite").parquet("s3a://spark/nyc_parking_raw.parquet")

print("--- DONE! You can now run the Geocoding script ---")
spark.stop()