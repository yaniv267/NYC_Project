from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, split, element_at , min, max, countDistinct
from nyc_schema import traffic_silver_schema


spark = SparkSession.builder \
.appName("NYC_Traffic_Silver_Processing") \
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

input_path = "s3a://spark/bronze/nyc_traffic/"
df_bronze = spark.read.parquet(input_path)


df_silver = df_bronze \
    .filter(col("status") != "-101") \
    .filter(col("speed").cast("float") > 0) \
    .dropDuplicates(["link_id", "data_as_of"]) \
    .withColumn("speed", col("speed").cast("float")) \
    .withColumn("travel_time", col("travel_time").cast("int")) \
    .withColumn("event_time", to_timestamp(col("data_as_of"), "yyyy-MM-dd'T'HH:mm:ss.000")) \
    .withColumn("ingestion_time", to_timestamp(col("ingested_at")))


df_silver = df_silver \
    .withColumn("first_coords", split(col("link_points"), " ")[0]) \
    .withColumn("latitude", split(col("first_coords"), ",")[0].cast("double")) \
    .withColumn("longitude", split(col("first_coords"), ",")[1].cast("double"))

final_columns = [field.name for field in traffic_silver_schema]
df_silver_final = df_silver.select(*final_columns)

print("🧐 Checking the first 5 records of Silver Data:")
df_silver_final.show(5, truncate=False)
total_rows = df_silver_final.count()

print(f"📊 Total observations in Silver Layer: {total_rows}")

time_range = df_silver_final.select(
    min("event_time").alias("earliest"), 
    max("event_time").alias("latest")
).show()

# 2. בדיקה כמה כבישים שונים יש לנו בתוך ה-1,440 האלו
unique_links = df_silver_final.select(countDistinct("link_id")).show()

output_path = "s3a://spark/silver/nyc_traffic/"
df_silver_final.write.mode("overwrite").parquet(output_path)

print(f"✨ Silver layer for Traffic complete! Processed data saved to: {output_path}")