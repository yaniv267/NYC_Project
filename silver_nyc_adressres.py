

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from nyc_schema import silver_address_schema 

# =========================
# 1. INITIALIZE SPARK SESSION
# =========================

spark = SparkSession.builder \
    .appName("silver_nyc_addresses") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "2g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Set logging level
spark.sparkContext.setLogLevel("WARN")

# =========================
# 2. LOAD DATA FROM BRONZE
# =========================
bronze_df = spark.read.parquet("s3a://spark/bronze/addresses/")            
print(f"Refining Bronze data to Silver...")


# =========================
# 3. DATA TRANSFORMATION & CLEANING
# =========================
df_silver = (
    bronze_df
    
    # Filter rows: keeping only records with geometry and street name
    .filter(F.col("the_geom").isNotNull() & F.col("Full Street Name").isNotNull())
    
    # Extract Longitude using Regex
    .withColumn("longitude", F.regexp_extract(F.col("the_geom"), r"POINT \(([^ ]+) ([^ ]+)\)", 1).cast("double"))
    
    # Extract Latitude using Regex
    .withColumn("latitude", F.regexp_extract(F.col("the_geom"), r"POINT \(([^ ]+) ([^ ]+)\)", 2).cast("double"))
    
    # Select and rename columns to standard format
    .select(
        F.col("Address Point ID").cast("string").alias("address_id"),
        F.trim(F.col("Full Street Name")).alias("street_name"),
        F.col("Borough Code").cast("string").alias("borough_code"),
        F.col("ZIPCODE").cast("string").alias("zip_code"),
        "longitude",
        "latitude"
    )
    
    # Remove duplicates based on unique address ID
    .dropDuplicates(["address_id"])
)

# Apply final schema from the schema file
final_columns = [field.name for field in silver_address_schema]
df_silver_final = df_silver.select(*final_columns)

# =========================
# 4. SAVE TO SILVER LAYER (MINIO)
# =========================
print("Saving cleaned data to Silver layer...")

df_silver_final.write.mode("overwrite").parquet("s3a://spark/silver/addresses/")

# =========================
# 5. VALIDATION & ANALYSIS
# =========================
print("\n" + "="*50)
print("📊 Silver Layer Results")
print("="*50)

final_count = df_silver_final.count()
print(f"Total processed records: {final_count:,}")

print("\n--- Final Silver Schema: ---")
df_silver_final.printSchema()

print("\n--- Top 10 rows: ---")
df_silver_final.show(10, truncate=False)
print("="*50)

# =========================
# 6. TERMINATE SESSION
# =========================
spark.stop()


# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, to_timestamp, concat, lit
# from nyc_schema import silver_crashes_schema # וודא שהסכימה כאן תואמת לעמודות הסופיות
# from pyspark.sql.functions import col, to_timestamp, concat, lit, date_format,month,year


# # 1. יצירת Spark Session
# spark = SparkSession.builder \
#     .appName("NYC_Crashes_Silver_Processing") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
#     .config("spark.driver.memory", "4g") \
#     .config("spark.executor.memory", "4g") \
#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
#     .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .getOrCreate()
      
# spark.sparkContext.setLogLevel("WARN")

# df_bronze = spark.read.parquet("s3a://spark/bronze/nyc_crashes/")
# # df_bronze.select("crash_date", "crash_time").show(20, False)

# df_silver = (
#     df_bronze
#     .dropDuplicates(["collision_id"])
#     .filter(col("latitude").isNotNull() & col("longitude").isNotNull())

#     # ⏰ timestamp
#     .withColumn("date_only", to_timestamp(col("crash_date"))) \
#     .withColumn(
#     "crash_timestamp",
#     to_timestamp(
#         concat(
#             date_format(col("date_only"), "yyyy-MM-dd"),
#             lit(" "),
#             col("crash_time")
#         ),
#         "yyyy-MM-dd H:mm"
#     )
# )

#     # 🧠 time features
#     .withColumn("year", year(col("crash_timestamp")))
#     .withColumn("month", month(col("crash_timestamp")))
#     .withColumn("day_of_week", date_format(col("crash_timestamp"), "EEEE"))

#     # 👥 injuries
#     .withColumn("total_injured", col("NUMBER_OF_PERSONS_INJURED").cast("int"))
#     .withColumn("total_killed", col("NUMBER_OF_PERSONS_KILLED").cast("int"))
#     .withColumn("pedestrians_injured", col("NUMBER_OF_PEDESTRIANS_INJURED").cast("int"))
#     .withColumn("cyclist_injured", col("NUMBER_OF_CYCLIST_INJURED").cast("int"))
#     .withColumn("motorist_injured", col("NUMBER_OF_MOTORIST_INJURED").cast("int"))

#     # 📍 location
#     .withColumn("latitude", col("latitude").cast("double"))
#     .withColumn("longitude", col("longitude").cast("double"))

#     # 🚗 causes
#     .withColumn("contributing_factor", col("CONTRIBUTING_FACTOR_VEHICLE_1"))
#     .withColumn("vehicle_type", col("VEHICLE_TYPE_CODE1"))

#     # ⏱ system
#     .withColumn("ingestion_time", col("ingestion_timestamp"))
# )
# df_silver_processed = df_silver.select(
#     "collision_id",
#     "crash_timestamp",
#     "year",
#     "month",
#     "day_of_week",
#     "latitude",
#     "longitude",
#     "on_street_name",
#     "total_injured",
#     "total_killed",
#     "pedestrians_injured",
#     "cyclist_injured",
#     "motorist_injured",
#     "contributing_factor",
#     "vehicle_type",
#     "ingestion_time",
#     col("BOROUGH").alias("borough")
# )

# output_path = "s3a://spark/silver/nyc_crashes/"

# df_silver_final = df_silver_processed.select(*[f.name for f in silver_crashes_schema])
# df_silver_final.show(50)
# df_silver_final.write \
#     .mode("overwrite") \
#     .partitionBy("year", "month") \
#     .parquet(output_path)

# print("Silver updated ✔")