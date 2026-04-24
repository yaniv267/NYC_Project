from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, concat, lit, date_format, month, year, current_date, add_months
#from Nyc_Project.src.common.nyc_schema import silver_crashes_schema 
from src.common.nyc_schema import silver_crashes_schema


# =========================
# 2. INITIALIZE SPARK SESSION
# =========================

spark = SparkSession.builder \
    .appName("NYC_Crashes_Silver_Processing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
      
spark.sparkContext.setLogLevel("WARN")

# =========================
# 3. READ RAW DATA FROM BRONZE
# =========================

df_bronze = spark.read.parquet("s3a://spark/bronze/nyc_crashes/")

# =========================
# 4. DATA CLEANING & TRANSFORMATION
# =========================

retention_limit = add_months(current_date(), -24)

df_silver = (
    df_bronze
    # OPTIMIZATION 1: Filter BEFORE dropDuplicates to reduce the amount of data being shuffled
    .filter(col("latitude").isNotNull() & col("longitude").isNotNull())

    # --- Timestamp Standardization --
    .withColumn("date_only", to_timestamp(col("crash_date"))) \
    .withColumn(
    "crash_timestamp",
    to_timestamp(
        concat(
            date_format(col("date_only"), "yyyy-MM-dd"),
            lit(" "),
            col("crash_time")
        ),
        "yyyy-MM-dd H:mm"
    )
)

    # Apply 60-month retention filter
    .filter(col("crash_timestamp") >= retention_limit)
    
    # Drop duplicates on the smaller, time-filtered dataset
    .dropDuplicates(["collision_id"])

    # --- Time Features Extraction ---
    .withColumn("year", year(col("crash_timestamp")))
    .withColumn("month", month(col("crash_timestamp")))
    .withColumn("day_of_week", date_format(col("crash_timestamp"), "EEEE"))

   # --- Injury & Casualty Metrics ---
    .withColumn("total_injured", col("NUMBER_OF_PERSONS_INJURED").cast("int"))
    .withColumn("total_killed", col("NUMBER_OF_PERSONS_KILLED").cast("int"))
    .withColumn("pedestrians_injured", col("NUMBER_OF_PEDESTRIANS_INJURED").cast("int"))
    .withColumn("cyclist_injured", col("NUMBER_OF_CYCLIST_INJURED").cast("int"))
    .withColumn("motorist_injured", col("NUMBER_OF_MOTORIST_INJURED").cast("int"))
    
    # --- Spatial Data & Attributes --
    .withColumn("latitude", col("latitude").cast("double"))
    .withColumn("longitude", col("longitude").cast("double"))
    .withColumn("contributing_factor", col("CONTRIBUTING_FACTOR_VEHICLE_1"))
    .withColumn("vehicle_type", col("VEHICLE_TYPE_CODE1"))
    .withColumn("ingestion_time", col("ingestion_timestamp"))
)

# =========================
# 5. SCHEMA ALIGNMENT (DATA CONTRACT)
# =========================
df_silver_processed = df_silver.select(
    "collision_id",
    "crash_timestamp",
    "year",
    "month",
    "day_of_week",
    "latitude",
    "longitude",
    "on_street_name",
    "total_injured",
    "total_killed",
    "pedestrians_injured",
    "cyclist_injured",
    "motorist_injured",
    "contributing_factor",
    "vehicle_type",
    "ingestion_time",
    col("BOROUGH").alias("borough")
)

# =========================
# 6. WRITE TO SILVER LAYER (PARQUET)
# =========================
output_path = "s3a://spark/silver/nyc_crashes/"

# OPTIMIZATION 2: Cache the DF so the show(), write(), and count() don't trigger the whole pipeline 3 times!
df_silver_final = df_silver_processed.select(*[f.name for f in silver_crashes_schema]).cache()

print("Previewing final Silver data:")
df_silver_final.show(10) # Reduced to 10 for cleaner logs

# OPTIMIZATION 3: coalesce(4) to prevent the "Small Files Problem" in partitioned directories
df_silver_final.coalesce(4).write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(output_path)

# =========================
# 7. FINAL EXECUTION REPORT
# =========================

final_count = df_silver_final.count() 

print("\n" + "="*50)
print("📊 BATCH PROCESSING SUMMARY")
print(f"Target Path: {output_path}")
print(f"Status: SUCCESS")
print(f"Total Records Processed: {final_count:,}")
print(f"Partitioning Strategy: Year / Month")
print("="*50 + "\n")