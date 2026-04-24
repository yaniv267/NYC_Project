from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
from Nyc_Project.src.common.nyc_schema import silver_311_schema
from Nyc_Project.src.common.elk_logger import log_to_elk
# =========================
# 2. INITIALIZE SPARK SESSION
# =========================

spark = (SparkSession.builder
    .appName("NYC_311_Silver_Final")
    .config("spark.jars.packages", 
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

log_to_elk("Silver Layer - Starting to clean 311 Complaints data.")

# =========================
# 3. READ RAW DATA FROM BRONZE
# =========================

print("📖 Reading 311 Bronze data...")
df_bronze = spark.read.parquet("s3a://spark/bronze/311_complaints/")


# ==========================================
# 4. DATA CLEANING & STANDARDIZATION
# ==========================================
# Applying 24-month data retention policy

retention_limit = F.add_months(F.current_date(), -24)

df_transformed = (df_bronze
    # 1. Cast the date first so we can filter on it
    .withColumn("created_date", F.to_timestamp("created_date", "yyyy-MM-dd'T'HH:mm:ss.000"))
    
    # OPTIMIZATION 1: Filter out old records BEFORE dropping duplicates (Reduces Shuffle payload significantly)
    .filter(F.col("created_date") >= retention_limit)
    
    # 2. Now drop duplicates on the much smaller remaining dataset
    .dropDuplicates(["unique_key"])
    
    .withColumn("agency", F.trim(F.upper(F.col("agency"))))
    .withColumn("complaint_type", F.trim(F.upper(F.col("complaint_type"))))
    .withColumn("descriptor", F.trim(F.upper(F.col("descriptor"))))
    .withColumn("city", F.trim(F.upper(F.col("city"))))
    .withColumn("borough", F.trim(F.upper(F.col("borough"))))
    .withColumn("street_name", F.trim(F.upper(F.col("street_name"))))
    .withColumn("incident_zip", F.trim(F.col("incident_zip")))
    .withColumn("latitude", F.col("latitude").cast("double"))
    .withColumn("longitude", F.col("longitude").cast("double"))
    .withColumn("year", F.year(F.col("created_date")).cast("int"))
    .withColumn("month", F.month(F.col("created_date")).cast("int"))
    .withColumn("hour", F.hour(F.col("created_date")).cast("int"))
    .withColumn("day_name", F.date_format(F.col("created_date"), "EEEE"))
)

# ==========================================
# 5. SCHEMA ALIGNMENT & DATA CONTRACT
# ==========================================
# Selecting columns based on the silver_311_schema definition

schema_columns = [field.name for field in silver_311_schema]

# OPTIMIZATION 2: Cache the final DataFrame to avoid running the whole pipeline twice for write and show
df_silver_final = df_transformed.select(*schema_columns).cache()

# ==========================================
# 6. WRITE TO SILVER LAYER (PARQUET)
# ==========================================

output_path = "s3a://spark/silver/311_complaints/"
print(f"💾 Saving data to Silver layer (Partitioned by year/month): {output_path}")

# OPTIMIZATION 3: coalesce(4) to prevent generating too many small files inside each month's partition
(df_silver_final.coalesce(4).write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(output_path))

# ==========================================
# 7. FINAL VALIDATION & REPORTING
# ==========================================

final_count = df_silver_final.count()

print("\n" + "="*50)
print("✅ SILVER PROCESSING COMPLETE")
print(f"Target: {output_path}")
print(f"Total Active Records Saved: {final_count:,}")
print("="*50)

# ---LOGSTASH ---
log_to_elk("Silver Layer - Cleaning finished successfully.")

print("\nPreview of Processed Silver Data:")
df_silver_final.show(5, truncate=False)


