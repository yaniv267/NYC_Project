from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from Nyc_Project.src.common.nyc_schema import silver_address_schema 

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
    
    # OPTIMIZATION: Early Deduplication to save expensive Regex CPU cycles on duplicate records
    .dropDuplicates(["Address Point ID"])
    
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
)

# Apply final schema from the schema file
final_columns = [field.name for field in silver_address_schema]

# OPTIMIZATION: Cache to prevent re-reading from Bronze 3 times for write, count, and show
df_silver_final = df_silver.select(*final_columns).cache()

# =========================
# 4. SAVE TO SILVER LAYER (MINIO)
# =========================
print("Saving cleaned data to Silver layer...")

# OPTIMIZATION: coalesce(1) to save as a single file, preventing the 200 small files problem from the default shuffle partitions
df_silver_final.coalesce(1).write.mode("overwrite").parquet("s3a://spark/silver/addresses/")

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