from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, to_date, trim, lit, year, month, add_months, current_date, date_format, to_timestamp, when, regexp_extract
from Nyc_Project.src.common.nyc_schema import silver_parking_violations
from Nyc_Project.src.common.elk_logger import log_to_elk
import json

# =========================
# 1. INITIALIZE SPARK SESSION
# =========================

spark = SparkSession.builder \
    .appName("NYC_Parking_Silver") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "2g") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =========================
# 2. READ FROM BRONZE LAYER
# =========================

df_bronze = spark.read.parquet("s3a://spark/bronze/nyc_parking_violation/")
bronze_count = df_bronze.count()
# =========================
# 3. DATA CLEANING & HISTORY MANAGEMENT
# =========================

retention_limit = add_months(current_date(), -24)

df_cleaned = (
    df_bronze
    # OPTIMIZATION 1: Early Filtering - remove duplicates and nulls BEFORE heavy calculations
    .dropDuplicates(["summons_number"])
    .filter(col("street_name").isNotNull())

    # Handle dates and timestamps first to enable retention filtering
    .withColumn("issue_timestamp", to_timestamp(col("issue_date")))
    .withColumn("issue_date", to_date(col("issue_timestamp")))
    
    # OPTIMIZATION 2: BUG FIX - Apply the retention limit filter that was missing
    .filter(col("issue_date") >= retention_limit)

    # Now apply the heavy string manipulation only on the required subset
    .withColumn("borough_code", 
        when(trim(upper(col("violation_county"))).isin("NY", "MN", "NEW Y", "MANHATTAN"), "1")
        .when(trim(upper(col("violation_county"))).isin("BX", "BRONX"), "2")
        .when(trim(upper(col("violation_county"))).isin("K", "BK", "KINGS", "BKLYN", "BROOKLYN"), "3")
        .when(trim(upper(col("violation_county"))).isin("Q", "QN", "QUEENS", "QU"), "4")
        .when(trim(upper(col("violation_county"))).isin("R", "ST", "RICHMOND", "STATEN ISLAND"), "5")
        .otherwise(None)
    )
    .filter(col("borough_code").isNotNull()) 

    # Extract hour from violation_time (e.g., "0409A")
    .withColumn("hour", regexp_extract(col("violation_time"), r"^(\d{2})", 1).cast("int"))
    # Clean street name - trim whitespace and uppercase
    .withColumn("street_name", trim(upper(col("street_name"))))  
    .withColumn("day_of_week", date_format(col("issue_timestamp"), "E"))
)

# =========================
# 4. ADD PARTITIONING COLUMNS
# =========================
df_cleaned = df_cleaned.withColumn("year", year(col("issue_date"))) \
                       .withColumn("month", month(col("issue_date")))

# =========================
# 5. APPLY DATA CONTRACT (SCHEMA ALIGNMENT)
# =========================

existing_columns = df_cleaned.columns
final_selection = []

for field in silver_parking_violations:
    if field.name in existing_columns:
        final_selection.append(col(field.name))
    else:
        final_selection.append(lit(None).cast(field.dataType).alias(field.name))

# OPTIMIZATION 3: Cache to enable fast count() and show() without re-running the Pipeline
df_silver_final = df_cleaned.select(*final_selection).cache()

# =========================
# 6. WRITE TO SILVER LAYER (MINIO)
# =========================
output_path = "s3a://spark/silver/nyc_parking_violation/"
print(f"💾 Saving to Silver layer with Year/Month partitioning at {output_path}...")

# OPTIMIZATION 4: Coalesce to prevent small files, and use overwrite with dynamic partitioning
df_silver_final.coalesce(4).write \
    .format("parquet") \
    .partitionBy("year", "month") \
    .mode("overwrite") \
    .save(output_path)

# Enterprise-grade logging
final_count = df_silver_final.count()
dropped_records = bronze_count - final_count

print("\n" + "="*50)
print("✅ SILVER PARKING VIOLATIONS PROCESS COMPLETE")
print(f"Target Path: {output_path}")
print(f"Total Bronze Records: {bronze_count:,}")
print(f"Total Valid Records Saved (Silver): {final_count:,}")
print(f"Total Records Dropped: {dropped_records:,}")
print("="*50 + "\n")

#  Emit Execution Metrics to Logstash
metrics_log = {
    "action": "Silver_Processing_Complete",
    "layer": "Silver",
    "dataset": "NYC_Parking_Violations",
    "metrics": {
        "bronze_records_read": bronze_count,
        "silver_records_written": final_count,
        "records_dropped": dropped_records
    }
}

log_to_elk(json.dumps(metrics_log))