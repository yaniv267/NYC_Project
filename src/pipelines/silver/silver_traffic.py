from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, split, min, max, year, month, hour, date_format, dayofmonth, to_date, when, current_date, add_months
from Nyc_Project.src.common.nyc_schema import silver_traffic_schema
import json
from Nyc_Project.src.common.elk_logger import log_to_elk

# =========================
# 2. INITIALIZE SPARK SESSION
# =========================

spark = (SparkSession.builder
         .appName("NYC_Traffic_Silver_Processing")
         .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
         .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
         .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
         .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

log_to_elk("Silver Layer - Starting to clean traffic data.")

# =========================
# 3. READ FROM BRONZE (RAW JSON)
# =========================

input_path = "s3a://spark/bronze/nyc_traffic/"
df_bronze = spark.read.parquet(input_path)
bronze_count = df_bronze.count()
# =========================
# 4. DATA TRANSFORMATION & CLEANING
# =========================

retention_limit = add_months(current_date(), -12)

df_silver = (df_bronze
             .withColumn("event_time", to_timestamp(col("data_as_of"), "yyyy-MM-dd'T'HH:mm:ss.000"))
             .filter(
                 (col("event_time") >= retention_limit) & 
                 (col("status") != "-101") & 
                 (col("speed").cast("float") > 0) & 
                 (col("speed").cast("float") <= 80)
             )
             .dropDuplicates(["link_id", "data_as_of"])
             .withColumn("borough", col("borough"))
             .withColumn("year", year(col("event_time")))
             .withColumn("month", month(col("event_time")))
             .withColumn("day", dayofmonth(col("event_time")))
             .withColumn("hour", hour(col("event_time")))
             .withColumn("day_name", date_format(col("event_time"), "EEEE"))
             .withColumn("date_id", to_date(col("event_time")))
             .withColumn("is_weekend", when(col("day_name").isin("Saturday", "Sunday"), True).otherwise(False))
             .withColumn("speed", col("speed").cast("float"))
             .withColumn("travel_time", col("travel_time").cast("int"))
             .withColumn("first_coords", split(col("link_points"), " ")[0])
             .withColumn("latitude", split(col("first_coords"), ",")[0].cast("double"))
             .withColumn("longitude", split(col("first_coords"), ",")[1].cast("double"))
             )

# =========================
# 5. APPLY SILVER DATA CONTRACT (SCHEMA ALIGNMENT)
# =========================


final_columns = [field.name for field in silver_traffic_schema]

df_silver_final = df_silver.select(*final_columns).cache()

print(f"📊 Total observations in Silver Layer: {df_silver_final.count()}")


# =========================
# 6. WRITE TO SILVER LAYER (PARQUET)
# =========================

output_path = "s3a://spark/silver/nyc_traffic/"

(df_silver_final.coalesce(4).write
 .mode("overwrite")
 .partitionBy("year", "month", "day")
 .parquet(output_path))

print(f"✨ Silver layer for Traffic complete! Saved to: {output_path}")



# --- Structured Logging: Dispatch Metrics to ELK ---
final_count = df_silver_final.count()
dropped_records = bronze_count - final_count

time_metrics = df_silver_final.select(
    min("event_time").alias("min_t"), 
    max("event_time").alias("max_t")
).collect()[0]

metrics_log = {
    "action": "Silver_Processing_Complete",
    "layer": "Silver",
    "dataset": "NYC_Traffic",
    "metrics": {
        "bronze_records_read": bronze_count,
        "silver_records_written": final_count,
        "records_dropped": dropped_records,
        "data_window_start": time_metrics["min_t"].isoformat() if time_metrics["min_t"] else None,
        "data_window_end": time_metrics["max_t"].isoformat() if time_metrics["max_t"] else None
    }
}

log_to_elk(json.dumps(metrics_log))

print(f"✨ Silver layer for Traffic complete! Saved to: {output_path}")
df_silver_final.show(5, truncate=False)