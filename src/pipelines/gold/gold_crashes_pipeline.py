from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.common.nyc_schema import gold_crashes_schema

# =========================
# 2. INITIALIZE SPARK SESSION
# =========================

spark = (SparkSession.builder 
    .appName("NYC_Gold_Enriched_Crashes") 
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0,org.elasticsearch:elasticsearch-spark-30_2.12:7.13.2")
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") 
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") 
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") 
    .config("spark.hadoop.fs.s3a.path.style.access", "true") 
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# =========================
# 3. LOAD SILVER DATASETS
# =========================

print("📖 Reading Silver datasets...")
df_crashes = spark.read.parquet("s3a://spark/silver/nyc_crashes/")
df_addresses = spark.read.parquet("s3a://spark/silver/addresses/")

# =========================
# 4. PREPARE GEO-REFERENCE LOOKUP
# =========================

addr_lookup = (df_addresses
    .select(
        F.round("latitude", 4).alias("lat_idx"),
        F.round("longitude", 4).alias("lon_idx"),
        F.col("street_name").alias("ref_address"),
    )
    .dropDuplicates(["lat_idx", "lon_idx"])
)

# =========================
# 5. DATA ENRICHMENT (SPATIAL JOIN)
# =========================

enriched_crashes = (df_crashes
    .withColumn("lat_idx", F.round("latitude", 4))
    .withColumn("lon_idx", F.round("longitude", 4))
    # OPTIMIZATION 1: Broadcast Join to prevent network shuffle
    .join(F.broadcast(addr_lookup), on=["lat_idx", "lon_idx"], how="left")
    .withColumn("final_borough", F.col("borough"))
    .withColumn("final_street", F.coalesce(F.col("ref_address"), F.col("on_street_name")))
)

# =========================
# 6. AGGREGATION & METRICS CALCULATION
# =========================

crash_base = (enriched_crashes
    .withColumn("borough", F.coalesce(F.col("final_borough"), F.lit("UNKNOWN")))
    .withColumn("street_name", F.coalesce(F.col("final_street"), F.lit("UNKNOWN LOCATION")))
    # OPTIMIZATION 2: Filter BEFORE groupBy to reduce shuffle payload
    .filter(F.col("borough") != "UNKNOWN")
    .filter(F.col("street_name") != "UNKNOWN LOCATION")
    .groupBy("borough", "street_name")
    .agg(
        F.count("collision_id").alias("total_crashes"),
        F.sum("total_injured").alias("total_injured"),
        F.sum("total_killed").alias("total_killed"),
        F.first("contributing_factor", ignorenulls=True).alias("main_cause"),
        F.first("latitude", ignorenulls=True).alias("latitude"),    
        F.first("longitude", ignorenulls=True).alias("longitude"),
        F.min("crash_timestamp").alias("first_crash_date"),
        F.max("crash_timestamp").alias("last_crash_date"),
        F.countDistinct(F.to_date("crash_timestamp")).alias("unique_crash_days"),
        F.first(F.date_format("crash_timestamp", "EEEE")).alias("sample_day_of_week")
    )
).cache() # OPTIMIZATION 3a: Cache the aggregated result

# =========================
# 7. SAFETY RANKING & RISK ANALYSIS
# =========================

# OPTIMIZATION 3b: Calculate scalar value efficiently instead of using crossJoin
total_city_crashes_val = crash_base.select(F.sum("total_crashes")).collect()[0][0] or 1

window_spec = Window.partitionBy("borough").orderBy(F.desc("total_crashes"))

gold_calculated = (crash_base
    # Use the scalar value directly (F.lit) instead of crossJoin
    .withColumn("crash_pct", (F.col("total_crashes") / F.lit(total_city_crashes_val)) * 100)
    .withColumn("danger_rank", F.rank().over(window_spec))
    .withColumn("relative_rank", F.percent_rank().over(window_spec))
    .withColumn("safety_label", 
        F.when(F.col("relative_rank") <= 0.1, "🔴 High Danger (Top 10%)")
        .when(F.col("relative_rank") <= 0.3, "🟡 Moderate (Top 30%)")
        .otherwise("🟢 Relatively Safe"))
    .withColumn("last_updated", F.current_timestamp())
    .select(
        "street_name", "borough", "total_crashes", "total_injured", "total_killed",
        F.coalesce(F.col("main_cause"), F.lit("Unknown")).alias("main_cause"),
        F.col("crash_pct").cast("double"),
        "danger_rank", "safety_label", "last_updated",
        "latitude", "longitude",
        "first_crash_date", 
        "last_crash_date", 
        "unique_crash_days", 
        "sample_day_of_week"
    )
)

# =========================
# 8. DATA CONTRACT & SCHEMA ENFORCEMENT
# =========================

final_columns = [F.col(field.name).cast(field.dataType) for field in gold_crashes_schema]
gold_final = gold_calculated.select(*final_columns)

gold_with_geo = gold_final.withColumn(
    "location",
    F.when(
        F.col("latitude").isNotNull() & F.col("longitude").isNotNull() & (F.col("latitude") != 0.0),
        F.concat_ws(",", F.col("latitude").cast("string"), F.col("longitude").cast("string"))
    ).otherwise(None)
)

final_columns_with_geo = [F.col(field.name).cast(field.dataType) for field in gold_crashes_schema] + [F.col("location")]
gold_final_to_es = gold_with_geo.select(*final_columns_with_geo)

# =========================
# 9. MULTI-TARGET EXPORT (Postgres & MinIO)
# =========================

jdbc_url = "jdbc:postgresql://postgres:5432/nyc_data" 
db_props = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

try:
    print("Exporting Gold to Postgres and MinIO...")
    gold_final.write.jdbc(url=jdbc_url, table="gold_crash_stats", mode="overwrite", properties=db_props)
    
    # OPTIMIZATION 4: Add partitionBy for efficient query reading
    gold_final.write.mode("overwrite").partitionBy("borough").parquet("s3a://spark/gold/crashes/")
    
    print(" Pipeline Completed Successfully!")
    gold_final.show(10, truncate=False)
except Exception as e:
    print(f"❌ Export failed: {e}")
    


# ==================================================
# 10. SYNC TO ELASTICSEARCH 
# ==================================================

try:
    print("💎 Syncing Crashes Gold to Elasticsearch (Overwrite Mode)...")
    
    (gold_final_to_es.write
        .format("org.elasticsearch.spark.sql")
        .option("es.nodes", "elasticsearch") 
        .option("es.port", "9200")
        .option("es.nodes.wan.only", "true")
        .option("es.nodes.discovery", "false")  # Added to prevent handshake/discovery errors
        .option("es.resource", "nyc_crashes_gold") 
        
        # Stability settings for optimal performance
        .option("es.batch.size.entries", "1000")
        .option("es.http.timeout", "2m")
        .option("es.http.retries", "5")
        
        # Full sync - delete and recreate the data
        .mode("overwrite")
        
        # Use unique doc_id (e.g., Borough + Street) to prevent duplicates
        # .option("es.mapping.id", "doc_id") 
        .save())
        
    print("✅ Success! Data is in Elasticsearch with Location field.")

except Exception as e:
    print(f"❌ Elasticsearch Write Error: {e}")