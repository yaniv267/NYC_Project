from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from Nyc_Project.src.common.nyc_schema import gold_traffic_realtime_schema, gold_traffic_analytics_schema

# =========================
# 2. INITIALIZE SPARK SESSION
# =========================


packages = [
    "org.elasticsearch:elasticsearch-spark-30_2.12:7.13.2",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.postgresql:postgresql:42.6.0"
]

spark = (SparkSession.builder
    .appName("NYC_Gold_Traffic_Pipeline")
    .config("spark.jars.packages", ",".join(packages)) # כאן קורה הקסם
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# ==========================================
# 3. LOAD SILVER DATASET
# ==========================================

print("📖 Reading Silver Traffic Data...")
df_traffic = spark.read.parquet("s3a://spark/silver/nyc_traffic/")

# OPTIMIZATION 1: Cache the enriched dataframe so the new column is materialized in RAM
enriched_traffic = df_traffic.withColumn(
    "street_name",
    F.coalesce(F.col("link_name"), F.col("link_id"))
).cache()

# ==========================================
# 4. REAL-TIME VIEW: LAST STATUS PER LINK
# ==========================================
# Last status for each Link
window_latest = Window.partitionBy("link_id").orderBy(F.desc("event_time"))
df_realtime = (enriched_traffic
    .withColumn("rn", F.row_number().over(window_latest))
    .filter(F.col("rn") == 1)
    .select(
        "link_id",
        "street_name",
        "borough",
        F.col("latitude").cast("double"),
        F.col("longitude").cast("double"),
        F.col("speed").alias("current_speed"),
        "travel_time",
        F.when(F.col("speed") < 15, "🔴 Heavy Traffic")
         .when(F.col("speed") < 30, "🟠 Moderate Flow")
         .otherwise("🟢 Clear").alias("traffic_status"),
        "event_time",
        F.current_timestamp().alias("last_updated")
    )
)
# Enforce Gold Real-time schema contract
df_realtime_final = df_realtime.select(*[F.col(f.name).cast(f.dataType) for f in gold_traffic_realtime_schema])

# ==========================================
# 5. ANALYTICS VIEW: CONGESTION & RELIABILITY
# ==========================================

# ANALYTICS
# Peak hour for each link
peak_hour = (enriched_traffic
    .groupBy("link_id", "day_name", "hour")
    .agg(F.avg("speed").alias("avg_speed_h"))
    .withColumn("rank", F.row_number().over(Window.partitionBy("link_id", "day_name").orderBy(F.asc("avg_speed_h"))))
    .filter(F.col("rank") == 1)
    .select("link_id", "day_name", F.col("hour").alias("peak_congestion_hour"))
)

# Aggregate historical data for long-term reliability metrics
df_analytics = (enriched_traffic
    .groupBy("link_id", "street_name", "borough", "day_name", "is_weekend")
    .agg(
        F.avg("speed").alias("avg_speed_daily"),
        F.count("*").alias("total_readings")
    )
    # OPTIMIZATION 2: Broadcast Join - peak_hour is tiny (1 row per link per day)
    .join(F.broadcast(peak_hour), ["link_id", "day_name"], "left")
    .withColumn("reliability_score",
        F.when(F.col("avg_speed_daily") > 40, "🟢 Highly Reliable")
         .when(F.col("avg_speed_daily") > 25, "🟡 Average")
         .otherwise("🔴 Usually Congested"))
    .withColumn("last_updated", F.current_timestamp())
)
# Enforce Gold Analytics schema contract
df_analytics_final = df_analytics.select(*[F.col(f.name).cast(f.dataType) for f in gold_traffic_analytics_schema])

# ==========================================
# 6. MULTI-TARGET EXPORT (POSTGRES & MINIO)
# ==========================================

PG_USER = 'postgres'
PG_PASSWORD = 'postgres'
PG_HOST = 'postgres'
PG_PORT = '5432'
PG_DB = 'nyc_data'

jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
db_props = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Target A: Syncing Gold views to PostgreSQL for Telegram Bot and Applications
try:
    print(f"💎 Syncing to Postgres (DB: {PG_DB}) for Telegram Bot...")
    df_realtime_final.write.jdbc(jdbc_url, "gold_traffic_realtime", "overwrite", db_props)
    df_analytics_final.write.jdbc(jdbc_url, "gold_traffic_analytics", "overwrite", db_props)
    print("✅ Postgres Sync Complete!")
except Exception as e:
    print(f"❌ Postgres Error: {e}")

# Target B: Archiving Gold Parquet files to MinIO for long-term storage
try:
    print("Saving Parquet to MinIO...")
    # OPTIMIZATION 3: Partition by borough for the Serving Layer queries
    df_realtime_final.write.mode("overwrite").partitionBy("borough").parquet("s3a://spark/gold/traffic_realtime/")
    df_analytics_final.write.mode("overwrite").partitionBy("borough").parquet("s3a://spark/gold/traffic_analytics/")
    print("MinIO Save Complete")
except Exception as e:
    print(f"❌ MinIO Error: {e}")

# ==========================================
# 7. FINALIZATION
# ==========================================

print("\n" + "="*50)
print("🚀 GOLD TRAFFIC MASTER PIPELINE FINISHED SUCCESSFULLY")
print("="*50)

# ==================================================
# 10. SYNC TO ELASTICSEARCH 
# ==================================================
try:
    print("💎 Syncing Traffic Gold to Elasticsearch (Overwrite Mode)...")
    
    (df_analytics_final.write
        .format("org.elasticsearch.spark.sql")
        .option("es.nodes", "elasticsearch") 
        .option("es.port", "9200")
        .option("es.nodes.wan.only", "true")
        .option("es.nodes.discovery", "false") # Prevents connection errors in Docker
        .option("es.resource", "nyc_traffic_gold") 
        
        # Stability settings for optimal performance
        .option("es.batch.size.entries", "1000")
        .option("es.http.timeout", "2m")
        .option("es.http.retries", "5")
        
        # Full synchronization - delete and recreate the data
        .mode("overwrite")
        
        # Optional: Define a unique ID to prevent duplicates in the future
        # .option("es.mapping.id", "link_id") 
        .save())
        
    print("✅ Success! Traffic Data is in Elasticsearch.")

except Exception as e:
    print(f"❌ Elasticsearch Write Error: {e}")

spark.stop()
print("🚀 Gold Traffic Master Pipeline Finished.")