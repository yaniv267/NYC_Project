from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.common.nyc_schema import gold_311_schema

# =========================
# 2. INITIALIZE SPARK SESSION
# =========================

spark = (SparkSession.builder 
    .appName("NYC_311_Gold_Analytics") 
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") 
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

print("📖 Reading 311 Silver data...")
# OPTIMIZATION 1: Cache the DataFrame because it branches into two separate aggregations below
silver_df = spark.read.parquet("s3a://spark/silver/311_complaints/").cache()

# ==========================================
# 4. ANALYTICS: PEAK HOUR CALCULATION
# ==========================================

peak_hour_df = (
    silver_df
    .groupBy("borough", "complaint_type", "street_name", "hour")
    .count()
    .withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("borough", "complaint_type", "street_name")
            .orderBy(F.desc("count"))
        )
    )
    .filter(F.col("rn") == 1)
    .select(
        "borough",
        "complaint_type",
        "street_name",
        F.col("hour").alias("peak_hour")
    )
)

# ==========================================
# 5. CORE AGGREGATION & METRICS
# ==========================================

print("📊 Aggregating complaints metrics...")

gold_base = (
    silver_df
    # OPTIMIZATION 2: Removed year/month/day_name from groupBy. 
  
    .groupBy(
        "borough",
        "complaint_type",
        "street_name"
    )
    .agg(
        F.count("unique_key").alias("complaint_count"),

        # 📍 location (representative point)
        F.first("latitude", ignorenulls=True).alias("latitude"),
        F.first("longitude", ignorenulls=True).alias("longitude"),

        # ⏱ time range
        F.min("created_date").alias("first_complaint_date"),
        F.max("created_date").alias("last_complaint_date")
    )
)

# ==========================================
# 6. ENRICHMENT & SCHEMA ENFORCEMENT
# ==========================================

final_gold_calculated = (
    gold_base
    .join(
        peak_hour_df,
        ["borough", "complaint_type", "street_name"],
        "left"
    )
    .withColumn(
        "intensity_level",
        F.when(F.col("complaint_count") > 100, "🔴 HIGH")
         .when(F.col("complaint_count") > 50, "🟠 MEDIUM")
         .otherwise("🟢 NORMAL")
    )
    .withColumn("last_updated_at", F.current_timestamp())
)

# --- Data Contract Alignment ---
print("🛡️ Enforcing Gold Schema...")

for f in gold_311_schema:
    if f.name not in final_gold_calculated.columns:
        final_gold_calculated = final_gold_calculated.withColumn(f.name, F.lit(None))

gold_final = final_gold_calculated.select(
    *[F.col(f.name).cast(f.dataType) for f in gold_311_schema]
)

# ==========================================
# 7. MULTI-TARGET EXPORT (S3 & POSTGRES)
# ==========================================
print("🚀 Exporting Gold to targets...")

# Target 1: MinIO (Parquet)
# OPTIMIZATION 3: Partitioning by borough for efficient querying by UI/Bot
gold_final.write.mode("overwrite").partitionBy("borough").parquet("s3a://spark/gold/311_complaints/")

# Target 2: PostgreSQL (For Telegram Bot and Web Consumption)
jdbc_url = "jdbc:postgresql://postgres:5432/nyc_data"
db_props = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

try:
    gold_final.write.jdbc(
        url=jdbc_url, 
        table="gold_311_stats", 
        mode="overwrite", 
        properties=db_props
    )
    print("\n" + "="*50)
    print("✅ GOLD 311 PIPELINE COMPLETED SUCCESSFULLY!")
    print("="*50 + "\n")
except Exception as e:
    print(f"❌ Postgres Export failed: {e}")

spark.stop()