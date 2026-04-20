import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from nyc_schema  import silver_violation_codes_schema

os.environ["PYSPARK_SUBMIT_ARGS"] = "--driver-memory 4g --executor-memory 4g pyspark-shell"

# =========================
# 1. SPARK SESSION
# =========================

spark = (SparkSession.builder \
    .appName("nyc_gold_traffic_violations") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0,org.elasticsearch:elasticsearch-spark-30_2.12:7.13.2") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate())

# ==========================================
# 2. STREET NORMALIZATION ENGINE (JOIN KEY)
# ==========================================

def normalize_street_expert(col_name):
    """
    Cleans and standardizes street names to improve Join match rates.
    Handles prefixes (Directionals), Suffixes, and Numeric formats.
    """
    c = F.upper(F.trim(F.col(col_name)))
    
    # Step A: Remove Directional prefixes (WB, EB, NB, SB, West, etc.) and intersections
    c = F.regexp_replace(c, r"^(WB|EB|NB|SB|W\s|E\s|N\s|S\s|WEST\s|EAST\s|NORTH\s|SOUTH\s)", "")
    c = F.regexp_replace(c, r"\s*@.*$", "") # מסיר כל מה שאחרי @ (צמתים)
    
    # Step B: Standardize Suffixes to common abbreviations (ST, AVE, RD)
    replacements = {
        r"\bAVENUE\b": "AVE", r"\bSTREET\b": "ST", r"\bROAD\b": "RD",
        r"\bPLACE\b": "PL", r"\bBOULEVARD\b": "BLVD", r"\bPARKWAY\b": "PKWY",
        r"\bEXPRESSWAY\b": "EXPY", r"\bTERRACE\b": "TER", r"\bCOURT\b": "CT"
    }
    for full, short in replacements.items():
        c = F.regexp_replace(c, full, short)
        
    # Step C: Normalize Numeric Streets (e.g., 3RD -> 3)
    c = F.regexp_replace(c, r"(\d+)(ST|ND|RD|TH)", r"$1")
    
    # Step D: Final cleanup of special characters and redundant spaces
    c = F.regexp_replace(c, r"[^A-Z0-9\s]", "")
    c = F.regexp_replace(c, r"\s+", " ")
    
    return F.trim(c)

print("🚀 Starting Unified Gold Pipeline...")

# ==========================================
# 3. LOAD SILVER LAYERS FROM MINIO
# ==========================================
print("📥 Loading Silver layers from MinIO...")
addresses_df = spark.read.parquet("s3a://spark/silver/addresses/")
tickets_df = spark.read.parquet("s3a://spark/silver/nyc_parking_violation/")
codes_df = spark.read.schema(silver_violation_codes_schema).parquet("s3a://spark/silver/violation_codes")

# ==========================================
# 4. PREPARE DIMENSION TABLES (Addresses & Codes)
# ==========================================
# Normalize addresses and calculate average coordinates per street/borough
addresses_clean = addresses_df.withColumn("street_key", normalize_street_expert("street_name")) \
    .groupBy("street_key", "borough_code").agg(
        F.avg("latitude").alias("lat"), 
        F.avg("longitude").alias("lon")
    )
    
# Prepare violation codes with fine amounts for different zones
codes_clean = codes_df.withColumn("violation_code_int", F.col("violation_code").cast("integer")) \
    .select("violation_code_int", "violation_description")

codes_clean = codes_df.withColumn("violation_code_int", F.col("violation_code").cast("integer")) \
    .select(
        "violation_code_int", 
        "violation_description", 
        "manhattan_96th_st_below",  # הבאת קנס מנהטן
        "all_other_areas"           # הבאת קנס שאר האזורים
    )
    
# ==========================================
# 5. PREPARE FACT DATA (Tickets) & CAMERA DETECTION
# ==========================================

tickets_prepared = tickets_df.withColumn("borough_code", 
    F.when(F.col("violation_county").isin("NY", "MN", "NEW Y"), "1")
     .when(F.col("violation_county").isin("BX", "BRONX"), "2")
     .when(F.col("violation_county").isin("K", "BK", "KINGS"), "3")
     .when(F.col("violation_county").isin("Q", "QN", "QUEENS"), "4")
     .when(F.col("violation_county").isin("R", "ST", "RICHM"), "5").otherwise(None)
).withColumn("street_key", normalize_street_expert("street_name")) \
 .withColumn("violation_code_int", F.col("violation_code").cast("integer")) \
 .withColumn("is_camera", F.when(F.col("violation_code_int").isin(7, 12, 36), "Yes").otherwise("No")) \
 .drop("violation_description") # <=== התיקון: מחיקת העמודה הכפולה לפני ה-Join

# ==========================================
# 6. DATA ENRICHMENT & MATCH RATE ANALYSIS
# ==========================================
print("🔗 Joining datasets and analyzing quality...")

# Calculate Match Rate for Data Quality reporting
match_check = tickets_prepared.join(addresses_clean, ["street_key", "borough_code"], "left")
total_count = tickets_prepared.count()
matched_count = match_check.filter(F.col("lat").isNotNull()).count()

print("\n" + "="*50)
print(f"📊 DATA QUALITY REPORT")
print(f"🎯 FINAL MATCH RATE: {(matched_count/total_count)*100:.2f}%")
print(f"✅ Records mapped: {matched_count:,} / {total_count:,}")
print("="*50 + "\n")

# Combine Fact table with Geo-Dimensions and Violation-Metadata
gold_base = tickets_prepared.join(addresses_clean, ["street_key", "borough_code"], "left") \
                            .join(codes_clean, ["violation_code_int"], "left")

# ==========================================
# 7. DATA AGGREGATION & BUSINESS LOGIC
# ==========================================
# Step A: Primary aggregation by street, violation type, and time

gold_agg = gold_base.groupBy(
    "street_name", "violation_code", "violation_description", "borough_code", "lat", "lon", "hour", "is_camera"
).agg(
    F.count("summons_number").alias("tickets_count"),
    F.min("issue_date").alias("start_date"),
    F.max("issue_date").alias("end_date"),
    F.max("issue_timestamp").alias("last_violation_ts"),
    F.sum(F.when(F.col("borough_code") == "1", F.col("manhattan_96th_st_below")).otherwise(F.col("all_other_areas"))).alias("total_fines")
)

# Step B: Windowing to compare ticket counts against street averages
street_window = Window.partitionBy("street_name")

# Step C: Final Field formatting, Risk Level logic, and Time Categorie
gold_final = gold_agg.withColumn(
    "avg_tickets_for_street", F.avg("tickets_count").over(street_window)
    ).select(
    "street_name",
    F.col("violation_code").cast("integer").alias("violation_code"),
    F.coalesce(F.col("violation_description"), F.lit("Unknown Violation")).alias("violation_desc"),
    F.col("tickets_count").cast("long"),
    "total_fines",
    "start_date",
    "end_date",
    F.col("end_date").alias("issue_date"),
    F.date_format(F.col("end_date"), "EEEE").alias("issue_day_name"),
    "lat", "lon",
    F.when(F.col("violation_code").cast("int") < 20, "Parking").otherwise("Moving").alias("category"),
    F.when(F.col("tickets_count") > (1.5 * F.col("avg_tickets_for_street")), "High Risk")
     .otherwise("Low Risk").alias("risk_level"),
    F.when(F.col("borough_code") == "1", "Manhattan")
     .when(F.col("borough_code") == "2", "Bronx")
     .when(F.col("borough_code") == "3", "Brooklyn")
     .when(F.col("borough_code") == "4", "Queens")
     .when(F.col("borough_code") == "5", "Staten Island").alias("borough"),
    F.current_timestamp().alias("last_updated"),
    "hour",
    "last_violation_ts",
    "is_camera",
    # Define Time of Day categories
    F.when((F.col("hour") >= 6) & (F.col("hour") < 12), "Morning")
     .when((F.col("hour") >= 12) & (F.col("hour") < 17), "Afternoon")
     .when((F.col("hour") >= 17) & (F.col("hour") < 21), "Evening")
     .otherwise("Night").alias("time_of_day"),
    F.when(
    F.col("lat").isNotNull() & F.col("lon").isNotNull() & (F.col("lat") != 0.0),
    F.concat_ws(",", F.col("lat").cast("string"), F.col("lon").cast("string"))
    ).otherwise(None).alias("location")
    
    
    )
# ==========================================
# 8. MULTI-TARGET EXPORT (MinIO & Postgres)
# ==========================================
gold_final = gold_final.coalesce(4).cache()
final_count = gold_final.count()
print(f"✅ Created Gold layer with {final_count} rows.")


# Target A: Parquet Files in MinIO (Object Storage)
if final_count > 0:
  
    print("💾 Saving Gold Parquet to MinIO...")
    gold_final.write.mode("overwrite").parquet("s3a://spark/gold/parking_violations_new/")
    
  # Target B: PostgreSQL Database (for Application/Bot usage)
    jdbc_url = "jdbc:postgresql://postgres:5432/nyc_data"
    db_props = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}
    
    try:
        print("💎 Syncing to Postgres...")
        gold_final.write.jdbc(url=jdbc_url, table="gold_traffic_violations", mode="overwrite", properties=db_props)
        print("🚀 DONE! Everything is ready.")
    except Exception as e:
        print(f"❌ Postgres Error: {e}")
        
# ==========================================
# 9. OPTIONAL: SYNC TO ELASTICSEARCH
# ==========================================
# This section is currently commented out but ready for production use.
# It handles high-performance indexing into Elasticsearch for Kibana dashboards.
# try:
#     print("💎 Syncing to Elasticsearch...")
#     (gold_final
#         .write
#         .format("org.elasticsearch.spark.sql")
#         .option("es.nodes", "elasticsearch") 
#         .option("es.port", "9200")
#         .option("es.nodes.wan.only", "true")
#         .option("es.resource", "nyc_parking_gold") 
#
#         # Performance Tuning to prevent CPU bottlenecking:
#         .option("es.batch.size.entries", "500") # Sends 500 rows per batch instead of thousands
#         .option("es.http.timeout", "1m")        # Gives Elasticsearch 1 minute to respond
#         .option("es.http.retries", "3")         # Retry mechanism for heavy loads
#
#         .mode("overwrite") 
#         .save())
#     print("✅ Success! Data is in Elasticsearch.")
# except Exception as e:
#     print(f"❌ Elasticsearch Write Error: {e}")

# ==========================================
# 10. TERMINATE SPARK SESSION
# ==========================================
spark.stop()