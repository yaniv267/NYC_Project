from pyspark.sql import SparkSession
# הוספתי כאן את add_months ו-current_date לרשימה
from pyspark.sql.functions import col, count, current_timestamp, sum as _sum, add_months, current_date
from nyc_schema import gold_camera_violation_schema
# 1. אתחול מהיר של ספארק
spark =( SparkSession.builder \
.appName("NYC_Camera_Violation_Gold") \
.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") 
.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") 
.config("spark.hadoop.fs.s3a.access.key", "minioadmin") 
.config("spark.hadoop.fs.s3a.secret.key", "minioadmin") 
.config("spark.hadoop.fs.s3a.path.style.access", "true") 
.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
.getOrCreate())

print("🚀 Starting Gold Layer Processing...")

# 2. הגדרות לוגיות
CAMERA_CODES = [7, 12, 36]
target_table = "gold_traffic_cameras"

# 3. חישוב תאריך הסף (חודש אחד אחורה)
cutoff_date_df = spark.range(1).select(add_months(current_date(), -12).alias("date"))
cutoff_date = cutoff_date_df.collect()[0]["date"]
print(f"📅 Filtering for data since: {cutoff_date}")

# 4. טעינת נתונים
codes_df = spark.read.json("s3a://spark/reference/violation_codes.json") \
    .select(
        col("code").cast("int").alias("violation_code"), 
        col("definition").alias("raw_desc")
    )

print("📖 Reading Partitioned Silver Data...")
silver_df = spark.read.parquet("s3a://spark/silver/parking_violations/") \
    .filter(col("issue_date") >= cutoff_date)

# 5. עיבוד: Join וסינון לפי קודי מצלמות
processed_df = (silver_df
    .join(codes_df, "violation_code", "inner")
    .filter(col("violation_code").isin(CAMERA_CODES))
    .groupBy("street_name", "raw_desc")
    .agg(count("summons_number").alias("tickets_count"))
)

# 6. החלת הסכימה הסופית
master_gold = (processed_df
    .withColumn("last_updated", current_timestamp())
    .select(
        col("street_name").cast("string"),
        col("raw_desc").alias("violation_description").cast("string"),
        col("tickets_count").cast("long"),
        col("last_updated").cast("timestamp")
    )
)

# 7. סטטיסטיקה להדפסה
total_res = master_gold.agg(_sum("tickets_count")).collect()[0][0]
total_observations = total_res if total_res is not None else 0

print("\n" + "="*40)
print(f"📊 LAST MONTH SUMMARY:")
print(f"✅ Total Camera Violations (Since {cutoff_date}): {total_observations:,}")
print(f"📍 Unique Streets with Activity: {master_gold.count():,}")
print("="*40 + "\n")

# 8. שמירה ל-PostgreSQL
jdbc_url = "jdbc:postgresql://postgres:5432/nyc_data" 
db_props = {"user": "user", "password": "password", "driver": "org.postgresql.Driver"}

try:
    print(f"💎 Syncing to Postgres (table: {target_table})...")
    master_gold.write.jdbc(url=jdbc_url, table=target_table, mode="overwrite", properties=db_props)
    print("✅ Export Success!")
except Exception as e:
    print(f"❌ DB Export failed: {e}")

master_gold.orderBy(col("tickets_count").desc()).show(10, truncate=False)