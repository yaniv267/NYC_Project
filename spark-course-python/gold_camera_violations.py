# from pyspark.sql import SparkSession
# # הוספתי כאן את add_months ו-current_date לרשימה
# from pyspark.sql.functions import col, count, current_timestamp, sum as _sum, add_months, current_date
# from nyc_schema import gold_camera_violation_schema
# # 1. אתחול מהיר של ספארק
# spark =( SparkSession.builder \
# .appName("NYC_Camera_Violation_Gold") \
# .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") 
# .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") 
# .config("spark.hadoop.fs.s3a.access.key", "minioadmin") 
# .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") 
# .config("spark.hadoop.fs.s3a.path.style.access", "true") 
# .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
# .getOrCreate())

# print("🚀 Starting Gold Layer Processing...")

# # 2. הגדרות לוגיות
# CAMERA_CODES = [7, 12, 36]
# target_table = "gold_traffic_cameras"

# # 3. חישוב תאריך הסף (חודש אחד אחורה)
# cutoff_date_df = spark.range(1).select(add_months(current_date(), -12).alias("date"))
# cutoff_date = cutoff_date_df.collect()[0]["date"]
# print(f"📅 Filtering for data since: {cutoff_date}")

# # 4. טעינת נתונים
# codes_df = spark.read.json("s3a://spark/reference/violation_codes.json") \
#     .select(
#         col("code").cast("int").alias("violation_code"), 
#         col("definition").alias("raw_desc")
#     )

# print("📖 Reading Partitioned Silver Data...")
# silver_df = spark.read.parquet("s3a://spark/silver/parking_violations/") \
#     .filter(col("issue_date") >= cutoff_date)

# # 5. עיבוד: Join וסינון לפי קודי מצלמות
# processed_df = (silver_df
#     .join(codes_df, "violation_code", "inner")
#     .filter(col("violation_code").isin(CAMERA_CODES))
#     .groupBy("street_name", "raw_desc")
#     .agg(count("summons_number").alias("tickets_count"))
# )

# # 6. החלת הסכימה הסופית
# master_gold = (processed_df
#     .withColumn("last_updated", current_timestamp())
#     .select(
#         col("street_name").cast("string"),
#         col("raw_desc").alias("violation_description").cast("string"),
#         col("tickets_count").cast("long"),
#         col("last_updated").cast("timestamp")
#     )
# )

# # 7. סטטיסטיקה להדפסה
# total_res = master_gold.agg(_sum("tickets_count")).collect()[0][0]
# total_observations = total_res if total_res is not None else 0

# print("\n" + "="*40)
# print(f"📊 LAST MONTH SUMMARY:")
# print(f"✅ Total Camera Violations (Since {cutoff_date}): {total_observations:,}")
# print(f"📍 Unique Streets with Activity: {master_gold.count():,}")
# print("="*40 + "\n")

# # 8. שמירה ל-PostgreSQL
# jdbc_url = "jdbc:postgresql://postgres:5432/nyc_data" 
# db_props = {"user": "user", "password": "password", "driver": "org.postgresql.Driver"}

# try:
#     print(f"💎 Syncing to Postgres (table: {target_table})...")
#     master_gold.write.jdbc(url=jdbc_url, table=target_table, mode="overwrite", properties=db_props)
#     print("✅ Export Success!")
# except Exception as e:
#     print(f"❌ DB Export failed: {e}")

# master_gold.orderBy(col("tickets_count").desc()).show(10, truncate=False)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, current_timestamp, sum as _sum, add_months, current_date, \
    split, regexp_replace, trim, upper, avg

# 1. אתחול ספארק
spark = (SparkSession.builder \
    .appName("NYC_Gold_Camera_With_Coords") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate())

# פונקציית נרמול לשיפור אחוז ההתאמה
def normalize_street_logic(c):
    # מתחילים מהעמודה המקורית (מנקים רווחים ואותיות גדולות)
    c = upper(trim(c))
    # מבצעים החלפות רגקס אחת אחרי השנייה
    c = regexp_replace(c, r"^(NB|SB|EB|WB|N/B|S/B|E/B|W/B)\s+", "")
    c = regexp_replace(c, r"(\d+)(ST|ND|RD|TH)", r"$1")
    c = regexp_replace(c, r"\bAVE\b", "AVE")
    c = regexp_replace(c, r"\bST\b", "ST")
    c = regexp_replace(c, r"\bBLVD\b|\bBLV\b", "BLVD")
    c = regexp_replace(c, r"\bTPKE\b", "TURNPIKE")
    return c

print("🚀 Starting Enriched Gold Layer Processing...")

# 2. הגדרות
CAMERA_CODES = [7, 12, 36]
target_table = "gold_traffic_cameras"

# 3. טעינת מימדים (Addresses & Codes)
print("📖 Loading Reference Data...")
# מילון קודים
codes_df = spark.read.json("s3a://spark/reference/violation_codes.json") \
    .select(col("code").cast("int").alias("violation_code"), col("definition").alias("raw_desc"))

# קובץ כתובות - נרמול וביצוע ממוצע לקואורדינטות למניעת כפילויות
address_coords = spark.read.parquet("s3a://spark/reference/addresses/") \
    .withColumn("norm_addr_street", normalize_street_logic(col("street_name_clean"))) \
    .groupBy("norm_addr_street") \
    .agg(avg("latitude").alias("lat"), avg("longitude").alias("lon"))

# 4. טעינת נתוני סילבר (חודשים אחרונים)
cutoff_date = spark.range(1).select(add_months(current_date(), -12).alias("date")).collect()[0]["date"]
silver_df = spark.read.parquet("s3a://spark/silver/parking_violations/") \
    .filter(col("issue_date") >= cutoff_date)

# 5. עיבוד ואיחוד (Join)
print("🧠 Processing and Joining Coordinates...")

# א. סינון מצלמות ואגרגציה
violations_base = (silver_df
    .join(codes_df, "violation_code", "inner")
    .filter(col("violation_code").isin(CAMERA_CODES))
    .groupBy("street_name", "raw_desc")
    .agg(count("summons_number").alias("tickets_count"))
)

# ב. נרמול שם הרחוב מהדוחות (לפני ה-@) לצורך ה-Join
violations_with_norm = violations_base.withColumn(
    "norm_viol_street", normalize_street_logic(split(col("street_name"), "@")[0])
)

# ג. Join סופי עם הקואורדינטות
final_gold = (violations_with_norm
    .join(address_coords, violations_with_norm.norm_viol_street == address_coords.norm_addr_street, "left")
    .withColumn("last_updated", current_timestamp())
    .select(
        col("street_name"),
        col("raw_desc").alias("violation_description"),
        col("tickets_count"),
        col("lat"),
        col("lon"),
        col("last_updated")
    )
)

# 6. בדיקת איכות נתונים (Audit)
total_streets = final_gold.count()
matched_streets = final_gold.filter(col("lat").isNotNull()).count()
print(f"\n✅ Matching Finish: {matched_streets}/{total_streets} streets found coordinates ({(matched_streets/total_streets)*100:.2f}%)")

# 7. שמירה ל-Postgres
jdbc_url = "jdbc:postgresql://postgres:5432/nyc_data" 
db_props = {"user": "user", "password": "password", "driver": "org.postgresql.Driver"}

try:
    print(f"💎 Syncing Enriched Gold to Postgres...")
    final_gold.write.jdbc(url=jdbc_url, table=target_table, mode="overwrite", properties=db_props)
    print("✅ Export Success! Data is ready for Telegram Bot.")
except Exception as e:
    print(f"❌ DB Export failed: {e}")

final_gold.orderBy(col("tickets_count").desc()).show(10, truncate=False)