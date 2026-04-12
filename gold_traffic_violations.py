# # from pyspark.sql import SparkSession
# # # הוספתי כאן את add_months ו-current_date לרשימה
# # from pyspark.sql.functions import col, count, current_timestamp, sum as _sum, add_months, current_date
# # from nyc_schema import gold_camera_violation_schema
# # # 1. אתחול מהיר של ספארק
# # spark =( SparkSession.builder \
# # .appName("NYC_Camera_Violation_Gold") \
# # .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") 
# # .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") 
# # .config("spark.hadoop.fs.s3a.access.key", "minioadmin") 
# # .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") 
# # .config("spark.hadoop.fs.s3a.path.style.access", "true") 
# # .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
# # .getOrCreate())

# # print("🚀 Starting Gold Layer Processing...")

# # # 2. הגדרות לוגיות
# # CAMERA_CODES = [7, 12, 36]
# # target_table = "gold_traffic_cameras"

# # # 3. חישוב תאריך הסף (חודש אחד אחורה)
# # cutoff_date_df = spark.range(1).select(add_months(current_date(), -12).alias("date"))
# # cutoff_date = cutoff_date_df.collect()[0]["date"]
# # print(f"📅 Filtering for data since: {cutoff_date}")

# # # 4. טעינת נתונים
# # codes_df = spark.read.json("s3a://spark/reference/violation_codes.json") \
# #     .select(
# #         col("code").cast("int").alias("violation_code"), 
# #         col("definition").alias("raw_desc")
# #     )

# # print("📖 Reading Partitioned Silver Data...")
# # silver_df = spark.read.parquet("s3a://spark/silver/parking_violations/") \
# #     .filter(col("issue_date") >= cutoff_date)

# # # 5. עיבוד: Join וסינון לפי קודי מצלמות
# # processed_df = (silver_df
# #     .join(codes_df, "violation_code", "inner")
# #     .filter(col("violation_code").isin(CAMERA_CODES))
# #     .groupBy("street_name", "raw_desc")
# #     .agg(count("summons_number").alias("tickets_count"))
# # )

# # # 6. החלת הסכימה הסופית
# # master_gold = (processed_df
# #     .withColumn("last_updated", current_timestamp())
# #     .select(
# #         col("street_name").cast("string"),
# #         col("raw_desc").alias("violation_description").cast("string"),
# #         col("tickets_count").cast("long"),
# #         col("last_updated").cast("timestamp")
# #     )
# # )

# # # 7. סטטיסטיקה להדפסה
# # total_res = master_gold.agg(_sum("tickets_count")).collect()[0][0]
# # total_observations = total_res if total_res is not None else 0

# # print("\n" + "="*40)
# # print(f"📊 LAST MONTH SUMMARY:")
# # print(f"✅ Total Camera Violations (Since {cutoff_date}): {total_observations:,}")
# # print(f"📍 Unique Streets with Activity: {master_gold.count():,}")
# # print("="*40 + "\n")

# # # 8. שמירה ל-PostgreSQL
# # jdbc_url = "jdbc:postgresql://postgres:5432/nyc_data" 
# # db_props = {"user": "user", "password": "password", "driver": "org.postgresql.Driver"}

# # try:
# #     print(f"💎 Syncing to Postgres (table: {target_table})...")
# #     master_gold.write.jdbc(url=jdbc_url, table=target_table, mode="overwrite", properties=db_props)
# #     print("✅ Export Success!")
# # except Exception as e:
# #     print(f"❌ DB Export failed: {e}")

# # master_gold.orderBy(col("tickets_count").desc()).show(10, truncate=False)

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     col, count, current_timestamp, avg, split, regexp_replace, 
#     trim, upper, broadcast, when, concat, coalesce, lit
# )

# # 1. אתחול ספארק
# spark = (SparkSession.builder \
#     .appName("NYC_Gold_Camera_With_Coords") \
#     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") \
#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
#     .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .getOrCreate())



# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     col, count, current_timestamp, avg, regexp_replace, 
#     trim, upper, broadcast, when, concat, coalesce, lit,min, max
# )

# # 1. אתחול ספארק עם הגדרות MinIO ו-Postgres
# spark = (SparkSession.builder \
#     .appName("NYC_Gold_Unified_Final_Telegram") \
#     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") \
#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
#     .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .getOrCreate())

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
# (
#     col, count, avg, current_timestamp, split, regexp_replace, 
#     trim, upper, broadcast, when, concat, coalesce, lit,min, max
# )

# 1. אתחול ספארק
spark = (SparkSession.builder \
    .appName("nyc_gold_traffic_violations") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate())


def normalize_street_logic(c):
    c = F.upper(F.trim(c))
    c = F.regexp_replace(c, r"^(NB|SB|EB|WB|N/B|S/B|E/B|W/B)\s+", "")
    c = F.regexp_replace(c, r"(\d+)(ST|ND|RD|TH)", r"$1")
    c = F.regexp_replace(c, r"\bAVENUE\b", "AVE")
    c = F.regexp_replace(c, r"\bSTREET\b", "ST")
    c = F.regexp_replace(c, r"\bSTR\b", "ST")
    c = F.regexp_replace(c, r"\bBOULEVARD\b|\bBLV\b", "BLVD")
    c = F.regexp_replace(c, r"\bPARKWAY\b", "PKWY")
    c = F.regexp_replace(c, r"\s+", " ") 
    return c

print("🚀 Starting Unified Gold Pipeline...")


silver_df = spark.read.parquet("s3a://spark/silver/nyc_traffic_violations/").cache()
addresses_df = spark.read.parquet("s3a://spark/silver/addresses/")


lookup_codes = spark.read.parquet("s3a://spark/silver/violation_codes/") \
    .withColumn("violation_code", F.col("violation_code").cast("int"))

# 4. הכנת שכבת המפות (Address Master)
address_master = (addresses_df
    .withColumn("norm_addr", normalize_street_logic(F.col("street_name_clean")))
    .groupBy("norm_addr")
    .agg(F.avg("latitude").alias("lat"), F.avg("longitude").alias("lon"))
).cache()

# 5. יצירת טבלת ה-Gold המאוחדת
camera_codes = [7, 21, 36, 38]

gold_final = (silver_df
    # 1. הכנת הנתונים לפני הקיבוץ
    .withColumn("street_clean", F.split(F.col("street_name"), "@")[0])
    .withColumn("norm_viol", normalize_street_logic(F.col("street_clean")))
    .withColumn("issue_date_tmp", F.to_date(F.col("issue_date"))) # שיניתי שם זמני למניעת בלבול
    
    # 2. אגרגציה - כאן נוצרים start_date ו-end_date
    .groupBy("street_name", "norm_viol", "violation_code", "violation_county")
    .agg(
        F.count("summons_number").alias("tickets_count"),
        F.min("issue_date_tmp").alias("start_date"), 
        F.max("issue_date_tmp").alias("end_date")
    )
    
    # 3. יצירת יום בשבוע מתוך התאריך האחרון שנמצא
    .withColumn("issue_day_name", F.date_format(F.col("end_date"), "EEEE"))
    
    # 4. Joins (Lookup וקואורדינטות)
    .join(F.broadcast(lookup_codes), on="violation_code", how="left")
    .join(F.broadcast(address_master), F.col("norm_viol") == F.col("norm_addr"), "left")
    
    # 5. בחירה סופית - שים לב לשינוי ב-issue_date
    .select(
        F.col("street_name"),
        F.col("violation_code"),
        F.coalesce(F.col("violation_description"), F.lit("Unknown Violation")).alias("violation_desc"),
        F.col("tickets_count"),
        F.col("start_date"),
        F.col("end_date"),
        # כאן התיקון: אנחנו מציגים את ה-end_date בתור ה-issue_date הסופי
        F.col("end_date").alias("issue_date"),
        F.col("issue_day_name"),
        F.col("lat").cast("double"),
        F.col("lon").cast("double"),
        F.when(F.col("violation_code").isin(camera_codes), "Camera").otherwise("Parking").alias("category"),
        F.when(F.col("tickets_count") > 1000, "🔴 High Risk")
         .when(F.col("tickets_count") > 500, "🟠 Medium Risk")
         .otherwise("🟢 Low Risk").alias("risk_level"),
        F.when(F.col("violation_county").isin("K", "Kings"), "Brooklyn")
         .when(F.col("violation_county").isin("Q", "Queens"), "Queens")
         .when(F.col("violation_county").isin("NY", "Manhattan"), "Manhattan")
         .when(F.col("violation_county").isin("BX", "Bronx"), "Bronx")
         .otherwise("Other").alias("borough"),
        F.current_timestamp().alias("last_updated")
    )
)
# 7. בדיקת תוצאות
final_count = gold_final.count()
print(f"✅ Created Gold layer with {final_count} rows.")
gold_final.orderBy(F.col("tickets_count").desc()).show(10, truncate=False)

# 8. שמירה כפולה: גם למינאו (גולד) וגם לפוסטגרס (בוט)
if final_count > 0:
    # שמירה למינאו כפורמט Parquet
    print("💾 Saving Gold Parquet to MinIO...")
    gold_final.write.mode("overwrite").parquet("s3a://spark/gold/traffic_violations/")
    
    # שמירה לפוסטגרס
    jdbc_url = "jdbc:postgresql://postgres:5432/nyc_data"
    db_props = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}
    
    try:
        print("💎 Syncing to Postgres...")
        gold_final.write.jdbc(url=jdbc_url, table="gold_traffic_violations", mode="overwrite", properties=db_props)
        print("🚀 DONE! Everything is ready.")
    except Exception as e:
        print(f"❌ Postgres Error: {e}")

spark.stop()