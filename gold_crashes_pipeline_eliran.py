from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from nyc_schema import gold_crashes_schema

# 1. אתחול סשן Spark
spark = (SparkSession.builder 
    .appName("NYC_Gold_Danger_Metrics") 
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") 
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") 
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") 
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") 
    .config("spark.hadoop.fs.s3a.path.style.access", "true") 
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
    .getOrCreate())

# 2. קריאת נתונים
df_crashes = spark.read.parquet("s3a://spark/silver/nyc_crashes/")
df_addresses = spark.read.parquet("s3a://spark/silver/addresses/")

# 3. הכנת ה-Lookup (כתובות)
addr_lookup = (df_addresses
    .select(
        F.round("latitude", 4).alias("lat_idx"),
        F.round("longitude", 4).alias("lon_idx"),
        F.col("street_name").alias("ref_street_name"),
        F.col("borough_code").cast("string").alias("ref_boro_code")
    )
    .dropDuplicates(["lat_idx", "lon_idx"])
)

# 4. מפת רובעים
borough_map = F.create_map([
    F.lit("1"), F.lit("MANHATTAN"),
    F.lit("2"), F.lit("BRONX"),
    F.lit("3"), F.lit("BROOKLYN"),
    F.lit("4"), F.lit("QUEENS"),
    F.lit("5"), F.lit("STATEN ISLAND")
])

# 5. שלב ההעשרה (Enrichment)
enriched_crashes = (
    df_crashes
    .withColumn("lat_idx", F.round("latitude", 4))
    .withColumn("lon_idx", F.round("longitude", 4))
    .join(addr_lookup, ["lat_idx", "lon_idx"], "left")
    .withColumn("final_borough", borough_map[F.col("ref_boro_code")])
    .withColumn("final_street", F.coalesce(F.col("ref_street_name"), F.col("on_street_name")))
)

# 6. אגרגציה (Crash Base)
# כאן אנחנו מחשבים את כל המדדים שביקשת ברמת רחוב ורובע
crash_base = (enriched_crashes
    .groupBy(
        F.coalesce(F.col("final_borough"), F.lit("UNKNOWN")).alias("borough"),
        F.coalesce(F.col("final_street"), F.lit("UNKNOWN LOCATION")).alias("street_name")
    )
    .agg(
        F.count("collision_id").alias("total_crashes"),
        F.sum("total_injured").alias("total_injured"),
        F.sum("total_killed").alias("total_killed"),
        F.first("contributing_factor", ignorenulls=True).alias("main_cause"),
        
        # 🧠 TIME FEATURES
        F.min("crash_timestamp").alias("first_crash_date"),
        F.max("crash_timestamp").alias("last_crash_date"),
        F.countDistinct(F.to_date("crash_timestamp")).alias("unique_crash_days"),
        F.first("day_of_week").alias("sample_day_of_week"),

        # תוספת קריטית לקיבנה: ממוצע מיקום לכל רחוב
        F.avg("latitude").alias("latitude"),
        F.avg("longitude").alias("longitude")
    )
)

# 7. חישוב סך הכל תאונות בעיר (Cross Join)
total_city_crashes = crash_base.select(F.sum("total_crashes").alias("city_total"))

# 8. הגדרת חלון לדירוג (Window Spec)
window_spec = Window.partitionBy("borough").orderBy(F.desc("total_crashes"))

# 9. חישוב מדדי סופיים (Metrics & Labels)
gold_calculated = (crash_base.crossJoin(total_city_crashes)
    .withColumn("crash_pct", (F.col("total_crashes") / F.col("city_total")) * 100)
    .withColumn("danger_rank", F.rank().over(window_spec))
    .withColumn("relative_rank", F.percent_rank().over(window_spec))
    .withColumn(
        "safety_label",
        F.when(F.col("relative_rank") <= 0.1, "🔴 High Danger (Top 10%)")
         .when(F.col("relative_rank") <= 0.3, "🟡 Moderate (Top 30%)")
         .otherwise("🟢 Relatively Safe")
    )
    .withColumn("last_updated", F.current_timestamp())
)

# 10. בחירת עמודות סופית (הוספתי את הקואורדינטות לרשימה שלך)
gold_calculated = gold_calculated.select(
    "street_name",
    "borough",
    "total_crashes",
    "total_injured",
    "total_killed",
    "main_cause",
    "latitude",   # חשוב למפות
    "longitude",  # חשוב למפות
    "crash_pct",
    "danger_rank",
    "safety_label",
    "first_crash_date",
    "last_crash_date",
    "unique_crash_days",
    "sample_day_of_week",
    "last_updated"
)

# 11. אכיפת סכימה (וודא ש-latitude ו-longitude קיימים ב-gold_crashes_schema)
final_columns = [F.col(f.name).cast(f.dataType) for f in gold_crashes_schema]
gold_final = gold_calculated.select(*final_columns)

# 12. כתיבה (Write)
jdbc_url = "jdbc:postgresql://postgres:5432/nyc_data"
db_props = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

try:
    print("🚀 Exporting Gold Metrics...")
    # כתיבה לפוסטגרס (לטבלה שביקשת)
    gold_final.write.jdbc(url=jdbc_url, table="gold_crash_stats", mode="overwrite", properties=db_props)
    
    # שמירה ל-MinIO כגיבוי Parquet
    gold_final.write.mode("overwrite").parquet("s3a://spark/gold/crashes_stats/")
    
    print("✅ Done ✔")
    gold_final.show(10, truncate=False)

except Exception as e:
    print(f"❌ Export failed: {e}")
    
 
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, avg, count, window, desc, when, max as spark_max, coalesce, lit

# # 1. יצירת סשן
# spark = (SparkSession.builder 
#     .appName("NYC_Master_Gold_With_Time") 
#     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") 
#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") 
#     .config("spark.hadoop.fs.s3a.access.key", "minioadmin") 
#     .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") 
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") 
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
#     .getOrCreate())

# # 2. קריאת נתוני Silver
# print("📖 Reading Silver data...")
# df_traffic = spark.read.parquet("s3a://spark/silver/nyc_traffic/")
# df_crashes = spark.read.parquet("s3a://spark/silver/nyc_crashes/")
# df_addresses = spark.read.parquet("s3a://spark/silver/addresses/")

# # 3. הכנת נתוני כתובות (ממוצע קואורדינטות לרחוב)
# addr_geo = df_addresses.groupBy("street_name_clean").agg(
#     avg("latitude").alias("latitude"),
#     avg("longitude").alias("longitude")
# )

# # 4. הכנת נתוני תנועה (חלון זמן של שעה)
# traffic_gold = (df_traffic 
#     .groupBy("link_name", window("event_time", "1 hour").alias("time_window")) 
#     .agg(avg("speed").alias("avg_speed"), count("link_id").alias("report_count"))
#     .withColumn("traffic_level", 
#         when(col("avg_speed") > 45, "🟢 Smooth")
#         .when(col("avg_speed") > 20, "🟡 Heavy")
#         .otherwise("🔴 Traffic Jam"))
# )

# # 5. הכנת נתוני תאונות (בטיחות)
# crash_gold = df_crashes.groupBy("on_street_name").agg(count("collision_id").alias("total_crashes"))
# max_val = crash_gold.select(spark_max("total_crashes")).collect()[0][0]
# crash_safety = crash_gold.withColumn("safety_score", (100 * (1 - (col("total_crashes") / max_val))).cast("int"))

# # 6. ה-Join הגדול
# master_gold = traffic_gold.join(
#     crash_safety, 
#     traffic_gold.link_name.contains(crash_safety.on_street_name), 
#     "left"
# ).join(
#     addr_geo,
#     traffic_gold.link_name.contains(addr_geo.street_name_clean),
#     "left"
# ).select(
#     col("link_name"),
#     col("time_window.start").alias("data_time"),
#     col("avg_speed").cast("int").alias("speed"),
#     col("traffic_level"),
#     col("total_crashes"),
#     col("safety_score"),
#     col("latitude"),
#     col("longitude")
# ).orderBy(desc("data_time"))

# # --- 7. חישוב סטטיסטיקות התאמה (Data Quality Metrics) ---
# total_rows = master_gold.count()
# matched_rows = master_gold.filter(col("latitude").isNotNull()).count()
# match_percentage = (matched_rows / total_rows) * 100 if total_rows > 0 else 0

# print("-" * 50)
# print(f"📊 DATA QUALITY REPORT:")
# print(f"Total rows in Gold: {total_rows}")
# print(f"Rows with Coordinates: {matched_rows}")
# print(f"Geo-Match Success Rate: {match_percentage:.2f}%")
# print("-" * 50)

# # 8. הצגת 20 שורות ראשונות
# print("\n💎 Final Gold Data Sample (First 20 Observations):")
# master_gold.show(20, truncate=False)

# # --- 9. Export to PostgreSQL (Serving Layer) ---
# try:
#     print("\n🚀 Exporting to Postgres (Serving Layer)...")
#     jdbc_url = "jdbc:postgresql://postgres:5432/nyc_data" 
#     db_props = {
#         "user": "postgres", 
#         "password": "postgres", 
#         "driver": "org.postgresql.Driver"
#     }
#     # We use 'overwrite' for the bot to always have the freshest snapshot
#     master_gold.write.jdbc(url=jdbc_url, table="gold_traffic_safety_stats", mode="overwrite", properties=db_props)
#     print("✅ Postgres Export Success!")
# except Exception as e:
#     print(f"❌ Postgres Export failed: {e}")

# # --- 10. Export to MinIO (Historical/Analytics Layer) ---
# try:
#     print("\n📦 Saving to MinIO Data Lake (Gold Layer)...")
#     # We define the path in the gold bucket
#     gold_path = "s3a://spark/gold/traffic_safety_history/"
    
#     # We save as Parquet and partition by data_time (cast to date) for efficient historical queries
#     (master_gold
#      .withColumn("report_date", col("data_time").cast("date"))
#      .write
#      .partitionBy("report_date")
#      .mode("append") # Use 'append' to keep history of all runs
#      .parquet(gold_path))
    
#     print(f"✅ MinIO Gold Storage Success! Path: {gold_path}")
# except Exception as e:
#     print(f"❌ MinIO Export failed: {e}")