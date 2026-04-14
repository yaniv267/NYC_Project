# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, avg, count, window, desc

# # 1. יצירת Spark Session
# spark = (SparkSession.builder 
#     .appName("NYC_Gold_View_Data") 
#     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") 
#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") 
#     .config("spark.hadoop.fs.s3a.access.key", "minioadmin") 
#     .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") 
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") 
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
#     .getOrCreate())

# # 2. קריאה משכבת ה-Silver (MinIO)
# print("\n📖 Reading Silver data...")
# df_traffic = spark.read.parquet("s3a://spark/silver/nyc_traffic/")
# df_crashes = spark.read.parquet("s3a://spark/silver/nyc_crashes/")

# # ---------------------------------------------------------
# # 3. עיבוד נתוני זהב - תנועה (Traffic)
# # ---------------------------------------------------------
# gold_traffic = (df_traffic 
#     .groupBy("link_name", window("event_time", "1 hour").alias("hour")) 
#     .agg(avg("speed").alias("avg_speed"), count("link_id").alias("reports"))
#     .select(col("link_name"), col("hour.start").alias("start_time"), col("avg_speed")))

# print("\n📊 --- 5 שורות ראשונות של נתוני תנועה (Traffic Stats) ---")
# gold_traffic.show(5, truncate=False)

# # ---------------------------------------------------------
# # 4. עיבוד נתוני זהב - תאונות (Crashes)
# # ---------------------------------------------------------
# gold_crashes = (df_crashes 
#     .groupBy("on_street_name") 
#     .agg(count("collision_id").alias("total_crashes"))
#     .orderBy(desc("total_crashes")))

# print("\n🚗 --- 5 שורות ראשונות של סיכום תאונות (Crash Summary) ---")
# gold_crashes.show(5, truncate=False)

# # ---------------------------------------------------------
# # 5. כתיבה ל-PostgreSQL
# # ---------------------------------------------------------
# # אם אתה מריץ מתוך הדוקר (dev_env): השתמש ב-postgres
# # אם אתה מריץ מהמחשב בחוץ: השתמש ב-127.0.0.1
# jdbc_url = "jdbc:postgresql://postgres:5432/nyc_data"
# db_props = {"user": "user", "password": "password", "driver": "org.postgresql.Driver"}

# try:
#     print("\n💎 Saving data to PostgreSQL...")
#     gold_traffic.write.jdbc(url=jdbc_url, table="fact_traffic_stats", mode="overwrite", properties=db_props)
#     gold_crashes.write.jdbc(url=jdbc_url, table="fact_crash_summary", mode="overwrite", properties=db_props)
#     print("✅ Export Success!")
# except Exception as e:
#     print(f"❌ DB Export failed, but data is shown above. Error: {e}")

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
   
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, avg, count, window, desc, when, max as spark_max, coalesce, lit, sum, first, rank, percent_rank
# from pyspark.sql.window import Window

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

# # 2. קריאת נתונים
# print("📖 Reading Silver data...")
# df_traffic = spark.read.parquet("s3a://spark/silver/nyc_traffic/")
# df_crashes = spark.read.parquet("s3a://spark/silver/nyc_crashes/")

# # 3. הכנת נתוני תנועה
# traffic_gold = (df_traffic 
#     .groupBy("link_name", window("event_time", "1 hour").alias("time_window")) 
#     .agg(
#         avg("speed").alias("avg_speed"),
#         first("borough", ignorenulls=True).alias("traffic_borough"),
#         first("latitude", ignorenulls=True).alias("latitude"),
#         first("longitude", ignorenulls=True).alias("longitude")
#     )
#     .withColumn("traffic_level", 
#         when(col("avg_speed") > 45, "🟢 Smooth")
#         .when(col("avg_speed") > 20, "🟡 Heavy")
#         .otherwise("🔴 Traffic Jam"))
# )

# # 4. הכנת נתוני תאונות + חישוב מדדי מסוכנות דינמיים
# crash_cols = [
#     count("collision_id").alias("total_crashes"),
#     sum("total_injured").alias("total_injured"),
#     sum("total_killed").alias("total_killed"),
#     first("contributing_factor", ignorenulls=True).alias("main_cause")
# ]

# if "borough" in df_crashes.columns:
#     crash_cols.append(first("borough", ignorenulls=True).alias("crash_borough"))
# else:
#     crash_cols.append(lit("NYC").alias("crash_borough"))

# crash_base = df_crashes.groupBy("on_street_name").agg(*crash_cols)

# # חישוב סך התאונות לצורך ה-Percentage
# total_city_crashes = crash_base.agg(sum("total_crashes")).collect()[0][0] or 1

# # הגדרת החלון לדירוג
# window_spec = Window.orderBy(desc("total_crashes"))

# crash_safety = (crash_base
#     .withColumn("crash_pct", (col("total_crashes") / total_city_crashes) * 100)
#     .withColumn("danger_rank", rank().over(window_spec))
#     .withColumn("relative_rank", percent_rank().over(window_spec)) # המדד הדינמי 0-1
#     .withColumn("safety_label", 
#         when(col("relative_rank") <= 0.1, "🔴 High Danger (Top 10%)")
#         .when(col("relative_rank") <= 0.3, "🟡 Moderate (Top 30%)")
#         .otherwise("🟢 Relatively Safe"))
# )

# # 5. ה-Join הגדול (כולל השדות החדשים)
# master_gold = traffic_gold.join(
#     crash_safety, 
#     traffic_gold.link_name.contains(crash_safety.on_street_name), 
#     "left"
# ).select(
#     col("link_name"),
#     col("time_window.start").alias("data_time"),
#     col("avg_speed").cast("int").alias("speed"),
#     col("traffic_level"),
#     coalesce(col("traffic_borough"), col("crash_borough"), lit("NYC")).alias("borough"),
#     coalesce(col("total_crashes"), lit(0)).alias("total_crashes"),
#     coalesce(col("total_injured"), lit(0)).alias("total_injured"),
#     coalesce(col("total_killed"), lit(0)).alias("total_killed"),
#     coalesce(col("main_cause"), lit("Clean Record")).alias("main_cause"),
#     coalesce(col("crash_pct"), lit(0)).alias("crash_pct"),
#     coalesce(col("danger_rank"), lit(999)).alias("danger_rank"),
#     coalesce(col("relative_rank"), lit(1.0)).alias("relative_rank"),
#     coalesce(col("safety_label"), lit("🟢 Relatively Safe")).alias("safety_label"),
#     col("latitude"),
#     col("longitude")
# ).orderBy(desc("data_time"))

# # --- 6. הדפסת תוצאות לפני שמירה ---
# print(f"\n✅ Previewing Gold Layer ({master_gold.count()} total rows):")
# master_gold.select("link_name", "speed", "traffic_level", "total_crashes", "safety_label", "danger_rank").show(10, truncate=False)

# # 7. שמירה ל-Postgres
# try:
#     print("\n🚀 Exporting to Postgres for Telegram Bot...")
#     jdbc_url = "jdbc:postgresql://postgres:5432/nyc_data" 
#     db_props = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}
#     master_gold.write.jdbc(url=jdbc_url, table="gold_traffic_safety_stats", mode="overwrite", properties=db_props)
#     print("✅ Postgres Success!")
# except Exception as e: print(f"❌ Postgres failed: {e}")

# # 8. שמירה ל-MinIO
# try:
#     print("\n📦 Saving to MinIO History...")
#     gold_path = "s3a://spark/gold/traffic_safety_history/"
#     (master_gold.withColumn("report_date", col("data_time").cast("date"))
#      .write.partitionBy("report_date").mode("append").parquet(gold_path))
#     print("✅ MinIO Success!")
# except Exception as e: print(f"❌ MinIO failed: {e}")

from pyspark.sql import SparkSession
# הוספנו את הפונקציות dayofweek ו-hour
from pyspark.sql.functions import col, avg, count, window, desc, when, max as spark_max, \
    coalesce, lit, sum, first, rank, percent_rank, dayofweek, hour 
from pyspark.sql.window import Window

# 1. יצירת סשן
spark = (SparkSession.builder 
    .appName("NYC_Master_Gold_With_Baseline") 
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") 
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") 
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") 
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") 
    .config("spark.hadoop.fs.s3a.path.style.access", "true") 
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
    .getOrCreate())

# 2. קריאת נתונים
print("📖 Reading Silver data...")
df_traffic = spark.read.parquet("s3a://spark/silver/nyc_traffic/")
df_crashes = spark.read.parquet("s3a://spark/silver/nyc_crashes/")

# --- 3. שלב חדש: חישוב בייסליין היסטורי (Typical Speed) ---
print("📊 Calculating historical speed patterns...")
historical_patterns = (df_traffic
    .withColumn("day_of_week", dayofweek("event_time"))
    .withColumn("hour_of_day", hour("event_time"))
    .groupBy("link_name", "day_of_week", "hour_of_day")
    .agg(avg("speed").alias("typical_speed"))
)

# 4. הכנת נתוני תנועה (כולל חילוץ זמן ל-Join עם הבייסליין)
traffic_gold = (df_traffic 
    .withColumn("day_idx", dayofweek("event_time"))
    .withColumn("hour_idx", hour("event_time"))
    .groupBy("link_name", window("event_time", "1 hour").alias("time_window"), "day_idx", "hour_idx") 
    .agg(
        avg("speed").alias("avg_speed"),
        first("borough", ignorenulls=True).alias("traffic_borough"),
        first("latitude", ignorenulls=True).alias("latitude"),
        first("longitude", ignorenulls=True).alias("longitude")
    )
    .withColumn("traffic_level", 
        when(col("avg_speed") > 45, "🟢 Smooth")
        .when(col("avg_speed") > 20, "🟡 Heavy")
        .otherwise("🔴 Traffic Jam"))
)

# 5. הכנת נתוני תאונות (ללא שינוי)
crash_cols = [
    count("collision_id").alias("total_crashes"),
    sum("total_injured").alias("total_injured"),
    sum("total_killed").alias("total_killed"),
    first("contributing_factor", ignorenulls=True).alias("main_cause")
]
if "borough" in df_crashes.columns:
    crash_cols.append(first("borough", ignorenulls=True).alias("crash_borough"))
else:
    crash_cols.append(lit("NYC").alias("crash_borough"))

crash_base = df_crashes.groupBy("on_street_name").agg(*crash_cols)
total_city_crashes = crash_base.agg(sum("total_crashes")).collect()[0][0] or 1
window_spec = Window.partitionBy("crash_borough").orderBy(desc("total_crashes"))

crash_safety = (crash_base
    .withColumn("crash_pct", (col("total_crashes") / total_city_crashes) * 100)
    .withColumn("danger_rank", rank().over(window_spec))
    .withColumn("relative_rank", percent_rank().over(window_spec))
    .withColumn("safety_label", 
        when(col("relative_rank") <= 0.1, "🔴 High Danger (Top 10%)")
        .when(col("relative_rank") <= 0.3, "🟡 Moderate (Top 30%)")
        .otherwise("🟢 Relatively Safe"))
)
# --- 6. ה-Join הגדול המשולב + ניקוי כפילויות ---
# שלב א': ביצוע ה-Join
joined_gold = (traffic_gold
    .join(historical_patterns, 
          (traffic_gold.link_name == historical_patterns.link_name) & 
          (traffic_gold.day_idx == historical_patterns.day_of_week) & 
          (traffic_gold.hour_idx == historical_patterns.hour_of_day), 
          "left")
    .join(crash_safety, 
          traffic_gold.link_name.contains(crash_safety.on_street_name), 
          "left")
)

# שלב ב': ניקוי כפילויות (Deduplication)
# אם מקטע תנועה קיבל כמה התאמות של תאונות, אנחנו משאירים רק את זו עם הכי הרבה תאונות
dedup_window = Window.partitionBy(traffic_gold["link_name"], "time_window").orderBy(desc("total_crashes"))

master_gold = (joined_gold
    .withColumn("row_num", rank().over(dedup_window))
    .filter(col("row_num") == 1) # משאירים רק את ההתאמה הכי "חזקה"
    .select(
        traffic_gold["link_name"], 
        col("time_window.start").alias("data_time"),
        col("avg_speed").cast("int").alias("speed"),
        col("typical_speed").cast("int").alias("typical_speed"),
        col("traffic_level"),
        coalesce(col("traffic_borough"), col("crash_borough"), lit("NYC")).alias("borough"),
        coalesce(col("total_crashes"), lit(0)).alias("total_crashes"),
        coalesce(col("total_injured"), lit(0)).alias("total_injured"),
        coalesce(col("total_killed"), lit(0)).alias("total_killed"),
        coalesce(col("main_cause"), lit("Clean Record")).alias("main_cause"),
        coalesce(col("crash_pct"), lit(0)).alias("crash_pct"),
        coalesce(col("danger_rank"), lit(999)).alias("danger_rank"),
        coalesce(col("relative_rank"), lit(1.0)).alias("relative_rank"),
        coalesce(col("safety_label"), lit("🟢 Relatively Safe")).alias("safety_label"),
        col("latitude"),
        col("longitude")
    ).orderBy(desc("data_time"))
)
# --- 7. הדפסת תוצאות ---
print(f"\n✅ Created Gold Layer with Baseline. Preview:")
master_gold.select("link_name", "speed", "typical_speed", "safety_label").show(10, truncate=False)

# 8. שמירה ל-Postgres (הטבלה תתעדכן עם העמודה החדשה)
try:
    print("\n🚀 Exporting to Postgres...")
    jdbc_url = "jdbc:postgresql://postgres:5432/nyc_data" 
    db_props = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}
    master_gold.write.jdbc(url=jdbc_url, table="gold_traffic_safety_stats", mode="overwrite", properties=db_props)
    print("✅ Postgres Success!")
except Exception as e: print(f"❌ Postgres failed: {e}")

# 9. שמירה ל-MinIO
try:
    gold_path = "s3a://spark/gold/traffic_safety_history/"
    (master_gold.withColumn("report_date", col("data_time").cast("date"))
     .write.partitionBy("report_date").mode("append").parquet(gold_path))
    print("✅ MinIO Success!")
except Exception as e: print(f"❌ MinIO failed: {e}")