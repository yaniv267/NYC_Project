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
   
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, window, desc, when, max as spark_max, coalesce, lit, sum, first

# 1. יצירת סשן
spark = (SparkSession.builder 
    .appName("NYC_Master_Gold_With_Time") 
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") 
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") 
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") 
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") 
    .config("spark.hadoop.fs.s3a.path.style.access", "true") 
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
    .getOrCreate())

# 2. קריאת נתוני Silver (אין צורך ב-addresses יותר, הכל כבר בתחבורה!)
print("📖 Reading Silver data...")
df_traffic = spark.read.parquet("s3a://spark/silver/nyc_traffic/")
df_crashes = spark.read.parquet("s3a://spark/silver/nyc_crashes/")

# 3. הכנת נתוני תנועה (חלון זמן של שעה + משיכת רובע וקואורדינטות מהתחבורה)
traffic_gold = (df_traffic 
    .groupBy("link_name", window("event_time", "1 hour").alias("time_window")) 
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

# 4. הכנת נתוני תאונות (בטיחות) - מותאם במדויק לסכימת הסילבר שלך!
# אם לא הוספת את borough לסילבר עדיין, זה לא יקרוס בזכות הלוגיקה הדינמית שהוספנו
crash_cols = [
    count("collision_id").alias("total_crashes"),
    sum("total_injured").alias("total_injured"), # כבר Integer בסילבר
    sum("total_killed").alias("total_killed"),   # כבר Integer בסילבר
    first("contributing_factor", ignorenulls=True).alias("main_cause")
]

if "borough" in df_crashes.columns:
    crash_cols.append(first("borough", ignorenulls=True).alias("crash_borough"))
else:
    crash_cols.append(lit("NYC").alias("crash_borough"))

crash_gold = df_crashes.groupBy("on_street_name").agg(*crash_cols)

# חישוב ציון בטיחות
try:
    max_val_row = crash_gold.select(spark_max("total_crashes")).collect()[0][0]
    max_val = max_val_row if max_val_row is not None else 1
except:
    max_val = 1
    
crash_safety = crash_gold.withColumn("safety_score", (100 * (1 - (col("total_crashes") / max_val))).cast("int"))

# 5. ה-Join הגדול (תחבורה + תאונות)
master_gold = traffic_gold.join(
    crash_safety, 
    traffic_gold.link_name.contains(crash_safety.on_street_name), 
    "left"
).select(
    col("link_name"),
    col("time_window.start").alias("data_time"),
    col("avg_speed").cast("int").alias("speed"),
    col("traffic_level"),
    coalesce(col("traffic_borough"), col("crash_borough"), lit("NYC")).alias("borough"), # חסין תקלות!
    coalesce(col("total_crashes"), lit(0)).alias("total_crashes"),
    coalesce(col("total_injured"), lit(0)).alias("total_injured"),
    coalesce(col("total_killed"), lit(0)).alias("total_killed"),
    coalesce(col("main_cause"), lit("Clean Record")).alias("main_cause"),
    coalesce(col("safety_score"), lit(100)).alias("safety_score"),
    col("latitude"),
    col("longitude")
).orderBy(desc("data_time"))

# --- 6. סטטיסטיקות ---
total_rows = master_gold.count()
print(f"\n✅ Created Gold Layer with {total_rows} rows.")
master_gold.show(5)

# --- 7. שמירה ל-Postgres (בשביל הבוט) ---
try:
    print("\n🚀 Exporting to Postgres...")
    jdbc_url = "jdbc:postgresql://postgres:5432/nyc_data" 
    db_props = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}
    master_gold.write.jdbc(url=jdbc_url, table="gold_traffic_safety_stats", mode="overwrite", properties=db_props)
    print("✅ Postgres Success!")
except Exception as e: print(f"❌ Postgres failed: {e}")

# --- 8. שמירה ל-MinIO (בשביל היסטוריה וקיבאנה) ---
try:
    print("\n📦 Saving to MinIO...")
    gold_path = "s3a://spark/gold/traffic_safety_history/"
    (master_gold.withColumn("report_date", col("data_time").cast("date"))
     .write.partitionBy("report_date").mode("append").parquet(gold_path))
    print("✅ MinIO Success!")
except Exception as e: print(f"❌ MinIO failed: {e}")