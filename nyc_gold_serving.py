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

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, window, desc, when, max as spark_max, coalesce, lit

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

# 2. קריאת נתוני Silver
print("📖 Reading Silver data...")
df_traffic = spark.read.parquet("s3a://spark/silver/nyc_traffic/")
df_crashes = spark.read.parquet("s3a://spark/silver/nyc_crashes/")

# 3. הכנת נתוני תנועה - עם חלון זמן של שעה
traffic_gold = (df_traffic 
    .groupBy("link_name", window("event_time", "1 hour").alias("time_window")) 
    .agg(
        avg("speed").alias("avg_speed"),
        count("link_id").alias("report_count")
    )
    .withColumn("traffic_level", 
        when(col("avg_speed") > 45, "🟢 Smooth")
        .when(col("avg_speed") > 20, "🟡 Heavy")
        .otherwise("🔴 Traffic Jam"))
)

# 4. הכנת נתוני תאונות (סטטיסטיקה כללית לפי רחוב)
crash_gold = (df_crashes 
    .groupBy("on_street_name") 
    .agg(count("collision_id").alias("total_crashes"))
)

max_val = crash_gold.select(spark_max("total_crashes")).collect()[0][0]
crash_safety = crash_gold.withColumn("safety_score", 
    (100 * (1 - (col("total_crashes") / max_val))).cast("int")
)

# 5. יצירת הטבלה המאוחדת עם זמן ותאריך
master_gold = traffic_gold.join(
    crash_safety, 
    traffic_gold.link_name.contains(crash_safety.on_street_name), 
    "left"
).select(
    col("link_name"),
    col("time_window.start").alias("data_time"), # מוסיף את שעת תחילת הדיווח
    col("avg_speed").cast("int").alias("speed"),
    col("traffic_level"),
    col("report_count"),
    coalesce(col("total_crashes"), lit(0)).alias("total_crashes"),
    coalesce(col("safety_score"), lit(100)).alias("safety_score")
).orderBy(desc("data_time")) # הכי חדש למעלה

# 6. שמירה ל-PostgreSQL
jdbc_url = "jdbc:postgresql://postgres:5432/nyc_data" 
db_props = {"user": "user", "password": "password", "driver": "org.postgresql.Driver"}

try:
    print("🚀 Saving Master Gold Table with Time to Postgres...")
    master_gold.write.jdbc(url=jdbc_url, table="bot_serving_table", mode="overwrite", properties=db_props)
    print("✅ Export Success! Time and Report Count included.")
except Exception as e:
    print(f"❌ DB Export failed: {e}")

# הצגת נתונים לדוגמה
print("\n💎 Final Gold Data Sample (Check the 'data_time' column):")
master_gold.show(5, truncate=False)