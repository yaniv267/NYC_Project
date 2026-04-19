# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, count, desc, when, sum, first, rank, percent_rank, coalesce, lit
# from pyspark.sql.window import Window
# from pyspark.sql.functions import current_timestamp
# # 1. יצירת סשן
# spark = (SparkSession.builder 
#     .appName("NYC_Gold_Crashes") 
#     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") 
#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") 
#     .config("spark.hadoop.fs.s3a.access.key", "minioadmin") 
#     .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") 
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") 
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
#     .getOrCreate())

# # ביטול לוגים מיותרים
# spark.sparkContext.setLogLevel("WARN")

# # 2. קריאת נתוני Silver
# print("📖 Reading Crashes Silver data...")
# df_crashes = spark.read.parquet("s3a://spark/silver/nyc_crashes/")

# # 3. הכנת אגרגציה - קיבוץ לפי רובע ורחוב לדיוק מקסימלי
# print("📊 Aggregating crash data by street and borough...")

# # וידוא ששם הרובע קיים, אם לא - שים "UNKNOWN"
# df_crashes = df_crashes.withColumn("borough", coalesce(col("borough"), lit("UNKNOWN")))

# crash_base = df_crashes.groupBy("borough", "on_street_name").agg(
#     count("collision_id").alias("total_crashes"),
#     sum("total_injured").alias("total_injured"),
#     sum("total_killed").alias("total_killed"),
#     first("contributing_factor", ignorenulls=True).alias("main_cause")
# )

# # 4. חישוב מדדי סכנה ודירוגים (Window Function)
# # חישוב סך הכל תאונות בעיר לצורך אחוזים
# total_city_crashes_row = crash_base.agg(sum("total_crashes")).collect()[0][0]
# total_city_crashes = total_city_crashes_row if total_city_crashes_row else 1

# # הגדרת חלון (Window) לדירוג רחובות בתוך כל רובע
# window_spec = Window.partitionBy("borough").orderBy(desc("total_crashes"))

# gold_crashes = (crash_base
#     .withColumn("crash_pct", (col("total_crashes") / total_city_crashes) * 100)
#     .withColumn("danger_rank", rank().over(window_spec))
#     .withColumn("relative_rank", percent_rank().over(window_spec))
#     .withColumn("safety_label", 
#         when(col("relative_rank") <= 0.1, "🔴 High Danger (Top 10%)")
#         .when(col("relative_rank") <= 0.3, "🟡 Moderate (Top 30%)")
#         .otherwise("🟢 Relatively Safe"))
#     .withColumn("last_updated", current_timestamp())
#     .select(
#         col("on_street_name").alias("street_name"), 
#         col("borough"),
#         "total_crashes", 
#         "total_injured",
#         "total_killed",
#         coalesce(col("main_cause"), lit("Unknown")).alias("main_cause"),
#         col("crash_pct").cast("double"),
#         "danger_rank",
#         "relative_rank",
#         "safety_label",
#         "last_updated"
#     ).orderBy(desc("total_crashes"))
# )

# # 5. ייצוא נתונים
# jdbc_url = "jdbc:postgresql://postgres:5432/nyc_data" 
# db_props = {
#     "user": "postgres", 
#     "password": "postgres", 
#     "driver": "org.postgresql.Driver"
# }

# try:
#     print("🚀 Exporting Gold Layer to Postgres...")
#     gold_crashes.write.jdbc(url=jdbc_url, table="gold_crash_stats", mode="overwrite", properties=db_props)
    
#     print("🚀 Exporting Gold Layer to MinIO...")
#     gold_crashes.write.mode("overwrite").parquet("s3a://spark/gold/crashes/")
    
#     print("✅ Gold Pipeline Success!")
#     gold_crashes.show(10)
    
# except Exception as e: 
#     print(f"❌ Export failed: {e}")
    
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
#from nyc_schema import gold_crashes_schema
from parking_violation.nyc_schema_eliran import gold_crashes_schema
# 1. יצירת סשן
spark = (SparkSession.builder 
    .appName("NYC_Gold_Enriched_Crashes") 
    #.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") 
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0,org.elasticsearch:elasticsearch-spark-30_2.12:7.13.2")
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") 
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") 
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") 
    .config("spark.hadoop.fs.s3a.path.style.access", "true") 
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")



# 2. קריאת נתונים
print("📖 Reading Silver datasets...")
df_crashes = spark.read.parquet("s3a://spark/silver/nyc_crashes/")
df_addresses = spark.read.parquet("s3a://spark/silver/addresses/")

from pyspark.sql import functions as F
from pyspark.sql.window import Window
# ... (ייבוא SparkSession וסכימות) ...

# 1. מפת המרה רשמית של NYC (מקוד לשם מלא)
'''
borough_map = F.create_map([
    F.lit("1"), F.lit("MANHATTAN"),
    F.lit("2"), F.lit("BRONX"),
    F.lit("3"), F.lit("BROOKLYN"),
    F.lit("4"), F.lit("QUEENS"),
    F.lit("5"), F.lit("STATEN ISLAND")
])
''' 

# 2. הכנת טבלת הכתובות (Source of Truth)
addr_lookup = (df_addresses
    .select(
        F.round("latitude", 4).alias("lat_idx"),
        F.round("longitude", 4).alias("lon_idx"),
        F.col("street_name").alias("ref_address"),
        #F.col("borough_code").cast("string").alias("ref_boro_code")
    )
    .dropDuplicates(["lat_idx", "lon_idx"])
)

# 3. העשרת נתוני התאונות על בסיס מיקום בלבד
enriched_crashes = (df_crashes
    .withColumn("lat_idx", F.round("latitude", 4))
    .withColumn("lon_idx", F.round("longitude", 4))
    .join(addr_lookup, on=["lat_idx", "lon_idx"], how="left")
    
    # שליפת הרובע - רק מהחיבור לכתובות!
    #.withColumn("final_borough", borough_map[F.col("ref_boro_code")])
    #.withColumn("final_borough", borough_map[F.col("borough")])
    .withColumn("final_borough", F.col("borough"))
    
    # שחזור שם הרחוב - עדיפות לכתובת הרשמית מה-Join
    .withColumn("final_street", F.coalesce(F.col("ref_address"), F.col("on_street_name")))
)

# 4. אגרגציה וסינון
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
        F.first("latitude", ignorenulls=True).alias("latitude"),    
        F.first("longitude", ignorenulls=True).alias("longitude"),
        F.min("crash_timestamp").alias("first_crash_date"),
        F.max("crash_timestamp").alias("last_crash_date"),
        F.countDistinct(F.to_date("crash_timestamp")).alias("unique_crash_days"),
        F.first(F.date_format("crash_timestamp", "EEEE")).alias("sample_day_of_week")
    )
    # אנחנו שומרים רק את מה שהצלחנו לזהות במידה סבירה
    .filter(F.col("borough") != "UNKNOWN")
    .filter(F.col("street_name") != "UNKNOWN LOCATION")
)

# --- (המשך חישובי מדדי בטיחות ושמירה ל-Postgres כרגיל) ---

# 6. חישוב אחוזים ומדדים (Cross Join למניעת אזהרות)
total_city_crashes = crash_base.select(F.sum("total_crashes").alias("city_total"))

window_spec = Window.partitionBy("borough").orderBy(F.desc("total_crashes"))

gold_calculated = (crash_base.crossJoin(total_city_crashes)
    .withColumn("crash_pct", (F.col("total_crashes") / F.col("city_total")) * 100)
    .withColumn("danger_rank", F.rank().over(window_spec))
    .withColumn("relative_rank", F.percent_rank().over(window_spec))
    .withColumn("safety_label", 
        F.when(F.col("relative_rank") <= 0.1, "🔴 High Danger (Top 10%)")
        .when(F.col("relative_rank") <= 0.3, "🟡 Moderate (Top 30%)")
        .otherwise("🟢 Relatively Safe"))
    .withColumn("last_updated", F.current_timestamp())
    .select(
        "street_name", "borough", "total_crashes", "total_injured", "total_killed",
        F.coalesce(F.col("main_cause"), F.lit("Unknown")).alias("main_cause"),
        F.col("crash_pct").cast("double"),
        "danger_rank", "safety_label", "last_updated",
        "latitude", "longitude",
        "first_crash_date", 
        "last_crash_date", 
        "unique_crash_days", 
        "sample_day_of_week"
    )
)

# 7. אכיפת הסכימה - זה החלק שחיפשת!
# כאן אנחנו משתמשים בסכימה כדי "לחתוך" ולסדר את הנתונים לייצוא
final_columns = [F.col(field.name).cast(field.dataType) for field in gold_crashes_schema]
gold_final = gold_calculated.select(*final_columns)


# יצירת שדה מיקום גיאוגרפי עבור קיבאנה
gold_with_geo = gold_calculated.withColumn(
    "location",
    F.when(
        F.col("latitude").isNotNull() & F.col("longitude").isNotNull() & (F.col("latitude") != 0.0),
        F.concat_ws(",", F.col("latitude").cast("string"), F.col("longitude").cast("string"))
    ).otherwise(None)
)

# עדכון רשימת העמודות הסופית (הוספת ה-location)
final_columns_with_geo = [F.col(field.name).cast(field.dataType) for field in gold_crashes_schema] + [F.col("location")]
gold_final_to_es = gold_with_geo.select(*final_columns_with_geo)

# 7. ייצוא
jdbc_url = "jdbc:postgresql://postgres:5432/nyc_data" 
db_props = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

try:
    print("Exporting Gold to Postgres and MinIO...")
    gold_final.write.jdbc(url=jdbc_url, table="gold_crash_stats", mode="overwrite", properties=db_props)
    gold_final.write.mode("overwrite").parquet("s3a://spark/gold/crashes/")
    print(" Pipeline Completed Successfully!")
    gold_final.show(10, truncate=False)
except Exception as e:
    print(f"❌ Export failed: {e}")


    # 3. בלוק הכתיבה לאלסטיק ב-Overwrite Mode
try:
    print("💎 Syncing Crashes Gold to Elasticsearch (Overwrite Mode)...")
    
    (gold_final_to_es.write
    .format("org.elasticsearch.spark.sql")
    .option("es.nodes", "elasticsearch") 
    .option("es.port", "9200")
    .option("es.nodes.wan.only", "true")
    .option("es.resource", "nyc_crashes_gold") 
        
        # הגדרות יציבות לביצועים אופטימליים
    .option("es.batch.size.entries", "1000")
    .option("es.http.timeout", "2m")
    .option("es.http.retries", "5")
        
        # סנכרון מלא - מחיקה ויצירה מחדש של הנתונים
    .mode("overwrite")
        
        # שימוש ב-doc_id הייחודי (Borough + Street) למניעת כפילויות
    #.option("es.mapping.id", "doc_id") 
    .save())
        
    print("✅ Success! Data is in Elasticsearch with Location field.")

except Exception as e:
    print(f"❌ Elasticsearch Write Error: {e}")