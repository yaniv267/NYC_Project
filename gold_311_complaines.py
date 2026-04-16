# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from pyspark.sql.window import Window
# from nyc_schema import gold_311_schema 

# # 1. אתחול (נשאר אותו דבר)
# spark = (SparkSession.builder \
#     .appName("NYC_311_complaines_gold") \
#     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") \
#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
#     .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .getOrCreate())

# # 2. קריאה מהסילבר
# silver_df = spark.read.parquet("s3a://spark/silver/311_complaints/")

# # --- חישוב שעת שיא (Peak Hour) לכל סוג תלונה בכל רובע ---
# # אנחנו בודקים איזו שעה מופיעה הכי הרבה לכל (Borough + Complaint Type)
# peak_hour_df = (silver_df
#     .groupBy("borough", "complaint_type", "hour")
#     .count()
#     .withColumn("rn", F.row_number().over(Window.partitionBy("borough", "complaint_type").orderBy(F.desc("count"))))
#     .filter(F.col("rn") == 1)
#     .select("borough", "complaint_type", F.col("hour").alias("peak_hour"))
# )

# # 3. יצירת טבלת ה-Gold המאוחדת
# gold_311 = (silver_df
#     # הוספת עמודת תאריך נקי (ללא שעה) בשביל הדיווח בבוט
#     .withColumn("data_time", F.to_date("created_date"))
    
#     # אגרגציה
#     .groupBy("borough", "complaint_type", "data_time", "street_name")
#     .agg(
#         F.count("unique_key").alias("complaint_count"),
#         F.avg("latitude").alias("avg_lat"),
#         F.avg("longitude").alias("avg_lon")
#     )
# )

# # 4. חיבור שעת השיא והוספת רמות אינטנסיביות
# final_gold = (gold_311
#     .join(peak_hour_df, ["borough", "complaint_type"], "left")
#     .withColumn("intensity_level", 
#         F.when(F.col("complaint_count") > 100, "🔴 HIGH")
#          .when(F.col("complaint_count") > 50, "🟠 MEDIUM")
#          .otherwise("🟢 NORMAL"))
#     .withColumn("last_updated_at", F.current_timestamp())
# )

# final_gold.show(5)

# # 5. שמירה (MinIO + Postgres)
# final_gold.write.mode("overwrite").parquet("s3a://spark/gold/311_complaines/")

# jdbc_url = "jdbc:postgresql://postgres:5432/nyc_data"
# db_props = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

# final_gold.write.jdbc(
#     url=jdbc_url, 
#     table="gold_311_stats", 
#     mode="overwrite", 
#     properties=db_props
# )

# print("✅ Gold 311 updated with Peak Hours and Data Dates!")
# spark.stop()


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from nyc_schema import gold_311_schema 

# 1. אתחול Spark Session
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

# 2. קריאה מהסילבר
print("📖 Reading 311 Silver data...")
silver_df = spark.read.parquet("s3a://spark/silver/311_complaints/")

# 3. חישוב שעת שיא (Peak Hour) לכל סוג תלונה בכל רובע
# זה נתון מעולה לבוט: "מתי הכי כדאי להימנע מהאזור?"
peak_hour_df = (silver_df
    .groupBy("borough", "complaint_type", "hour")
    .count()
    .withColumn("rn", F.row_number().over(Window.partitionBy("borough", "complaint_type").orderBy(F.desc("count"))))
    .filter(F.col("rn") == 1)
    .select("borough", "complaint_type", F.col("hour").alias("peak_hour"))
)

# 4. אגרגציה ראשית לפי רובע, סוג תלונה וזמן (לפי הסכימה)
print("📊 Aggregating complaints metrics...")
gold_base = (silver_df
    .groupBy("borough", "complaint_type", "year", "month", "day_name")
    .agg(
        F.count("unique_key").alias("complaint_count"),
        F.avg("latitude").alias("avg_lat"),
        F.avg("longitude").alias("avg_lon"),
        F.max("created_date").alias("latest_incident") # מתי קרה המקרה האחרון
    )
)

# 5. חיבור הנתונים, הוספת אינטנסיביות ואכיפת סכימה
final_gold_calculated = (gold_base
    .join(peak_hour_df, ["borough", "complaint_type"], "left")
    .withColumnRenamed("peak_hour", "hour") # התאמה לשם השדה בסכימה
    .withColumn("intensity_level", 
        F.when(F.col("complaint_count") > 100, "🔴 HIGH")
         .when(F.col("complaint_count") > 50, "🟠 MEDIUM")
         .otherwise("🟢 NORMAL"))
    .withColumn("last_updated_at", F.current_timestamp())
)

# --- אכיפת סכימה סופית (The Data Contract) ---
print("🛡️ Enforcing Gold Schema...")
gold_final = final_gold_calculated.select(*[F.col(field.name).cast(field.dataType) for field in gold_311_schema])

gold_final.show(5, truncate=False)

# 6. שמירה (MinIO + Postgres)
print("🚀 Exporting Gold to targets...")

# שמירה ל-MinIO
gold_final.write.mode("overwrite").parquet("s3a://spark/gold/311_complaints/")

# שמירה ל-Postgres לשימוש הבוט
jdbc_url = "jdbc:postgresql://postgres:5432/nyc_data"
db_props = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

try:
    gold_final.write.jdbc(
        url=jdbc_url, 
        table="gold_311_stats", 
        mode="overwrite", 
        properties=db_props
    )
    print("✅ Gold 311 Pipeline Completed Successfully!")
except Exception as e:
    print(f"❌ Postgres Export failed: {e}")

spark.stop()