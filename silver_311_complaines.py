# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from nyc_schema import silver_311_schema # וודא שהסכימה מוגדרת בקובץ nyc_schema.py

# # 1. יצירת Spark Session
# spark = SparkSession.builder \
#     .appName("NYC_311_Silver_Processing") \
#     .config("spark.jars.packages", 
#     "org.apache.hadoop:hadoop-aws:3.3.4,"
#      "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
#     .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .getOrCreate()

# # 2. קריאה מה-Bronze
# print("📖 Reading data from Bronze layer...")
# df_bronze = spark.read.parquet("s3a://spark/bronze/311_complaints/")

# # 3. תהליך הניקוי + ניהול היסטוריה (24 חודשים)
# retention_limit = F.add_months(F.current_date(), -24)

# df_cleaned = (df_bronze
#     .dropDuplicates(["unique_key"])
#     .withColumn("created_date", F.to_timestamp("created_date", "yyyy-MM-dd'T'HH:mm:ss.000"))
#     # סינון היסטוריה: רק נתונים רלוונטיים שנתיים אחורה
#     .filter(F.col("created_date") >= retention_limit)
#     # סטנדרטיזציה: טקסט ב-Uppercase וניקוי רווחים
#     .withColumn("complaint_type", F.trim(F.upper(F.col("complaint_type"))))
#     .withColumn("borough", F.trim(F.upper(F.col("borough"))))
#     .withColumn("city", F.trim(F.upper(F.col("city"))))
#     .withColumn("street_name", F.trim(F.upper(F.col("street_name"))))
#     # המרת טיפוסים מספריים
#     .withColumn("latitude", F.col("latitude").cast("double"))
#     .withColumn("longitude", F.col("longitude").cast("double"))
#     # חילוץ עמודות זמן לפרטישנים ותובנות
#     .withColumn("year", F.year(F.col("created_date")))
#     .withColumn("month", F.month(F.col("created_date")))
#     .withColumn("hour", F.hour(F.col("created_date")))
#     .withColumn("day_name", F.date_format(F.col("created_date"), "EEEE"))
#     # סינון רשומות פגומות בבסיסן
#     .filter(F.col("unique_key").isNotNull() & F.col("created_date").isNotNull())
# )

# # 4. לוגיקה דינמית: התאמה לסכימה (Defensive Selection)
# # המנגנון הזה מבטיח שהסקריפט לא יישבר אם חסרה עמודה בברונז
# existing_columns = df_cleaned.columns
# final_selection = []

# for field in silver_311_schema:
#     if field.name in existing_columns:
#         final_selection.append(F.col(field.name))
#     else:
#         # אם העמודה חסרה בסכימה של הברונז, ניצור עמודת Null בטיפוס הנכון
#         print(f"⚠️ Warning: Column {field.name} missing in Bronze, filling with NULL")
#         final_selection.append(F.lit(None).cast(field.dataType).alias(field.name))

# # הוספת עמודות ה-Partition (שנה וחודש) לבחירה הסופית
# # וודא שהן קיימות או מחושבות
# df_silver_final = df_cleaned.select(*final_selection)

# print("👀 Preview of Silver Data (Top 5 rows):")
# df_silver_final.show(5, truncate=False)

# # 5. כתיבה ל-Silver עם Partitioning
# print("💾 Saving to Silver layer: s3a://spark/silver/311_complaints/ ...")
# (df_silver_final.write 
#     .mode("overwrite") 
#     .partitionBy("year", "month") 
#     .parquet("s3a://spark/silver/311_complaints/"))

# print("✅ Silver Process Complete! Data is cleaned, normalized, and stored.")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
from nyc_schema import silver_311_schema

# =========================
# 2. INITIALIZE SPARK SESSION
# =========================

spark = (SparkSession.builder
    .appName("NYC_311_Silver_Final")
    .config("spark.jars.packages", 
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# =========================
# 3. READ RAW DATA FROM BRONZE
# =========================

print("📖 Reading 311 Bronze data...")
df_bronze = spark.read.parquet("s3a://spark/bronze/311_complaints/")
df_bronze.select("created_date").show(20, False)

# ==========================================
# 4. DATA CLEANING & STANDARDIZATION
# ==========================================
# Applying 24-month data retention policy

retention_limit = F.add_months(F.current_date(), -24)

df_transformed = (df_bronze
    .dropDuplicates(["unique_key"])
    .withColumn("created_date", F.to_timestamp("created_date", "yyyy-MM-dd'T'HH:mm:ss.000"))
    .filter(F.col("created_date") >= retention_limit)
    .withColumn("agency", F.trim(F.upper(F.col("agency"))))
    .withColumn("complaint_type", F.trim(F.upper(F.col("complaint_type"))))
    .withColumn("descriptor", F.trim(F.upper(F.col("descriptor"))))
    .withColumn("city", F.trim(F.upper(F.col("city"))))
    .withColumn("borough", F.trim(F.upper(F.col("borough"))))
    .withColumn("street_name", F.trim(F.upper(F.col("street_name"))))
    .withColumn("incident_zip", F.trim(F.col("incident_zip")))
    .withColumn("latitude", F.col("latitude").cast("double"))
    .withColumn("longitude", F.col("longitude").cast("double"))
    .withColumn("year", F.year(F.col("created_date")).cast("int"))
    .withColumn("month", F.month(F.col("created_date")).cast("int"))
    .withColumn("hour", F.hour(F.col("created_date")).cast("int"))
    .withColumn("day_name", F.date_format(F.col("created_date"), "EEEE"))
)

# ==========================================
# 5. SCHEMA ALIGNMENT & DATA CONTRACT
# ==========================================
# Selecting columns based on the silver_311_schema definition

schema_columns = [field.name for field in silver_311_schema]
df_silver_final = df_transformed.select(*schema_columns)

# ==========================================
# 6. WRITE TO SILVER LAYER (PARQUET)
# ==========================================

output_path = "s3a://spark/silver/311_complaints/"
print(f"💾 Saving data to Silver layer (Partitioned by year/month): {output_path}")

(df_silver_final.write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(output_path))

# ==========================================
# 7. FINAL VALIDATION & REPORTING
# ==========================================
print("\n" + "="*50)
print("✅ SILVER PROCESSING COMPLETE")
print(f"Target: {output_path}")
print("="*50)
df_silver_final.show(5, truncate=False)