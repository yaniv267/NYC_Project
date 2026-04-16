# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, to_timestamp, concat, lit
# from nyc_schema import crashes_silver_schema

# # 1. יצירת Spark Session
# spark = SparkSession.builder \
#     .appName("NYC_Crashes_Silver_Processing") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
#     .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .getOrCreate()

# # 2. קריאה מה-Bronze
# input_path = "s3a://spark/bronze/nyc_crashes/"
# df_bronze = spark.read.parquet(input_path)

# print(f"📦 Starting transformation for {df_bronze.count()} records...")

# # 3. טרנספורמציה וניקוי (שימוש בסוגריים במקום לוכסנים למניעת שגיאות הזחה)
# df_silver = (df_bronze
#     .dropDuplicates(["collision_id"])
#     .filter(col("latitude").isNotNull() & col("longitude").isNotNull())
    
#     # איחוד תאריך ושעה - H:mm פותר את השגיאה של שעות עם ספרה אחת
#     .withColumn("crash_timestamp", to_timestamp(
#         concat(col("crash_date").substr(1,10), lit(" "), col("crash_time")), 
#         "yyyy-MM-dd H:mm"
#     ))
    
#     # המרת שדות מספרים
#     .withColumn("total_injured", col("number_of_persons_injured").cast("int"))
#     .withColumn("total_killed", col("number_of_persons_killed").cast("int"))
#     .withColumn("pedestrians_injured", col("number_of_pedestrians_injured").cast("int"))
#     .withColumn("cyclist_injured", col("number_of_cyclist_injured").cast("int"))
#     .withColumn("motorist_injured", col("number_of_motorist_injured").cast("int"))
    
#     # המרת מיקום וזמן הזרקה
#     .withColumn("latitude", col("latitude").cast("double"))
#     .withColumn("longitude", col("longitude").cast("double"))
#     .withColumn("ingestion_time", to_timestamp(col("ingested_at")))
# )

# # 4. בחירת עמודות סופיות
# df_silver_final = df_silver.select(
#     col("collision_id"),
#     col("crash_timestamp"),
#     col("latitude"),
#     col("longitude"),
#     col("on_street_name"),
#     col("total_injured"),
#     col("total_killed"),
#     col("pedestrians_injured"),
#     col("cyclist_injured"),
#     col("motorist_injured"),
#     col("contributing_factor_vehicle_1").alias("contributing_factor"),
#     col("vehicle_type_code1").alias("vehicle_type"),
#     col("ingestion_time"),
#     col("borough")
# )

# # 5. בדיקות והדפסה
# print("-" * 30)
# print(f"📊 Total Observations in Crashes Silver: {df_silver_final.count()}")
# df_silver_final.selectExpr("min(crash_timestamp) as earliest", "max(crash_timestamp) as latest").show()
# print("-" * 30)

# # 6. שמירה ל-Silver ב-MinIO
# output_path = "s3a://spark/silver/nyc_crashes/"
# df_silver_final.write.mode("overwrite").parquet(output_path)

# print(f"✅ Silver layer complete! Saved to: {output_path}")
# df_silver_final.show(5, truncate=False)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, concat, lit
from nyc_schema import silver_crashes_schema # וודא שהסכימה כאן תואמת לעמודות הסופיות

# 1. יצירת Spark Session
spark = SparkSession.builder \
    .appName("NYC_Crashes_Silver_Processing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
    
# ביטול לוגים מיותרים
spark.sparkContext.setLogLevel("WARN")

# 2. קריאה מה-Bronze (Batch)
input_path = "s3a://spark/bronze/nyc_crashes/"
df_bronze = spark.read.parquet(input_path)

# 3. טרנספורמציה וניקוי
df_silver = (df_bronze
    .dropDuplicates(["collision_id"])
    .filter(col("latitude").isNotNull() & col("longitude").isNotNull())
    
    # איחוד תאריך ושעה - שים לב: משתמשים ב-CRASH_DATE המקורי מהסכימה
    .withColumn("crash_timestamp", to_timestamp(
        concat(col("CRASH_DATE"), lit(" "), col("CRASH_TIME")), 
        "MM/dd/yyyy H:mm"
    ))
    
    # המרת שדות מספרים (שימוש בשמות מהסכימה המקורית)
    .withColumn("total_injured", col("NUMBER_OF_PERSONS_INJURED").cast("int"))
    .withColumn("total_killed", col("NUMBER_OF_PERSONS_KILLED").cast("int"))
    .withColumn("pedestrians_injured", col("NUMBER_OF_PEDESTRIANS_INJURED").cast("int"))
    .withColumn("cyclist_injured", col("NUMBER_OF_CYCLIST_INJURED").cast("int"))
    .withColumn("motorist_injured", col("NUMBER_OF_MOTORIST_INJURED").cast("int"))
    
    # המרת מיקום וזמן הזרקה (תיקון שם העמודה ל-ingestion_timestamp)
    .withColumn("latitude", col("latitude").cast("double"))
    .withColumn("longitude", col("longitude").cast("double"))
    .withColumn("ingestion_time", col("ingestion_timestamp")) 
)

# 4. בחירת עמודות סופיות (שמות נקיים ל-Silver)
df_silver_processed = df_silver.select(
    col("collision_id"),
    col("crash_timestamp"),
    col("year"),      # חשוב שיופיעו כאן
    col("month"),
    col("latitude"),
    col("longitude"),
    col("on_street_name"),
    col("total_injured"),
    col("total_killed"),
    col("pedestrians_injured"),
    col("cyclist_injured"),
    col("motorist_injured"),
    col("CONTRIBUTING_FACTOR_VEHICLE_1").alias("contributing_factor"),
    col("VEHICLE_TYPE_CODE1").alias("vehicle_type"),
    col("ingestion_time"),
    col("BOROUGH").alias("borough")
)



# 5. שמירה ל-Silver ב-MinIO
output_path = "s3a://spark/silver/nyc_crashes/"


final_columns = [field.name for field in silver_crashes_schema]
df_silver_final = df_silver_processed.select(*final_columns)

# כדאי להוסיף partitionBy גם כאן אם הדאטה גדול
df_silver_final.write \
    .mode("overwrite") \
    .partitionBy("year", "month")\
    .parquet(output_path)

print(f"Silver layer complete! Saved to: {output_path}")
df_silver_final.show(5, truncate=False)