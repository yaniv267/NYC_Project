from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, concat, lit
from nyc_schema import crashes_silver_schema

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

# 2. קריאה מה-Bronze
input_path = "s3a://spark/bronze/nyc_crashes/"
df_bronze = spark.read.parquet(input_path)

print(f"📦 Starting transformation for {df_bronze.count()} records...")

# 3. טרנספורמציה וניקוי (שימוש בסוגריים במקום לוכסנים למניעת שגיאות הזחה)
df_silver = (df_bronze
    .dropDuplicates(["collision_id"])
    .filter(col("latitude").isNotNull() & col("longitude").isNotNull())
    
    # איחוד תאריך ושעה - H:mm פותר את השגיאה של שעות עם ספרה אחת
    .withColumn("crash_timestamp", to_timestamp(
        concat(col("crash_date").substr(1,10), lit(" "), col("crash_time")), 
        "yyyy-MM-dd H:mm"
    ))
    
    # המרת שדות מספרים
    .withColumn("total_injured", col("number_of_persons_injured").cast("int"))
    .withColumn("total_killed", col("number_of_persons_killed").cast("int"))
    .withColumn("pedestrians_injured", col("number_of_pedestrians_injured").cast("int"))
    .withColumn("cyclist_injured", col("number_of_cyclist_injured").cast("int"))
    .withColumn("motorist_injured", col("number_of_motorist_injured").cast("int"))
    
    # המרת מיקום וזמן הזרקה
    .withColumn("latitude", col("latitude").cast("double"))
    .withColumn("longitude", col("longitude").cast("double"))
    .withColumn("ingestion_time", to_timestamp(col("ingested_at")))
)

# 4. בחירת עמודות סופיות
df_silver_final = df_silver.select(
    col("collision_id"),
    col("crash_timestamp"),
    col("latitude"),
    col("longitude"),
    col("on_street_name"),
    col("total_injured"),
    col("total_killed"),
    col("pedestrians_injured"),
    col("cyclist_injured"),
    col("motorist_injured"),
    col("contributing_factor_vehicle_1").alias("contributing_factor"),
    col("vehicle_type_code1").alias("vehicle_type"),
    col("ingestion_time")
)

# 5. בדיקות והדפסה
print("-" * 30)
print(f"📊 Total Observations in Crashes Silver: {df_silver_final.count()}")
df_silver_final.selectExpr("min(crash_timestamp) as earliest", "max(crash_timestamp) as latest").show()
print("-" * 30)

# 6. שמירה ל-Silver ב-MinIO
output_path = "s3a://spark/silver/nyc_crashes/"
df_silver_final.write.mode("overwrite").parquet(output_path)

print(f"✅ Silver layer complete! Saved to: {output_path}")
df_silver_final.show(5, truncate=False)