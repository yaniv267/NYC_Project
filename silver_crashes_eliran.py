from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, concat, lit, date_format,month,year
from nyc_schema import silver_crashes_schema 

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
df_silver = (
    df_bronze
    .dropDuplicates(["collision_id"])
    .filter(col("latitude").isNotNull() & col("longitude").isNotNull())

    # ⏰ timestamp
    .withColumn("date_only", to_timestamp(col("crash_date"))) \
    .withColumn(
    "crash_timestamp",
    to_timestamp(
        concat(
            date_format(col("date_only"), "yyyy-MM-dd"),
            lit(" "),
            col("crash_time")
        ),
        "yyyy-MM-dd H:mm"
    )
)
     # 🧠 time features
    .withColumn("year", year(col("crash_timestamp")))
    .withColumn("month", month(col("crash_timestamp")))
    .withColumn("day_of_week", date_format(col("crash_timestamp"), "EEEE"))

    # 👥 injuries
    .withColumn("total_injured", col("NUMBER_OF_PERSONS_INJURED").cast("int"))
    .withColumn("total_killed", col("NUMBER_OF_PERSONS_KILLED").cast("int"))
    .withColumn("pedestrians_injured", col("NUMBER_OF_PEDESTRIANS_INJURED").cast("int"))
    .withColumn("cyclist_injured", col("NUMBER_OF_CYCLIST_INJURED").cast("int"))
    .withColumn("motorist_injured", col("NUMBER_OF_MOTORIST_INJURED").cast("int"))

    # 📍 location
    .withColumn("latitude", col("latitude").cast("double"))
    .withColumn("longitude", col("longitude").cast("double"))

    # 🚗 causes
    .withColumn("contributing_factor", col("CONTRIBUTING_FACTOR_VEHICLE_1"))
    .withColumn("vehicle_type", col("VEHICLE_TYPE_CODE1"))

    # ⏱ system
    .withColumn("ingestion_time", col("ingestion_timestamp"))
)
# 4. בחירת עמודות סופיות
df_silver_final =df_silver.select(
    "collision_id",
    "crash_timestamp",
    "year",
    "month",
    "day_of_week",
    "latitude",
    "longitude",
    "on_street_name",
    "total_injured",
    "total_killed",
    "pedestrians_injured",
    "cyclist_injured",
    "motorist_injured",
    "contributing_factor",
    "vehicle_type",
    "ingestion_time",
    col("BOROUGH").alias("borough")
)

# 5. בדיקות והדפסה
print("-" * 30)
print(f"📊 Total Observations in Crashes Silver: {df_silver_final.count()}")
# df_silver_final.selectExpr("min(crash_timestamp) as earliest", "max(crash_timestamp) as latest").show()
print("-" * 30)

# 6. שמירה ל-Silver ב-MinIO
output_path = "s3a://spark/silver/nyc_crashes/"
df_silver_final = df_silver_final.select(*[f.name for f in silver_crashes_schema])

df_silver_final.write.mode("overwrite").parquet(output_path)

print(f"✅ Silver layer complete! Saved to: {output_path}")
df_silver_final.show(5, truncate=False)