from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, split, min, max, countDistinct, year, month, hour, date_format
from nyc_schema import silver_traffic_schema 

# 1. אתחול
spark = (SparkSession.builder
    .appName("NYC_Traffic_Silver_Processing")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# 2. קריאה מה-Bronze
input_path = "s3a://spark/bronze/nyc_traffic/"
df_bronze = spark.read.parquet(input_path)

# 3. טרנספורמציה
df_silver = (df_bronze
    # סינון נתונים לא תקינים
    .filter((col("status") != "-101") & (col("speed").cast("float") > 0))
    .dropDuplicates(["link_id", "data_as_of"])
    
    # טיפול בזמנים
    .withColumn("event_time", to_timestamp(col("data_as_of"), "yyyy-MM-dd'T'HH:mm:ss.000"))
    .withColumn("ingestion_time", to_timestamp(col("ingested_at")))
    
    # חילוץ שדות זמן לאנליטיקה (הבוט יודה לך על זה)
    .withColumn("year", year(col("event_time")))
    .withColumn("month", month(col("event_time")))
    .withColumn("hour", hour(col("event_time")))
    .withColumn("day_name", date_format(col("event_time"), "EEEE"))
    
    # המרת סוגי נתונים
    .withColumn("speed", col("speed").cast("float"))
    .withColumn("travel_time", col("travel_time").cast("int"))
    
    # חילוץ קואורדינטות מהשדה link_points
    .withColumn("first_coords", split(col("link_points"), " ")[0])
    .withColumn("latitude", split(col("first_coords"), ",")[0].cast("double"))
    .withColumn("longitude", split(col("first_coords"), ",")[1].cast("double"))
)

# 4. אכיפת סכימה סופית (The Data Contract)
final_columns = [field.name for field in silver_traffic_schema]
df_silver_final = df_silver.select(*final_columns)

# 5. בדיקות איכות (Logs)
print(f"📊 Total observations in Silver Layer: {df_silver_final.count()}")
df_silver_final.select(min("event_time"), max("event_time")).show()

# 6. שמירה ל-Silver ב-MinIO
output_path = "s3a://spark/silver/nyc_traffic/"
(df_silver_final.write
    .mode("overwrite")
    .partitionBy("year", "month") # חלוקה למחיצות לביצועים טובים יותר
    .parquet(output_path))

print(f"✨ Silver layer for Traffic complete! Saved to: {output_path}")
df_silver_final.show(5, truncate=False)