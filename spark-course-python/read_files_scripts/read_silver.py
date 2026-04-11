from pyspark.sql import SparkSession

# 1. יצירת Spark Session (עם חיבור ל-MinIO)
spark = SparkSession.builder \
    .appName("NYC_Parking_Read_Silver") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# העלמת הודעות לוג מיותרות כדי שהטרמינל יהיה נקי וקריא
spark.sparkContext.setLogLevel("ERROR")

# 2. קריאת הנתונים משכבת ה-Silver
silver_path = "s3a://spark/silver/nyc_parking_violation/"
print(f"Reading data from: {silver_path}...\n")

df_silver = spark.read.parquet(silver_path)

# 3. הצגת התוצאות
print("=== Data Schema ===")
df_silver.printSchema()

print("\n=== Sample Data (First 10 Rows) ===")
# truncate=False גורם לכך שהעמודות לא ייחתכו אם הטקסט ארוך
df_silver.show(10, truncate=False)

print("\n=== Total Records ===")
total_count = df_silver.count()
print(f"Total rows in Silver layer: {total_count}")

# סגירת הסשן בצורה מסודרת
spark.stop()