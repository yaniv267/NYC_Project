from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# 1. הקונפיגורציה שלך לחיבור ל-MinIO
spark = SparkSession.builder \
    .appName("NYC_Data_Cleaning_Final") \
    .config("spark.driver.memory", "12g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.jars.packages",
     "org.apache.hadoop:hadoop-aws:3.3.4,"
     "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
    .getOrCreate()

# 2. קריאת נתוני הדוחות מהנתיב שציינת
df_kafka_raw = spark.read.parquet("s3a://spark/nyc_parking_raw.parquet")

# 3. שלב הניקוי והנרמול (Standardization)
# כאן אנחנו מאחדים את כל הקיצורים שמצאנו ב-Distinct לתוך 5 הרובעים הרשמיים
df_final = df_kafka_raw.withColumn("borough_name", 
    when(col("violation_county").isin("K", "BK", "Kings"), "Brooklyn")
    .when(col("violation_county").isin("Q", "QN", "Qns", "Queens", "Q"), "Queens")
    .when(col("violation_county").isin("NY", "MN", "Manhattan"), "Manhattan")
    .when(col("violation_county").isin("BX", "Bronx"), "Bronx")
    .when(col("violation_county").isin("R", "ST", "Rich", "Staten Island"), "Staten Island")
    .otherwise("Other/Unknown")
)

# 4. בדיקה סופית של התוצאות
print("--- תוצאות ניקוי הרובעים ---")
df_final.groupBy("borough_name").count().orderBy(col("count").desc()).show()








































































































