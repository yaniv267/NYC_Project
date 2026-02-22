from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import requests
import io
import pandas as pd

# 1. הקמת Spark Session עם הגדרות MinIO
spark = SparkSession.builder \
    .appName("NYC_API_To_MinIO_With_Token") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# 2. הגדרות API
# השתמש במזהה 7vky-p6i4 עבור נתוני הכתובות העדכניים
APP_TOKEN = "gdLWTLhefvaSPLJI2AV4lTv4m"
api_url = "https://data.cityofnewyork.us/resource/7vky-p6i4.csv?$limit=100000"


headers = {
    "X-App-Token": APP_TOKEN
}

print(f"--- Fetching data from NYC API: {api_url} ---")

try:
    # 3. שליפת הנתונים בעזרת Requests
    response = requests.get(api_url, headers=headers)
    
    if response.status_code == 200:
        # 4. המרה ל-Dataframe (שימוש ב-Pandas כגשר לספארק)
        pdf = pd.read_csv(io.StringIO(response.text))
        print(f"Received {len(pdf)} rows. Columns: {pdf.columns.tolist()}")
        
        # 5. יצירת Spark DataFrame
        spark_df = spark.createDataFrame(pdf)
        
        # 6. בחירת עמודות והתאמה לפרויקט שלך
        # אנחנו מוודאים שהעמודות תואמות למה שאתה מחפש בסקריפט ה-Kafka שלך
        final_df = spark_df.select(
            col("house_number"),
            col("street_name"),
            col("borough").alias("violation_county"),
            col("latitude").cast("double").alias("lat"),
            col("longitude").cast("double").alias("lon")
        )
        
        # 7. כתיבה ל-MinIO בפורמט Parquet
        print("--- Writing to MinIO (geo_cache.parquet) ---")
        final_df.write.mode("overwrite").parquet("s3a://spark/reference/geo_cache.parquet")
        print("--- SUCCESS! Your cache is now ready in MinIO ---")
        
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        print(f"Response error: {response.text}")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    spark.stop()