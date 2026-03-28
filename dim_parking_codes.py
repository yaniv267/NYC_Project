import requests
import json
import gc
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim

# אתחול Spark Session
spark = SparkSession.builder \
    .appName("ingest_parking_codes") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# ה-API של קודי הדוחות
base_url = "https://data.cityofnewyork.us/resource/ncbg-6agr.json"
output_path = "s3a://spark/data/dims/dim_parking_violation_codes"

print(f"--- Downloading Parking Violation Codes ---")

try:
    response = requests.get(base_url, timeout=60)
    response.raise_for_status()
    data = response.json()

# המרה ל-DataFrame
    json_rdd = spark.sparkContext.parallelize([json.dumps(record) for record in data])
    df_raw = spark.read.json(json_rdd)
    

    
    # --- תיקון שמות העמודות לפי ה-SODA API ---
    df_final = df_raw.select(
            col("code").alias("violation_code"), # המזהה של הדוח
            upper(trim(col("definition"))).alias("violation_descr"), # התיאור
            col("manhattan_96th_st_below").cast("double"), # קנס באזור יקר
            col("all_other_areas").cast("double") # קנס בשאר העיר
        )

# תצוגה לבדיקה - כאן תראה את הטקסטים והמחירים
    print("DEBUG: Parking Codes Map:")
    df_final.show(10, truncate=False)
    

    df_final.write.mode("overwrite").parquet(output_path)
    
    print(f"✅ Successfully saved parking codes to: {output_path}")

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    spark.stop()