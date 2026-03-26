import requests
import json
import gc
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim

# 1. הקמת ה-Spark Session
spark = SparkSession.builder \
    .appName("dim_adress") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

base_url = "https://data.cityofnewyork.us/resource/inkn-q76z.json"
output_path = "s3a://spark/data/dims/dim_adress"

chunk_size = 30000
offset = 0

print(f"--- Starting Download of NYC Address Points (inkn-q76z) ---")

while offset < 1300000:
    url = f"{base_url}?$limit={chunk_size}&$offset={offset}"
    print(f"Fetching rows {offset} to {offset + chunk_size}...")
    
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        if not data:
            print("--- No more data found. ---")
            break
            
        # המרה ל-DataFrame
        json_rdd = spark.sparkContext.parallelize([json.dumps(record) for record in data])
        df_chunk = spark.read.json(json_rdd)
        
        # --- שים לב: כל השורות הבאות חייבות להיות באותה רמת הזחה של ה-try ---
        df_final = df_chunk.select(
            col("objectid").alias("address_id"),
            upper(trim(col("full_street_name"))).alias("full_address"),
            col("l_zip").alias("zip_code"),
            col("boroughcode").alias("borough_code"),
            col("the_geom.coordinates")[0][0][0].cast("double").alias("longitude"),
            # רמה 0: הקו הראשון | רמה 0: הנקודה הראשונה | רמה 1: Latitude
            col("the_geom.coordinates")[0][0][1].cast("double").alias("latitude")
        )
        
         
        print("DEBUG: Data with coordinates:")
        df_final.show(5, truncate=False)
        
        # שמירה ל-MinIO
        df_final.write.mode("append").parquet(output_path)
        
        print(f"✅ Batch at offset {offset} saved successfully.")

        # ניקוי זיכרון
        del data, df_chunk, df_final
        gc.collect()
        
        offset += chunk_size
        
    except Exception as e:
        print(f"⚠️ Error at offset {offset}: {e}. Retrying in 10 seconds...")
        import time
        time.sleep(10)
        continue

print(f"✅ Job Finished! All address points are in: {output_path}")