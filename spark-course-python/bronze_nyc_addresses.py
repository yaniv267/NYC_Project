# import requests
# import json
# import gc
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, upper, trim, concat_ws # הוספנו concat_ws

# # 1. הקמת ה-Spark Session
# spark = SparkSession.builder \
#     .appName("ingest_address_points_final") \
#     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
#     .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .getOrCreate()

# base_url = "https://data.cityofnewyork.us/resource/uf93-f8nk.json"
# output_path = "s3a://spark/data/dims/dim_address_points"

# chunk_size = 30000 
# offset = 0

# print(f"--- Starting Download of NYC Address Points (ID: uf93-f8nk) ---")

# while offset < 1300000:
#     url = f"{base_url}?$limit={chunk_size}&$offset={offset}"
#     print(f"\n--- Fetching rows {offset} to {offset + chunk_size} ---")
    
#     try:
#         response = requests.get(url, timeout=60)
#         response.raise_for_status()
#         data = response.json()
        
#         if not data:
#             break
            
#         json_rdd = spark.sparkContext.parallelize([json.dumps(record) for record in data])
#         df_chunk = spark.read.json(json_rdd)
        
#         # --- תיקון שמות העמודות לפי מה ש-Spark מצא בלוג שלך ---
#         df_final = df_chunk.select(
#             col("addresspointid").alias("address_id"),
#             # מחברים מספר בית + שם רחוב לכתובת אחת
#             upper(trim(concat_ws(" ", col("house_number"), col("full_street_name")))).alias("full_address"),
#             col("zipcode").alias("zip_code"),
#             col("boroughcode").alias("borough_code"),
#             # Longitude הוא האיבר הראשון במערך (אינדקס 0)
#             col("the_geom.coordinates")[0].cast("double").alias("longitude"),
#             # Latitude הוא האיבר השני במערך (אינדקס 1)
#             col("the_geom.coordinates")[1].cast("double").alias("latitude")
#         )
        
        
#         # תצוגה לבדיקה - כאן נראה אם הצלחנו
#         print(f"DEBUG: Data Preview for offset {offset}:")
#         df_final.show(10, truncate=False)
        
#         # שמירה ל-MinIO
#         mode = "overwrite" if offset == 0 else "append"
#         df_final.write.mode(mode).parquet(output_path)
        
#         print(f"✅ Batch {offset} saved successfully.")

#         del data, df_chunk, df_final
#         gc.collect()
#         offset += chunk_size
        
#     except Exception as e:
#         print(f"⚠️ Error at offset {offset}: {e}")
#         import time
#         time.sleep(10)
#         continue

# print(f"✅ Job Finished!")

# import requests
# import json
# import gc
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, upper, trim, concat_ws

# # 1. הקמת ה-Spark Session
# spark = SparkSession.builder \
#     .appName("ingest_address_points_final") \
#     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
#     .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .getOrCreate()

# # --- שינוי נתיב ליעד הנכון ---
# base_url = "https://data.cityofnewyork.us/resource/uf93-f8nk.json"
# output_path = "s3a://spark/reference/addresses/" # הנתיב שבו הסקריפטים האחרים מחפשים

# chunk_size = 50000 # הגדלתי קצת את הקצב
# offset = 0

# print(f"🚀 Starting Download: NYC Address Points to {output_path}")

# while offset < 1300000:
#     url = f"{base_url}?$limit={chunk_size}&$offset={offset}"
#     print(f"📦 Fetching rows {offset} to {offset + chunk_size}...")
    
#     try:
#         response = requests.get(url, timeout=60)
#         response.raise_for_status()
#         data = response.json()
        
#         if not data:
#             break
            
#         json_rdd = spark.sparkContext.parallelize([json.dumps(record) for record in data])
#         df_chunk = spark.read.json(json_rdd)
        
#         # --- עיבוד הנתונים ---
#         df_final = df_chunk.select(
#             col("addresspointid").alias("address_id"),
#             # שומרים את שם הרחוב הנקי (קריטי ל-Join עם ה-Gold!)
#             upper(trim(col("full_street_name"))).alias("street_name_clean"),
#             # כתובת מלאה (מספר + רחוב)
#             upper(trim(concat_ws(" ", col("house_number"), col("full_street_name")))).alias("full_address"),
#             col("zipcode").alias("zip_code"),
#             # חילוץ קואורדינטות מהמבנה הגיאומטרי
#             col("the_geom.coordinates")[0].cast("double").alias("longitude"),
#             col("the_geom.coordinates")[1].cast("double").alias("latitude")
#         )
        
#         # שמירה בפורמט Parquet
#         # בבאץ' הראשון דורסים (overwrite), בשאר מוסיפים (append)
#         write_mode = "overwrite" if offset == 0 else "append"
#         df_final.write.mode(write_mode).parquet(output_path)
        
#         print(f"✅ Saved batch {offset}. Sample: {df_final.select('full_address').first()}")

#         # ניקוי זיכרון
#         del data, df_chunk, df_final
#         gc.collect()
#         offset += chunk_size
        
#     except Exception as e:
#         print(f"⚠️ Error at offset {offset}: {e}")
#         import time
#         time.sleep(5)
#         continue

# print(f"🏁 Mission Accomplished! Data is waiting for you in {output_path}")

import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, upper, trim
import os
os.environ["PYSPARK_SUBMIT_ARGS"] = "--driver-memory 4g pyspark-shell"


spark = SparkSession.builder \
    .appName("bronze_nyc_addresses") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

base_url = "https://data.cityofnewyork.us/resource/uf93-f8nk.json"
output_path = "s3a://spark/bronze/addresses/"
chunk_size = 50000
offset = 0

print(f"🚀 Starting Raw Ingestion to Bronze...")

while offset < 1300000:
    url = f"{base_url}?$limit={chunk_size}&$offset={offset}"
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        if not data: break
            
        # שמירה כ-JSON גולמי בלי עיבוד
#        json_rdd = spark.sparkContext.parallelize([json.dumps(record) for record in data])
#        df_raw = spark.read.json(json_rdd)
        
#        write_mode = "overwrite" if offset == 0 else "append"
#        df_raw.write.mode(write_mode).json(output_path)

# יצירת DataFrame ישירות מהרשימה (Best Practice - חוסך המרות מיותרות לטקסט ול-RDD)
        df_raw = spark.createDataFrame(data)
        
        write_mode = "overwrite" if offset == 0 else "append"
        # שמירה בפורמט עמודתי דחוס
        df_raw.write.mode(write_mode).parquet(output_path)
        
        print(f"✅ Ingested batch {offset}")
        offset += chunk_size
        
    except Exception as e:
        print(f"⚠️ Error at offset {offset}: {e}")
        break

print(f"🏁 Bronze Layer Complete.")