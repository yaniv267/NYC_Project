# import requests
# import json
from pyspark.sql import SparkSession
# from pyspark.sql.functions import broadcast, upper, trim
# import os
# os.environ["PYSPARK_SUBMIT_ARGS"] = "--driver-memory 4g pyspark-shell"
import urllib.request
import os



spark = SparkSession.builder \
    .appName("bronze_nyc_addresses") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "2g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()


# base_url = "https://data.cityofnewyork.us/resource/uf93-f8nk.json"
csv_url = "https://data.cityofnewyork.us/api/views/uf93-f8nk/rows.csv?accessType=DOWNLOAD"
output_path = "s3a://spark/bronze/addresses/"


# שימוש בנתיב זמני גנרי (עובד גם בלינוקס/דוקר וגם בווינדוס)
# זה מבטיח שהקוד ירוץ על כל מחשב או שרת
temp_dir = "/tmp" if os.name != 'nt' else os.environ.get('TEMP', 'C:\\temp')
local_csv_file = os.path.join(temp_dir, "nyc_addresses_full_dump.csv")

output_path = "s3a://spark/bronze/addresses/"

print("🚀 Starting Automated Ingestion Pipeline...")
print(f"📥 Downloading full dataset to temporary generic path: {local_csv_file}")

# =========================
# 3. DOWNLOAD RAW CSV
# =========================
try:
    urllib.request.urlretrieve(csv_url, local_csv_file)
    print("✅ Download completed successfully!")
except Exception as e:
    print(f"❌ Failed to download file: {e}")
    spark.stop()
    exit()

# =========================
# 4. LOAD TO SPARK & WRITE TO MINIO
# =========================
print("💾 Loading CSV to Spark and writing to Bronze layer in MinIO...")

# קריאת הקובץ הזמני
df_raw = spark.read.csv(local_csv_file, header=True, inferSchema=True)

# העלאה ל-MinIO בפורמט Parquet
df_raw.repartition(1).write.mode("overwrite").parquet(output_path)

total_rows = df_raw.count()
print(f"🏁 Bronze Layer Complete. Total records written: {total_rows}")

# =========================
# 5. CLEANUP
# =========================
# מחיקת הקובץ הזמני מהשרת כדי למנוע הצטברות זבל
if os.path.exists(local_csv_file):
    os.remove(local_csv_file)
    print("🧹 Temporary files cleaned up.")
    
spark.stop()

# chunk_size = 50000
# offset = 0

# print(f"🚀 Starting Raw Ingestion to Bronze...")

# while offset < 1300000:
#     url = f"{base_url}?$limit={chunk_size}&$offset={offset}"
#     try:
#         response = requests.get(url, timeout=60)
#         response.raise_for_status()
#         data = response.json()
        
#         if not data: break
            
#         # שמירה כ-JSON גולמי בלי עיבוד
# #        json_rdd = spark.sparkContext.parallelize([json.dumps(record) for record in data])
# #        df_raw = spark.read.json(json_rdd)
        
# #        write_mode = "overwrite" if offset == 0 else "append"
# #        df_raw.write.mode(write_mode).json(output_path)

# # יצירת DataFrame ישירות מהרשימה (Best Practice - חוסך המרות מיותרות לטקסט ול-RDD)
#         df_raw = spark.createDataFrame(data)
        
#         write_mode = "overwrite" if offset == 0 else "append"
#         # שמירה בפורמט עמודתי דחוס
#         df_raw.write.mode(write_mode).parquet(output_path)
        
#         print(f"✅ Ingested batch {offset}")
#         offset += chunk_size
        
#     except Exception as e:
#         print(f"⚠️ Error at offset {offset}: {e}")
#         break

# print(f"🏁 Bronze Layer Complete.")