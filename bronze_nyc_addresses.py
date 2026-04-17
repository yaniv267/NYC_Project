import urllib.request
import os
from pyspark.sql import SparkSession

# =========================
# 1. SPARK SESSION
# =========================
spark = SparkSession.builder \
    .appName("bronze_nyc_addresses_automated") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =========================
# 2. CONFIGURATION & URLS
# =========================
# הקישור הרשמי ל-Dump המלא של העירייה
csv_url = "https://data.cityofnewyork.us/api/views/uf93-f8nk/rows.csv?accessType=DOWNLOAD"

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