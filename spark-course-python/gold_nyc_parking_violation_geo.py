from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, avg, current_timestamp, split, regexp_replace, trim, upper

# 1. אתחול Spark Session
spark = (SparkSession.builder \
.appName("NYC_Parking_Gold_Volume_Report") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate())



# 2. לוגיקת נרמול שמות רחובות (הגשר שמחבר בין המקורות)
def normalize_street_logic(c):
    c = upper(trim(c))
    # הסרת כיוונים (NB, SB וכו')
    c = regexp_replace(c, r"^(NB|SB|EB|WB|N/B|S/B|E/B|W/B)\s+", "")
    # האחדת מספרים (164TH -> 164)
    c = regexp_replace(c, r"(\d+)(ST|ND|RD|TH)", r"$1")
    # קיצורים סטנדרטיים
    c = regexp_replace(c, r"\bAVENUE\b", "AVE")
    c = regexp_replace(c, r"\bSTREET\b", "ST")
    c = regexp_replace(c, r"\bBOULEVARD\b|\bBLV\b", "BLVD")
    c = regexp_replace(c, r"\bPARKWAY\b", "PKWY")
    return c

print("🚀 Starting Gold Pipeline and calculating counts...")

# 3. טעינת נתונים גולמיים לצורך הדו"ח
silver_df = spark.read.parquet("s3a://spark/silver/nyc_parking_violation/")
addresses_df = spark.read.parquet("s3a://spark/reference/addresses/")

# 4. חישוב הנתונים לדו"ח (Volume Report)
total_tickets = silver_df.count()
unique_streets_tickets = silver_df.select("street_name").distinct().count()

total_addresses = addresses_df.count()
unique_streets_addresses = addresses_df.select("street_name_clean").distinct().count()

# הצגת הדו"ח למסך
print("\n" + "📈" + "="*50)
print("📋 FINAL DATA VOLUME REPORT")
print("="*50)
print(f"🎫 TOTAL TICKETS (Silver):        {total_tickets:,} rows")
print(f"🛣️ UNIQUE STREETS IN TICKETS:    {unique_streets_tickets:,} streets")
print("-" * 50)
print(f"🏠 TOTAL ADDRESS POINTS (S3):    {total_addresses:,} rows")
print(f"🗺️ UNIQUE STREETS IN ADDRESSES:  {unique_streets_addresses:,} streets")
print("="*50 + "\n")

# 5. עיבוד הנתונים: חישוב קואורדינטות ממוצעות לכל רחוב
address_master = (addresses_df
    .withColumn("norm_addr", normalize_street_logic(col("street_name_clean")))
    .groupBy("norm_addr")
    .agg(avg("latitude").alias("lat"), avg("longitude").alias("lon")))

# 6. יצירת טבלת ה-Gold המועשרת
gold_final = (silver_df
    .withColumn("street_clean", split(col("street_name"), "@")[0])
    .withColumn("norm_viol", normalize_street_logic(col("street_clean")))
    .groupBy("street_name", "norm_viol", "violation_code")
    .agg(count("summons_number").alias("tickets_count"))
    .join(address_master, col("norm_viol") == col("norm_addr"), "left")
    .select(
        "street_name", 
        "violation_code", 
        "tickets_count", 
        col("lat").cast("double"), 
        col("lon").cast("double")
    )
    .withColumn("last_updated", current_timestamp()))

# 7. שמירה ל-Postgres (הקפד להריץ את ה-CREATE DATABASE קודם!)
jdbc_url = "jdbc:postgresql://postgres:5432/nyc_data"
db_props = {"user": "user", "password": "password", "driver": "org.postgresql.Driver"}

try:
    print(f"💎 Syncing to Postgres (nyc_data.gold_all_violations_geo)...")
    gold_final.write.jdbc(url=jdbc_url, table="gold_all_violations_geo", mode="overwrite", properties=db_props)
    print("✅ Success! Gold Layer is ready for the bot.")
except Exception as e:
    print(f"❌ JDBC Write Error: {e}")

spark.stop()