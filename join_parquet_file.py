from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, split, broadcast
)

# 1. Initialize Session

spark = SparkSession.builder \
    .appName("NYC_Parking_Final_Join") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 2. Load Data
df_kafka = spark.read.parquet("s3a://spark/nyc_parking_raw.parquet")
df_addresses = spark.read.parquet("s3a://spark/data/dims/address")

# --- לוגיקת ניקוי אחידה לשני הצדדים ---

# רג'קס להסרת סיומות רחוב נפוצות (ST, AVE, STREET וכו')
# רג'קס משופר להסרת כיוונים, סיומות ומספרי רחובות (כדי ש-17TH יהפוך ל-17)
STREET_CLEAN_REGEX = r'\b(STREET|ST|AVENUE|AVE|AV|ROAD|RD|PLACE|PL|DRIVE|DR|BLVD|LANE|LN|PKWY|PKY|EXPRESSWAY|EXP|TER|COURT|CT|TH|RD|ND|ST|WEST|EAST|NORTH|SOUTH|W|E|N|S)\b'

# 3. Apply Cleaning to Kafka
df_k_clean = df_kafka.select(
    col("summons_number"),
    split(trim(col("house_number")), "-").getItem(0).alias("h_k"),
    trim(
        regexp_replace(
            regexp_replace(upper(col("street_name")), STREET_CLEAN_REGEX, ''),
            r'[^A-Z0-9 ]', ''
        )
    ).alias("s_k")
).filter(col("h_k").isNotNull()).dropDuplicates(["summons_number"])

# 4. Apply Cleaning to Addresses
df_a_clean = df_addresses.select(
    split(trim(col("house_number")), "-").getItem(0).alias("h_a"),
    trim(
        regexp_replace(
            regexp_replace(upper(col("street_name")), STREET_CLEAN_REGEX, ''),
            r'[^A-Z0-9 ]', ''
        )
    ).alias("s_a"),
    col("longitude"),
    col("latitude")
).dropDuplicates(["h_a", "s_a"])

# 5. Perform Join (Matching s_k to s_a and h_k to h_a)
df_joined = df_k_clean.join(
    broadcast(df_a_clean),
    (df_k_clean.s_k == df_a_clean.s_a) &
    (df_k_clean.h_k == df_a_clean.h_a),
    "left"
)

# 6. Show Results
print("\n" + "="*50)
print("FINAL JOIN SUCCESS RATE")
print("="*50)

total = df_k_clean.count()
no_match = df_joined.filter(col("longitude").isNull()).count()
matched = total - no_match

print(f"Total Parking Tickets: {total}")
print(f"Successfully Matched: {matched} ({(matched/total)*100:.2f}%)")
print(f"Failed to Match: {no_match}")

# Optional: Sample of successful matches
df_joined.filter(col("longitude").isNotNull()).select(
    "summons_number", "h_k", "s_k", "longitude", "latitude"
).show(10, truncate=False)

spark.stop()