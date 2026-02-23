from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim, regexp_replace, split

spark = SparkSession.builder \
    .appName("Investigate_Join_Failures") \
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

# 3. הניקוי הנוכחי שלנו (כדי שנראה מה יוצא ממנו)
STREET_CLEAN_REGEX = r'\b(STREET|ST|AVENUE|AVE|ROAD|RD|PLACE|PL|DRIVE|DR|BLVD|LANE|LN|PKWY|EXPRESSWAY|EXP|TER|COURT|CT|TH|RD|ND|ST|WEST|EAST|NORTH|SOUTH|W|E|N|S)\b'

df_k_clean = df_kafka.select(
    col("summons_number"),
    col("street_name").alias("original_street"),
    col("house_number").alias("original_house"),
    split(trim(col("house_number")), "-").getItem(0).alias("h_k"),
    trim(regexp_replace(regexp_replace(upper(col("street_name")), STREET_CLEAN_REGEX, ''), r'[^A-Z0-9 ]', '')).alias("s_k")
).dropDuplicates(["summons_number"])

df_a_clean = df_addresses.select(
    split(trim(col("house_number")), "-").getItem(0).alias("h_a"),
    trim(regexp_replace(regexp_replace(upper(col("street_name")), STREET_CLEAN_REGEX, ''), r'[^A-Z0-9 ]', '')).alias("s_a")
).dropDuplicates(["h_a", "s_a"])

# 4. ביצוע Join ומציאת הכישלונות
df_joined = df_k_clean.join(df_a_clean, (df_k_clean.s_k == df_a_clean.s_a) & (df_k_clean.h_k == df_a_clean.h_a), "left")

# 5. בידוד ה-Failed Matches (איפה שאין קואורדינטות)
df_failures = df_joined.filter(col("s_a").isNull())

print("\n" + "="*80)
print("TOP 100 FAILED MATCHES (Why did these fail?)")
print("="*80)
# נראה את השם המקורי מול השם הנקי כדי להבין מה חסר
df_failures.select("original_house", "h_k", "original_street", "s_k").show(100, truncate=False)

# 6. סטטיסטיקה של הרחובות הבעייתיים ביותר
print("\n" + "="*80)
print("MOST COMMON FAILED STREETS")
print("="*80)
df_failures.groupBy("original_street").count().orderBy(col("count").desc()).show(20)

spark.stop()