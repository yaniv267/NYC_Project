
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim,lit
from nyc_schema import silver_violation_codes_schema

# =========================
# 2. INITIALIZE SPARK SESSION
# =========================

spark = SparkSession.builder \
    .appName("process_parking_codes_silver") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# =========================
# 3. READ FROM BRONZE (RAW JSON)
# =========================
df_raw = spark.read.json("s3a://spark/bronze/violation_codes.json")

# =========================
# 4. DATA TRANSFORMATION & CLEANING
# =========================

df_cleaned = df_raw.select(
    col("code").cast("string").alias("violation_code"), 
    upper(trim(col("definition"))).alias("violation_description"),
    col("manhattan_96th_st_below").cast("double"),
    col("all_other_areas").cast("double")
)

# =========================
# 5. APPLY SILVER DATA CONTRACT (SCHEMA ALIGNMENT)
# =========================
# Ensuring the DataFrame strictly follows the imported schema fields
existing_columns = df_cleaned.columns
final_selection = []

for field in silver_violation_codes_schema:
    if field.name in existing_columns:
        final_selection.append(col(field.name))
    else:
        # If a field is missing in raw data, populate with Null casted to schema type
        final_selection.append(lit(None).cast(field.dataType).alias(field.name))

df_silver_final = df_cleaned.select(*final_selection)

# =========================
# 6. WRITE TO SILVER LAYER (PARQUET)
# =========================
df_silver_final.write.mode("overwrite").parquet("s3a://spark/silver/violation_codes/")

print("✅ Successfully processed violation codes and saved to Silver layer.")