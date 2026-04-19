from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. אתחול סשן מהיר (רק לקריאה)
spark = (SparkSession.builder 
    .appName("Exact_Value_Check") 
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") 
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") 
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") 
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") 
    .config("spark.hadoop.fs.s3a.path.style.access", "true") 
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
    .getOrCreate())

# 2. קריאה מהברונז
df_bronze = spark.read.parquet("s3a://spark/bronze/addresses_eliran/")

# 3. חילוץ ה-Longitude כמחרוזת (String) כדי לשמור על דיוק טקסטואלי
geo_regex = r"POINT \(([^ ]+) ([^ ]+)\)"

df_check = (
    df_bronze
    .withColumn("lon_str", F.regexp_extract(F.col("the_geom"), geo_regex, 1))
    .withColumn("lat_str", F.regexp_extract(F.col("the_geom"), geo_regex, 2))
    
    # 4. פילטר על הערך המדויק שביקשת (כטקסט)
    .filter(F.col("lon_str") == "-73.82327")
)

# 5. הצגת התוצאה
print("\n" + "="*70)
print(f"🎯 מחפש רשומות עם הערך המדויק: -73.82327")
print("="*70)

# בדיקה אם נמצאו שורות
result_count = df_check.count()

if result_count > 0:
    print(f"✅ נמצאו {result_count} רשומות תואמות:")
    df_check.select(
        F.col("the_geom").alias("Original_Geom"),
        F.col("Full Street Name").alias("Street"),
        F.col("lon_str").alias("Extracted_Longitude")
    ).show(truncate=False)
else:
    print("❌ לא נמצאה אף רשומה עם הערך המדויק '-73.82327'.")
    print("מוודא מה כן קיים - מציג 5 שורות ראשונות מהחילוץ הכללי:")
    df_bronze.select("the_geom") \
             .withColumn("extracted", F.regexp_extract(F.col("the_geom"), geo_regex, 1)) \
             .show(5, truncate=False)

print("="*70)
spark.stop()