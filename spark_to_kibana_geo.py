from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import requests
import time
import os

# 1. הקמת ה-Session
spark = SparkSession.builder \
    .appName("NYC_Parking_Final_Optimized") \
    .config("spark.jars.packages", 
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.elasticsearch:elasticsearch-spark-30_2.12:7.13.2") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

CACHE_PATH = "s3a://spark/reference/geo_cache.parquet"

# 2. פונקציית גיאוקודינג
def get_coords(house, street, county):
    if not house or not street: return None
    b_map = {"K": "Brooklyn", "M": "Manhattan", "Q": "Queens", "BX": "Bronx", "R": "Staten Island"}
    addr = f"{house} {street}, {b_map.get(county, 'NY')}, NYC"
    try:
        r = requests.get("https://nominatim.openstreetmap.org/search", 
                         params={"q": addr, "format": "json", "limit": 1},
                         headers={"User-Agent": "nyc_data_engineering_final"})
        data = r.json()
        if data:
            time.sleep(1) # חובה בגלל מגבלות ה-API
            return float(data[0]["lat"]), float(data[0]["lon"])
    except: return None
    return None

geocode_udf = udf(get_coords, StructType([
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType())
]))

# 3. טעינת הנתונים החדשים מה-MinIO
print("--- Reading raw data from MinIO ---")
df_all = spark.read.parquet("s3a://spark/nyc_parking_geocoded")

# 4. ניהול ה-Cache (מילון הכתובות)
try:
    geo_cache = spark.read.parquet(CACHE_PATH)
    print("--- Loaded existing geo-cache from MinIO ---")
except:
    print("--- No cache found. Creating a new one ---")
    schema_cache = StructType([
        StructField("house_number", StringType()),
        StructField("street_name", StringType()),
        StructField("violation_county", StringType()),
        StructField("coords", StructType([
            StructField("lat", DoubleType()),
            StructField("lon", DoubleType())
        ]))
    ])
    geo_cache = spark.createDataFrame([], schema_cache)

# 5. זיהוי כתובות חדשות שדורשות API
unique_new_addresses = df_all.select("house_number", "street_name", "violation_county").distinct()
addresses_to_query = unique_new_addresses.join(geo_cache, ["house_number", "street_name", "violation_county"], "left_anti")

# 6. הרצת גיאוקודינג רק לחדשים
if addresses_to_query.count() > 0:
    print(f"--- Querying API for {addresses_to_query.count()} NEW addresses ---")
    new_geo_results = addresses_to_query.withColumn("coords", 
        geocode_udf(col("house_number"), col("street_name"), col("violation_county")))
    
    # עדכון המילון ב-MinIO
    updated_cache = geo_cache.union(new_geo_results)
    updated_cache.write.mode("overwrite").parquet(CACHE_PATH)
    print("--- Cache updated successfully ---")
else:
    print("--- All addresses already in cache! Skipping API calls ---")
    updated_cache = geo_cache

# 7. חיבור הנתונים (Join) והכנה לאלסטיק
print("--- Preparing final data with locations ---")
final_df = df_all.join(updated_cache, ["house_number", "street_name", "violation_county"], "left") \
    .withColumn("location", struct(col("coords.lat").alias("lat"), col("coords.lon").alias("lon"))) \
    .filter(col("location.lat").isNotNull())

# 8. כתיבה לאלסטיק
print("--- Writing to Elasticsearch ---")
final_df.write.format("es") \
    .option("es.nodes", "172.17.0.1") \
    .option("es.resource", "nyc_parking_mapped") \
    .option("es.nodes.wan.only", "true") \
    .mode("append") \
    .save()

print("--- SUCCESS! Check your dashboard in Kibana ---")
spark.stop()