from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import requests
import time

# 1. הקמת Session
spark = SparkSession.builder \
    .appName("NYC_Geocoding_Final") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.elasticsearch:elasticsearch-spark-30_2.12:7.13.2") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 2. פונקציית גיאוקודינג
def get_coords(house, street, county):
    if not house or not street: return None
    addr = f"{house} {street}, {county}, NYC"
    try:
        # שימוש ב-API חינמי (שימו לב למגבלת קצבים)
        r = requests.get("https://nominatim.openstreetmap.org/search", 
                         params={"q": addr, "format": "json", "limit": 1},
                         headers={"User-Agent": "my_app_v1"})
        data = r.json()
        if data:
            return float(data[0]["lat"]), float(data[0]["lon"])
    except:
        return None
    return None

geocode_udf = udf(get_coords, StructType([
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType())
]))

# 3. עיבוד
df = spark.read.parquet("s3a://spark/nyc_parking_geocoded")

# הוספת קואורדינטות (מומלץ להריץ על sample קטן קודם כי זה לוקח זמן)
df_geo = df.withColumn("coords", geocode_udf(col("house_number"), col("street_name"), col("violation_county"))) \
           .withColumn("location", struct(col("coords.lat").alias("lat"), col("coords.lon").alias("lon")))

# 4. כתיבה לאינדקס החדש
df_geo.write.format("es") \
    .option("es.nodes", "172.17.0.1") \
    .option("es.resource", "nyc_parking_mapped") \
    .save()