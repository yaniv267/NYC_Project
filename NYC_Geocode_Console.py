from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import requests
import time

# 1️⃣ יצירת SparkSession
spark = SparkSession.builder \
    .appName("NYC_Geocode_Console_Kafka") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# 2️⃣ סכימה של השדות הרלוונטיים
schema = StructType([
    StructField("house_number", StringType(), True),
    StructField("street_name", StringType(), True),
    StructField("violation_county", StringType(), True)
])

# 3️⃣ קריאה מקפקא
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:9093") \
    .option("subscribe", "nyc_parking_data_last_date") \
    .option("startingOffsets", "earliest") \
    .load()

# המרת value ל-STRING
df = df.selectExpr("CAST(value AS STRING) as json_str")

# 4️⃣ פענוח JSON לפי הסכימה
parsed_df = df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# 5️⃣ Cache לכתובות שכבר פענחנו
address_cache = {}

# 6️⃣ פונקציית גיאוקודינג עם cache
def geocode_cached(house_number, street_name, violation_county):
    if not house_number or not street_name:
        return None, None

    borough_map = {"K": "Brooklyn", "M": "Manhattan", "Q": "Queens", "B": "Bronx", "R": "Staten Island"}
    borough = borough_map.get(violation_county, "NY")

    address = f"{house_number} {street_name}, {borough}, New York City"

    # אם כבר פענחנו, מחזירים מיד
    if address in address_cache:
        return address_cache[address]

    # אחרת מבצעים geocoding
    try:
        url = "https://nominatim.openstreetmap.org/search"
        params = {"q": address, "format": "json", "limit": 1}
        headers = {"User-Agent": "nyc-data-project"}
        r = requests.get(url, params=params, headers=headers, timeout=5)
        data = r.json()
        if data:
            lat = float(data[0]["lat"])
            lon = float(data[0]["lon"])
            address_cache[address] = (lat, lon)
            time.sleep(1)  # חשוב כדי לא להיחסם
            return lat, lon
    except:
        return None, None

    return None, None

# 7️⃣ הרשמה של UDF
geocode_spark_udf = udf(geocode_cached, StructType([
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True)
]))

# 8️⃣ הוספת LAT ו-LON
df_geocoded = parsed_df.withColumn("geo", geocode_spark_udf(col("house_number"), col("street_name"), col("violation_county"))) \
                       .withColumn("lat", col("geo.lat")) \
                       .withColumn("lon", col("geo.lon")) \
                       .drop("geo")

# 9️⃣ הצגה בקונסול
df_geocoded.select("house_number", "street_name", "violation_county", "lat", "lon").show(50, truncate=False)

spark.stop()

