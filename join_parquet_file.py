
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    upper,
    trim,
    regexp_replace,
    split
)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
# --------------------------------------------------
# יצירת Spark Session
# --------------------------------------------------

# 1️⃣ Spark session עם חיבור ל-MinIO
spark = SparkSession.builder \
    .appName("Join_Addresses_Kafka") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# --------------------------------------------------
# טעינת הנתונים
# --------------------------------------------------

windowSpec = Window.partitionBy("summons_number").orderBy("longitude")
df_kafka = spark.read.parquet("s3a://spark/nyc_parking_raw.parquet")
df_addresses = spark.read.parquet("s3a://spark/data/dims/address")

# --------------------------------------------------
# ניקוי KAFKA
# --------------------------------------------------

df_kafka_clean = df_kafka.select(
    col("summons_number"),

    # ניקוי house number + טיפול במקפים
    split(
        trim(upper(col("house_number"))),
        "-"
    ).getItem(0).alias("house_number_kafka_clean"),

    # ניקוי street name מלא
    trim(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                upper(col("street_name")),
                                r'^/.*', ''
                            ),
                            r'C/O.*', ''
                        ),
                        r'\b(STREET|ST|AVENUE|AVE|ROAD|RD|PLACE|PL|DRIVE|DR|BLVD|LANE|LN|PKWY|EXPRESSWAY|EXP|TER|COURT|CT)\b',
                        ''
                    ),
                    r'\b(EAST|WEST|NORTH|SOUTH|E|W|N|S)\b',
                    ''
                ),
                r'[^A-Z0-9 ]',
                ''
            ),
            r'\s+',
            ' '
        )
    ).alias("street_name_kafka_clean")
)

# --------------------------------------------------
# ניקוי ADDRESSES
# --------------------------------------------------

df_addresses_clean = df_addresses.select(
    split(
        trim(upper(col("house_number"))),
        "-"
    ).getItem(0).alias("house_number_addr_clean"),

    trim(upper(col("street_name"))).alias("street_name_addr"),

    col("longitude"),
    col("latitude")
)

# --------------------------------------------------
# JOIN
# --------------------------------------------------

df_joined = df_kafka_clean.join(
    df_addresses_clean,
    (df_kafka_clean.street_name_kafka_clean == df_addresses_clean.street_name_addr) &
    (df_kafka_clean.house_number_kafka_clean == df_addresses_clean.house_number_addr_clean),
    "inner"
)

# --------------------------------------------------
# תוצאה
# --------------------------------------------------

df_joined.select(
    "summons_number",
    "house_number_kafka_clean",
    "street_name_kafka_clean",
    "longitude",
    "latitude"
).show(100, truncate=False)

print("Kafka count:", df_kafka_clean.count())
print("Addresses count:", df_addresses_clean.count())
print("Joined count:", df_joined.count())
